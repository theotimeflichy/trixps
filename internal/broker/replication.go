package broker

import (
	"fmt"
	netrpc "net/rpc"
	"time"

	rpctypes "trixps/internal/rpc"
)

/**
 * RPCService represent the net/rpc service of the broker
 *
 * b is the underlying broker every method dispatch to
 */
type RPCService struct {
	b *Broker
}

/**
 * This function serve service discovery by returning the cluster
 *
 * @param args metadata request
 * @param reply cluster metadata
 * @return nil
 */
func (s *RPCService) Metadata(_ rpctypes.MetadataArgs, reply *rpctypes.MetadataReply) error {

	// reply with the current cluster snapshot of the fsm
	reply.Cluster = s.b.fsm.Cluster()
	return nil
}

/**
 * This function report this node's raft health
 *
 * @param args
 * @param reply filled with the node id and its raft state
 * @return nil
 */
func (s *RPCService) Health(_ rpctypes.HealthArgs, reply *rpctypes.HealthReply) error {

	reply.ID = s.b.cfg.ID
	reply.RaftState = s.b.raftState()
	return nil
}

/**
 * This function do a single write with synchronous replication
 *
 * @param args the topic, partition, key and value of the record to write
 * @param reply filled with the offset and flag on success
 * @return nil once the record is committed on the leader
 */
func (s *RPCService) Produce(args rpctypes.ProduceArgs, reply *rpctypes.ProduceReply) error {

	// check if leader
	b := s.b
	topic, p := args.Topic, args.Partition

	cluster := b.fsm.Cluster()
	asg, ok := cluster.Partitions[p]
	if !ok {
		return fmt.Errorf("Partition %d not found", p)
	}
	if asg.Leader != b.cfg.ID {
		return fmt.Errorf("not leader, the leader of %d is %s", p, asg.Leader)
	}

	// open the log of the (topic, partition)
	lg, err := b.logFor(topic, p)
	if err != nil {
		return err
	}

	// get mutex and lock it
	plock := b.produceLock(topic, p)
	plock.Lock()
	defer plock.Unlock()

	// reserve the next offset and a timestamp
	offset := lg.NextOffset()
	ts := time.Now().UnixNano()

	// synchronous replication to the follower
	if asg.Follower != "" && asg.Follower != b.cfg.ID {
		followerAddr := cluster.RPCAddrOf(asg.Follower)
		if err := b.replicateToFollower(followerAddr, topic, p, offset, args.Key, args.Value, ts); err != nil {
			return fmt.Errorf("replicate to follower %s failed, writing refused: %w", asg.Follower, err)
		}
	}

	// save locally
	if err := lg.AppendAt(offset, args.Key, args.Value, ts); err != nil {
		return err
	}
	reply.Offset = offset
	reply.Committed = true
	return nil
}

/**
 * This function do a batched message sending
 *
 * @param args the topic, partition and the ordered records to write
 * @param reply filled with the base offset, the record count and a flag
 * @return nil if ok
 */
func (s *RPCService) ProduceBatch(args rpctypes.ProduceBatchArgs, reply *rpctypes.ProduceBatchReply) error {

	// check if empty
	b := s.b
	topic, p := args.Topic, args.Partition
	if len(args.Records) == 0 {
		return nil
	}

	// check if leader
	cluster := b.fsm.Cluster()
	asg, ok := cluster.Partitions[p]
	if !ok {
		return fmt.Errorf("partition %d not found", p)
	}
	if asg.Leader != b.cfg.ID {
		return fmt.Errorf("not leader: the leader of %d is %s", p, asg.Leader)
	}

	// open the log of the (topic, partition)
	lg, err := b.logFor(topic, p)
	if err != nil {
		return err
	}

	// get mutex and lock it
	plock := b.produceLock(topic, p)
	plock.Lock()
	defer plock.Unlock()

	// reserve the base offset and a timestamp
	base := lg.NextOffset()
	ts := time.Now().UnixNano()

	// assign contiguous offsets to the whole batch
	recs := make([]rpctypes.ReplicateRecord, len(args.Records))
	for i, r := range args.Records {
		recs[i] = rpctypes.ReplicateRecord{Offset: base + uint64(i), Key: r.Key, Value: r.Value, TsNano: ts}
	}

	// synchronous replication to the follower
	if asg.Follower != "" && asg.Follower != b.cfg.ID {
		followerAddr := cluster.RPCAddrOf(asg.Follower)
		if err := b.replicateBatchToFollower(followerAddr, topic, p, base, recs); err != nil {
			return fmt.Errorf("replication has failed to %s failed, writing refused : %w", asg.Follower, err)
		}
	}

	// commit locally only after the follower has acked
	for _, rr := range recs {
		if err := lg.AppendAt(rr.Offset, rr.Key, rr.Value, rr.TsNano); err != nil {
			return err
		}
	}
	reply.BaseOffset = base
	reply.Count = len(recs)
	reply.Committed = true
	return nil
}

/**
 * This function is called on the follower by the partition leader to append a batch
 *
 * @param args the topic, partition and the ordered records to append
 * @param reply with Ack
 * @return nil if ok
 */
func (s *RPCService) ReplicateBatch(args rpctypes.ReplicateBatchArgs, reply *rpctypes.ReplicateBatchReply) error {

	// open the log of the (topic, partition)
	lg, err := s.b.logFor(args.Topic, args.Partition)
	if err != nil {
		return err
	}

	// writing each records
	for _, r := range args.Records {
		if err := lg.AppendAt(r.Offset, r.Key, r.Value, r.TsNano); err != nil {
			return err
		}
	}
	reply.Ack = true
	return nil
}

/**
 * This function bring the follower up to date and then send a batch
 *
 * @param addr the rpc address
 * @param topic the topic id
 * @param p the partition number
 * @param base the offset
 * @param recs the records of the current batch to replicate
 * @return nil if ok
 */
func (b *Broker) replicateBatchToFollower(addr, topic string, p int, base uint64, recs []rpctypes.ReplicateRecord) error {

	// check adresse
	if addr == "" {
		return fmt.Errorf("address is missing")
	}

	// dial the follower
	client, err := b.dial(addr)
	if err != nil {
		return err
	}

	// ask the follower how far it has caught up
	var st rpctypes.FollowerStateReply
	if err := client.Call("RPC.FollowerState", rpctypes.FollowerStateArgs{Topic: topic, Partition: p}, &st); err != nil {
		b.dropClient(addr)
		return err
	}

	// fill missing offset
	if st.NextOffset < base {
		lg, err := b.logFor(topic, p)
		if err != nil {
			return err
		}
		missing, err := lg.ReadFrom(st.NextOffset, int(base-st.NextOffset))
		if err != nil {
			return err
		}
		catchup := make([]rpctypes.ReplicateRecord, len(missing))
		for i, r := range missing {
			catchup[i] = rpctypes.ReplicateRecord{Offset: r.Offset, Key: r.Key, Value: r.Value, TsNano: r.TsNano}
		}
		if err := b.sendReplicateBatch(client, addr, topic, p, catchup); err != nil {
			return err
		}
	}

	// send the current batch
	return b.sendReplicateBatch(client, addr, topic, p, recs)
}

/**
 * This function do one ReplicateBatch call and check the ack
 *
 * @param client the rpc client
 * @param addr the follower
 * @param topic the topic id
 * @param p the partition number
 * @param recs the records to replicat
 * @return nil if ok
 */
func (b *Broker) sendReplicateBatch(client *netrpc.Client, addr, topic string, p int, recs []rpctypes.ReplicateRecord) error {

	// if empty, stop
	if len(recs) == 0 {
		return nil
	}

	// send the batch
	args := rpctypes.ReplicateBatchArgs{Topic: topic, Partition: p, Records: recs}
	var reply rpctypes.ReplicateBatchReply
	if err := client.Call("RPC.ReplicateBatch", args, &reply); err != nil {
		b.dropClient(addr)
		return err
	}

	// if missing : throw error
	if !reply.Ack {
		return fmt.Errorf("The follower didn't acknoledge")
	}
	return nil
}

/**
 * This function make sure the follower holds every record up to offset, then send the new one
 *
 *
 * @param addr the rpc address of the follower
 * @param topic the topic name
 * @param p the partition number
 * @param offset the offset of the new record (exclusive upper bound for the catch-up)
 * @param key the record key
 * @param val the record value
 * @param ts the record timestamp in nanoseconds
 * @return nil once the follower has acked the new record
 */
func (b *Broker) replicateToFollower(addr, topic string, p int, offset uint64, key, val string, ts int64) error {

	// no address means no follower to replicate to
	if addr == "" {
		return fmt.Errorf("adresse du follower inconnue")
	}

	// dial the follower
	client, err := b.dial(addr)
	if err != nil {
		return err
	}

	// ask the follower how far it has caught up on this (topic, partition)
	var st rpctypes.FollowerStateReply
	if err := client.Call("RPC.FollowerState", rpctypes.FollowerStateArgs{Topic: topic, Partition: p}, &st); err != nil {
		b.dropClient(addr)
		return err
	}

	// backfill the missing offsets [st.NextOffset, offset)
	if st.NextOffset < offset {
		lg, err := b.logFor(topic, p)
		if err != nil {
			return err
		}
		missing, err := lg.ReadFrom(st.NextOffset, int(offset-st.NextOffset))
		if err != nil {
			return err
		}
		for _, r := range missing {
			if err := b.sendReplicate(client, addr, topic, p, r.Offset, r.Key, r.Value, r.TsNano); err != nil {
				return err
			}
		}
	}

	// replicate the new record
	return b.sendReplicate(client, addr, topic, p, offset, key, val, ts)
}

/**
 * This function do a single Replicate call and check the ack
 *
 * @param client the rpc client
 * @param addr the follower
 * @param topic the topic name
 * @param p the partition number
 * @param offset the offset at which the record must be written
 * @param key the record key
 * @param val the record value
 * @param ts the record timestamp
 * @return nil if ok
 */
func (b *Broker) sendReplicate(client *netrpc.Client, addr, topic string, p int, offset uint64, key, val string, ts int64) error {

	// send the record
	args := rpctypes.ReplicateArgs{Topic: topic, Partition: p, Offset: offset, Key: key, Value: val, TsNano: ts}
	var reply rpctypes.ReplicateReply
	if err := client.Call("RPC.Replicate", args, &reply); err != nil {
		b.dropClient(addr)
		return err
	}

	// if ask missing -> throw error
	if !reply.Ack {
		return fmt.Errorf("Follower has not acknoledge")
	}
	return nil
}

/**
 * This function is called to replicate the message on follower
 *
 * @param args the topic, partition, offset, key, value and timestamp to append
 * @param reply filled with Ack
 * @return nil if ok
 */
func (s *RPCService) Replicate(args rpctypes.ReplicateArgs, reply *rpctypes.ReplicateReply) error {

	// open the log of the (topic, partition)
	lg, err := s.b.logFor(args.Topic, args.Partition)
	if err != nil {
		return err
	}

	// writing
	if err := lg.AppendAt(args.Offset, args.Key, args.Value, args.TsNano); err != nil {
		return err
	}
	reply.Ack = true
	return nil
}

/**
 * This function return the next offset expected by the follower
 *
 * @param args the topic and partition being queried
 * @param reply filled with NextOffset
 * @return nil if ok
 */
func (s *RPCService) FollowerState(args rpctypes.FollowerStateArgs, reply *rpctypes.FollowerStateReply) error {

	// open the log of the (topic, partition)
	lg, err := s.b.logFor(args.Topic, args.Partition)
	if err != nil {
		return err
	}

	// send next waited offset
	reply.NextOffset = lg.NextOffset()
	return nil
}

/**
 * This function is called by a consumer to read
 *
 * @param args the topic, partition, starting offset and maximum record count
 * @param reply filled with the records read and next offset
 * @return nil if ok
 */
func (s *RPCService) Fetch(args rpctypes.FetchArgs, reply *rpctypes.FetchReply) error {

	// check partition
	b := s.b
	asg, ok := b.fsm.Cluster().Partitions[args.Partition]
	if !ok {
		return fmt.Errorf("partition %d is not found", args.Partition)
	}

	// check if leader
	if asg.Leader != b.cfg.ID {
		return fmt.Errorf("not leader: the leader of partition %d is %s", args.Partition, asg.Leader)
	}

	// open the log of the (topic, partition)
	lg, err := b.logFor(args.Topic, args.Partition)
	if err != nil {
		return err
	}

	// reading
	recs, err := lg.ReadFrom(args.Offset, args.Max)
	if err != nil {
		return err
	}

	// sending to consumer
	next := args.Offset
	for _, r := range recs {
		reply.Records = append(reply.Records, rpctypes.FetchRecord{
			Offset: r.Offset, Key: r.Key, Value: r.Value, TsNano: r.TsNano,
		})
		next = r.Offset + 1
	}
	reply.NextOffset = next
	return nil
}
