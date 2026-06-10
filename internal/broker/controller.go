package broker

import (
	"net"
	"time"

	"trixps/internal/metadata"
	"trixps/internal/raftfsm"
)

/**
 * This function run the control loop, checking if leader every seconds
 */
func (b *Broker) controlLoop() {

	// tick every second
	ticker := time.NewTicker(1 * time.Second)

	// stop the ticker once the function has ended
	defer ticker.Stop()

	// on each tick : check if leader and if so initiate
	for range ticker.C {

		if b.raft.State().String() != "Leader" {
			continue
		}

		b.ensureBootstrap()
		b.reconcile()
	}
}

/**
 * This function register the brokers and assign the partitions once
 */
func (b *Broker) ensureBootstrap() {

	// if everything is already registered and assigned, do nothing
	c := b.fsm.Cluster()
	if len(c.Brokers) == len(b.peers) && len(c.Partitions) == b.cfg.NumPartitions {
		return
	}

	// register every known broker missing
	for _, p := range b.peers {
		if _, ok := c.Brokers[p.ID]; !ok {
			_ = b.applyCommand(raftfsm.Command{
				Type: raftfsm.CmdRegisterBroker, BrokerID: p.ID, RPCAddr: p.RPCAddr,
			})
		}
	}

	// deterministic placement
	ids := b.sortedPeerIDs()
	n := len(ids)
	if n == 0 {
		return
	}

	// assign each missing partition a leader and a follower
	for p := 0; p < b.cfg.NumPartitions; p++ {
		if _, ok := c.Partitions[p]; ok {
			continue
		}
		leader := ids[p%n]
		follower := ids[(p+1)%n]
		_ = b.applyCommand(raftfsm.Command{
			Type: raftfsm.CmdAssign, Partition: p, Leader: leader, Follower: follower,
			NumPartitions: b.cfg.NumPartitions,
		})
	}
}

/**
 * This function probe the health of the brokers and
 * repair the partitions whose leader or follower failed
 */
func (b *Broker) reconcile() {

	// check heartbeat of the every broker
	c := b.fsm.Cluster()
	alive := map[string]bool{}
	for id, br := range c.Brokers {
		alive[id] = b.isAlive(id, br.RPCAddr)
	}

	// update the alive state of brokers
	for id := range c.Brokers {
		if !alive[id] && !c.Down[id] {
			_ = b.applyCommand(raftfsm.Command{Type: raftfsm.CmdMarkDown, BrokerID: id})
		} else if alive[id] && c.Down[id] {
			_ = b.applyCommand(raftfsm.Command{Type: raftfsm.CmdMarkUp, BrokerID: id})
		}
	}

	// Repair the potential broken partitions
	c = b.fsm.Cluster()
	for p := 0; p < c.NumPartitions; p++ {

		// skip the partitions that are not assigned
		asg, ok := c.Partitions[p]
		if !ok {
			continue
		}

		// nothing to repair if both the leader and the follower are alive
		leaderDown := !alive[asg.Leader]
		followerDown := asg.Follower != "" && !alive[asg.Follower]
		if !leaderDown && !followerDown {
			continue
		}

		// if the leader is down, promote the follower if alive, else any survivor
		newLeader := asg.Leader
		if leaderDown {
			if alive[asg.Follower] {
				newLeader = asg.Follower
			} else {
				newLeader = pickAlive(c, alive, "")
			}
		}

		// pick another survivor as the new follower
		newFollower := pickAlive(c, alive, newLeader)

		// stop if nothing changed
		if newLeader == "" {
			continue
		}

		// apply the reassignment
		if newLeader != asg.Leader || newFollower != asg.Follower {
			_ = b.applyCommand(raftfsm.Command{
				Type: raftfsm.CmdReassignLeader, Partition: p,
				Leader: newLeader, Follower: newFollower,
			})
		}
	}
}

/**
 * This function tell if a broker is reachable
 *
 * @param id the id of borker
 * @param rpcAddr the rpc address
 * @return true if succeeds
 */
func (b *Broker) isAlive(id, rpcAddr string) bool {

	// if it's calling itself
	if id == b.cfg.ID {
		return true
	}

	// connectign to the broker
	conn, err := net.DialTimeout("tcp", rpcAddr, 700*time.Millisecond)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

/**
 * This function return a live broker different from exclude
 *
 * @param c the cluster metadata
 * @param alive the alive map
 * @param exclude a broker to skip
 * @return the id of a broker alive
 */
func pickAlive(c *metadata.Cluster, alive map[string]bool, exclude string) string {

	// walk the alive broker ids in their canonical order
	for _, id := range c.AliveBrokerIDs() {
		if id == exclude {
			continue
		}
		if alive[id] {
			return id
		}
	}
	return ""
}
