package raftfsm

import (
	"encoding/json"
	"io"
	"sync"

	"github.com/hashicorp/raft"
	"trixps/internal/metadata"
)

/**
 * CmdType represent the type of the commande.
 */
type CmdType string

/**
 * The CmdType constants name.
 *
 * CmdRegisterBroker register a broker in the cluster
 * CmdAssign (re)assign a partition
 * CmdMarkDown mark a broker offline
 * CmdMarkUp mark a broker back online
 * CmdReassignLeader promote the follower to leader
 */
const (
	CmdRegisterBroker CmdType = "register_broker"
	CmdAssign         CmdType = "assign"
	CmdMarkDown       CmdType = "mark_down"
	CmdMarkUp         CmdType = "mark_up"
	CmdReassignLeader CmdType = "reassign_leader"
)

/**
 * Command represent one mutation logged by raft
 *
 * Type is the command
 * BrokerID is the id of the broker
 * RPCAddr is the rpc address of the broker
 * Partition is the partition the command is about
 * Leader is the id of the leader broker
 * Follower is the id of the follower broker
 * NumPartitions is the number of partitions
 */
type Command struct {
	Type          CmdType `json:"type"`
	BrokerID      string  `json:"broker_id,omitempty"`
	RPCAddr       string  `json:"rpc_addr,omitempty"`
	Partition     int     `json:"partition,omitempty"`
	Leader        string  `json:"leader,omitempty"`
	Follower      string  `json:"follower,omitempty"`
	NumPartitions int     `json:"num_partitions,omitempty"`
}

/**
 * FSM represent the state machine of cluster
 *
 * mu is the mutex protecting the cluster
 * cluster is the cluster
 */
type FSM struct {
	mu      sync.RWMutex
	cluster *metadata.Cluster
}

/**
 * This function create an FSM
 *
 * @return a ready to use empty FSM
 */
func New() *FSM {
	return &FSM{cluster: metadata.NewCluster()}
}

/**
 * This function return a copy of cluster
 *
 * @return a deep copy of the current cluster
 */
func (f *FSM) Cluster() *metadata.Cluster {

	// close the lock
	f.mu.RLock()

	// open the lock when fnction is over
	defer f.mu.RUnlock()

	// return the copy
	return f.cluster.Clone()
}

/**
 * This function is called by raft for every command which is sent
 *
 * @param l the raft log
 * @return nil on success, or the error
 */
func (f *FSM) Apply(l *raft.Log) interface{} {

	// decode the command from the log entry
	var cmd Command
	if err := json.Unmarshal(l.Data, &cmd); err != nil {
		return err
	}

	// close the lock of mutex
	f.mu.Lock()

	// open the lock once the function is ended
	defer f.mu.Unlock()
	c := f.cluster

	// apply the ommand
	switch cmd.Type {
	case CmdRegisterBroker:
		c.Brokers[cmd.BrokerID] = metadata.Broker{ID: cmd.BrokerID, RPCAddr: cmd.RPCAddr}
		delete(c.Down, cmd.BrokerID)
	case CmdAssign:
		if cmd.NumPartitions > c.NumPartitions {
			c.NumPartitions = cmd.NumPartitions
		}
		c.Partitions[cmd.Partition] = metadata.Assignment{Leader: cmd.Leader, Follower: cmd.Follower}
	case CmdMarkDown:
		c.Down[cmd.BrokerID] = true
	case CmdMarkUp:
		delete(c.Down, cmd.BrokerID)
	case CmdReassignLeader:
		a := c.Partitions[cmd.Partition]
		a.Leader = cmd.Leader
		a.Follower = cmd.Follower
		c.Partitions[cmd.Partition] = a
	}
	return nil
}


/**
 * This function capture the current state
 *
 * @return a raft.FSMSnapshot
 */
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {

	// close the lock
	f.mu.RLock()

	// open the lock when function is overr
	defer f.mu.RUnlock()

	// marshall to json
	data, err := f.cluster.Marshal()
	if err != nil {
		return nil, err
	}

	// return the snapshot
	return &snapshot{data: data}, nil
}

/**
 * This function rebuild the FSM state from a persisted snapshot
 *
 * @param rc the snapshot
 * @return nil if ok
 */
func (f *FSM) Restore(rc io.ReadCloser) error {

	// closing the reader at the end of func
	defer rc.Close()

	// read the snapshot
	data, err := io.ReadAll(rc)
	if err != nil {
		return err
	}

	// decode from json
	c, err := metadata.Unmarshal(data)
	if err != nil {
		return err
	}

	// replace
	f.mu.Lock()
	f.cluster = c
	f.mu.Unlock()
	return nil
}

/**
 * snapshot represent the serialized cluster state.
 *
 * data is the serialized cluster state
 */
type snapshot struct{ data []byte }

/**
 * This function write the snapshot
 *
 * @param sink the destination raft provides for the snapshot bytes
 * @return nil once the snapshot has been fully written
 */
func (s *snapshot) Persist(sink raft.SnapshotSink) error {

	// write the bytes
	if _, err := sink.Write(s.data); err != nil {
		sink.Cancel()
		return err
	}

	// close
	return sink.Close()
}

/**
 * This function is called by raft when the snapshot is not uselful anymore
 */
func (s *snapshot) Release() {}
