package metadata

import (
	"encoding/json"
	"sort"
)

/**
 * Assignment represent the assignment of one partition
 *
 * Leader is the id of the leader broker
 * Follower is the id of the follower broker
 */
type Assignment struct {
	Leader   string `json:"leader"`
	Follower string `json:"follower"`
}

/**
 * Broker represent a member of the cluster
 *
 * ID is the id of the broker
 * RPCAddr is the rpc address of the broker (data plane)
 */
type Broker struct {
	ID      string `json:"id"`
	RPCAddr string `json:"rpc_addr"`
}

/**
 * Cluster represent the full state of the cluster (replicated by raft)
 *
 * NumPartitions is the number of partitions
 * Brokers is the map [id -> broker]
 * Partitions is the map [partition -> assignment]
 * Down is the map of the brokers marked offline
 */
type Cluster struct {
	NumPartitions int                `json:"num_partitions"`
	Brokers       map[string]Broker  `json:"brokers"`
	Partitions    map[int]Assignment `json:"partitions"`
	Down          map[string]bool    `json:"down"`
}

/**
 * This function create an empty cluster
 *
 * @return an empty cluster
 */
func NewCluster() *Cluster {
	return &Cluster{
		Brokers:    map[string]Broker{},
		Partitions: map[int]Assignment{},
		Down:       map[string]bool{},
	}
}

/**
 * This function return a deep copy of the cluster
 *
 * @return a copy of the cluster
 */
func (c *Cluster) Clone() *Cluster {

	// new empty cluster
	n := NewCluster()
	n.NumPartitions = c.NumPartitions

	// copy the brokers
	for k, v := range c.Brokers {
		n.Brokers[k] = v
	}

	// copy the partitions
	for k, v := range c.Partitions {
		n.Partitions[k] = v
	}

	// copy the down brokers
	for k, v := range c.Down {
		n.Down[k] = v
	}

	return n
}

/**
 * This function return the ids of the alive brokers, sorted (deterministic)
 *
 * @return the sorted ids of the alive brokers
 */
func (c *Cluster) AliveBrokerIDs() []string {

	// keep only the brokers that are not down
	var ids []string
	for id := range c.Brokers {
		if !c.Down[id] {
			ids = append(ids, id)
		}
	}

	// sort to be deterministic
	sort.Strings(ids)
	return ids
}

/**
 * This function return the rpc address of a broker
 *
 * @param id the id of the broker
 * @return the rpc address of the broker
 */
func (c *Cluster) RPCAddrOf(id string) string {
	return c.Brokers[id].RPCAddr
}

/**
 * This function encode the cluster to json
 *
 * @return the cluster as json bytes
 */
func (c *Cluster) Marshal() ([]byte, error) { return json.Marshal(c) }

/**
 * This function decode a cluster from json
 *
 * @param b the json bytes
 * @return the cluster
 */
func Unmarshal(b []byte) (*Cluster, error) {

	// build an empty cluster then fill it from the json
	c := NewCluster()
	if err := json.Unmarshal(b, c); err != nil {
		return nil, err
	}
	return c, nil
}
