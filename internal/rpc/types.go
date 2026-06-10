package rpc

import "trixps/internal/metadata"




/**
 * MetadataArgs represent the argument of the metadata rpc
 */
type MetadataArgs struct{}

/**
 * MetadataReply represent the reply of the metadata rpc
 *
 * Cluster is the cluster
 */
type MetadataReply struct {
	Cluster *metadata.Cluster
}




/**
 * ProduceArgs represent the one message
 *
 * Topic is the topic id
 * Partition is the partition number
 * Key is the key of the message
 * Value is the value of the message
 */
type ProduceArgs struct {
	Topic     string
	Partition int
	Key       string
	Value     string
}

/**
 * ProduceReply represent the reply of sending message
 *
 * Offset is the offset given to the message
 * Committed is true once the message is replicated and ACK
 */
type ProduceReply struct {
	Offset    uint64
	Committed bool
}




/**
 * BatchRecord represent one message inside a batch
 *
 * Key is the key of the message
 * Value is the value of the message
 */
type BatchRecord struct {
	Key   string
	Value string
}

/**
 * ProduceBatchArgs represent the argument of the batch
 *
 * Topic is the topic id
 * Partition is the partition number
 * Records is the list of messages
 */
type ProduceBatchArgs struct {
	Topic     string
	Partition int
	Records   []BatchRecord
}

/**
 * ProduceBatchReply represent the reply of the batch
 *
 * BaseOffset is the offset of the first message
 * Count is the number of messages
 * Committed is true once the batch is replicated and ACK
 */
type ProduceBatchReply struct {
	BaseOffset uint64
	Count      int
	Committed  bool
}




/**
 * ReplicateRecord represent one record of a replicated batch
 *
 * Offset is the offset given by the leader
 * Key is the key of the message
 * Value is the value of the message
 * TsNano is the timestamp
 */
type ReplicateRecord struct {
	Offset uint64
	Key    string
	Value  string
	TsNano int64
}

/**
 * ReplicateBatchArgs represent the argument of the batch replication rpc
 *
 * Topic is the topic
 * Partition is the partition number
 * Records is the list of records to replicate
 */
type ReplicateBatchArgs struct {
	Topic     string
	Partition int
	Records   []ReplicateRecord
}

/**
 * ReplicateBatchReply represent the reply of the batch replication rpc
 *
 * Ack is true once the follower wrote everuthing
 */
type ReplicateBatchReply struct {
	Ack bool
}



/**
 * ReplicateArgs represent the argument of the replication rpc
 *
 * Topic is the topic id
 * Partition is the partition number
 * Offset is the offset
 * Key is the key
 * Value is the value
 * TsNano is the timestamp
 */
type ReplicateArgs struct {
	Topic     string
	Partition int
	Offset    uint64
	Key       string
	Value     string
	TsNano    int64
}

/**
 * ReplicateReply represent the reply of the replication rpc
 *
 * Ack is true once the follower wrote
 */
type ReplicateReply struct {
	Ack bool
}




/**
 * FollowerStateArgs represent the argument of the follower state rpc
 *
 * Topic is the topic
 * Partition is the partition number
 */
type FollowerStateArgs struct {
	Topic     string
	Partition int
}

/**
 * FollowerStateReply represent the reply of the follower state rpc
 *
 * NextOffset is the next offset the follower should receive
 */
type FollowerStateReply struct {
	NextOffset uint64
}




/**
 * FetchArgs represent the argument of the fetch rpc
 *
 * Topic is the topic id
 * Partition is the partition number
 * Offset is the offset to start the reader
 * Max is the max record
 */
type FetchArgs struct {
	Topic     string
	Partition int
	Offset    uint64
	Max       int
}

/**
 * FetchRecord represent one message
 *
 * Offset is the offset
 * Key is the key
 * Value is the value
 * TsNano is the timestamp
 */
type FetchRecord struct {
	Offset uint64
	Key    string
	Value  string
	TsNano int64
}

/**
 * FetchReply represent the reply of the fetch rpc
 *
 * Records is the list msg
 * NextOffset is the next offset
 */
type FetchReply struct {
	Records    []FetchRecord
	NextOffset uint64
}





/**
 * HealthArgs represent the argument of the health rpc
 */
type HealthArgs struct{}

/**
 * HealthReply represent the reply of the health rpc
 *
 * ID is broker id
 * RaftState is the raft state actually (Leader / Follower or Candidate)
 */
type HealthReply struct {
	ID        string
	RaftState string
}
