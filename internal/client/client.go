package client

import (
	"fmt"
	"net"
	netrpc "net/rpc"
	"time"

	"trixps/internal/metadata"
	rpctypes "trixps/internal/rpc"
)

/**
 * Client represent a client object
 *
 * brokers is the list of broker
 */
type Client struct {
	brokers []string
}

/**
 * This function build a client from a list of broker rpc addresses
 *
 * @param brokers the broker rpc addresses
 * @return a client
 */
func New(brokers []string) *Client { return &Client{brokers: brokers} }

/**
 * This function open a rpc connection to one broker address
 *
 * @param addr the broker rpc address
 * @return a rpc client
 */
func dial(addr string) (*netrpc.Client, error) {

	// dial the address
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return nil, err
	}

	return netrpc.NewClient(conn), nil
}

/**
 * This function ask each broker if alive
 * This is dicovery
 *
 * @return the cluster metadata if found
 */
func (c *Client) Metadata() (*metadata.Cluster, error) {

	// try every broker
	var lastErr error
	for _, addr := range c.brokers {

		// try to dial the broker
		cl, err := dial(addr)
		if err != nil {
			lastErr = err
			continue
		}

		// getting metadata
		var reply rpctypes.MetadataReply
		err = cl.Call("RPC.Metadata", rpctypes.MetadataArgs{}, &reply)
		cl.Close()
		if err != nil {
			lastErr = err
			continue
		}

		// return the cluster if founded
		if reply.Cluster != nil {
			return reply.Cluster, nil
		}
	}

	// no broker found
	if lastErr == nil {
		lastErr = fmt.Errorf("aucun broker joignable")
	}
	return nil, lastErr
}

/**
 * This function send a message
 *
 * @param topic the topic id
 * @param partition the target partition
 * @param key the message key
 * @param value the message content
 * @return the offset given
 */
func (c *Client) Produce(topic string, partition int, key, value string) (uint64, error) {

	// retry multiple times
	var lastErr error
	for attempt := 0; attempt < 8; attempt++ {

		// refresh the metadata
		cl, err := c.Metadata()
		if err != nil {
			lastErr = err
			time.Sleep(1 * time.Second)
			continue
		}

		// find the leader of the partition
		asg, ok := cl.Partitions[partition]
		if !ok || asg.Leader == "" {
			lastErr = fmt.Errorf("No leader found %d", partition)
			time.Sleep(1 * time.Second)
			continue
		}

		// dial the leader
		addr := cl.RPCAddrOf(asg.Leader)
		conn, err := dial(addr)
		if err != nil {
			lastErr = err
			time.Sleep(1 * time.Second)
			continue
		}

		// send the message to the leader
		var reply rpctypes.ProduceReply
		err = conn.Call("RPC.Produce", rpctypes.ProduceArgs{Topic: topic, Partition: partition, Key: key, Value: value}, &reply)
		conn.Close()
		if err != nil {
			lastErr = err
			time.Sleep(1 * time.Second)
			continue
		}
		return reply.Offset, nil
	}
	return 0, fmt.Errorf("Failed to send message : %w", lastErr)
}

/**
 * This function send several messages
 *
 * @param topic the topic id
 * @param partition the target partition
 * @param recs the records to send in one batch
 * @return the base offset
 */
func (c *Client) ProduceBatch(topic string, partition int, recs []rpctypes.BatchRecord) (uint64, error) {

	// check not empty
	if len(recs) == 0 {
		return 0, nil
	}

	// retry multiple times
	var lastErr error
	for attempt := 0; attempt < 8; attempt++ {

		// refresh the metadata
		cl, err := c.Metadata()
		if err != nil {
			lastErr = err
			time.Sleep(1 * time.Second)
			continue
		}

		// getting leader
		asg, ok := cl.Partitions[partition]
		if !ok || asg.Leader == "" {
			lastErr = fmt.Errorf("No leader found %d", partition)
			time.Sleep(1 * time.Second)
			continue
		}

		// dial the leader
		conn, err := dial(cl.RPCAddrOf(asg.Leader))
		if err != nil {
			lastErr = err
			time.Sleep(1 * time.Second)
			continue
		}

		// sending message
		var reply rpctypes.ProduceBatchReply
		err = conn.Call("RPC.ProduceBatch", rpctypes.ProduceBatchArgs{Topic: topic, Partition: partition, Records: recs}, &reply)
		conn.Close()
		if err != nil {
			lastErr = err
			time.Sleep(1 * time.Second)
			continue
		}
		return reply.BaseOffset, nil
	}
	return 0, fmt.Errorf("Failed to send the batch : %w", lastErr)
}

/**
 * This function read messages
 *
 * @param topic the topic id
 * @param partition the partition
 * @param offset the offset
 * @param max the maximum number of records
 * @return the fetch reply
 */
func (c *Client) Fetch(topic string, partition int, offset uint64, max int) (*rpctypes.FetchReply, error) {

	// retry multiple times
	var lastErr error
	for attempt := 0; attempt < 8; attempt++ {

		// refresh the metadata
		cl, err := c.Metadata()
		if err != nil {
			lastErr = err
			time.Sleep(1 * time.Second)
			continue
		}

		// getting leader
		asg, ok := cl.Partitions[partition]
		if !ok || asg.Leader == "" {
			lastErr = fmt.Errorf("No leader found : %d", partition)
			time.Sleep(1 * time.Second)
			continue
		}

		// dial the leader
		conn, err := dial(cl.RPCAddrOf(asg.Leader))
		if err != nil {
			lastErr = err
			time.Sleep(1 * time.Second)
			continue
		}

		// reading message
		var reply rpctypes.FetchReply
		err = conn.Call("RPC.Fetch", rpctypes.FetchArgs{Topic: topic, Partition: partition, Offset: offset, Max: max}, &reply)
		conn.Close()
		if err != nil {
			lastErr = err
			time.Sleep(1 * time.Second)
			continue
		}
		return &reply, nil
	}
	return nil, fmt.Errorf("Reading has failed : %w", lastErr)
}

/**
 * This function ask one specific broker status
 *
 * @param addr the broker rpc address
 * @return the health reply
 */
func (c *Client) Health(addr string) (*rpctypes.HealthReply, error) {

	// dial the broker directly
	conn, err := dial(addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	// ask for the health
	var reply rpctypes.HealthReply
	if err := conn.Call("RPC.Health", rpctypes.HealthArgs{}, &reply); err != nil {
		return nil, err
	}
	return &reply, nil
}

/**
 * This function return the list of broker addresses
 *
 * @return the broker rpc addresses
 */
func (c *Client) Brokers() []string { return c.brokers }
