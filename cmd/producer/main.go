package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"trixps/internal/client"
	"trixps/internal/hashing"
	rpctypes "trixps/internal/rpc"
)

func main() {

	// read args
	var (
		brokersFlag = flag.String("brokers", "localhost:9001", "list of brokers")
		topic       = flag.String("topic", "demo", "topic id")
		key         = flag.String("key", "", "message key")
		value       = flag.String("value", "", "message content")
		count       = flag.Int("count", 0, "number of message to publish")
		batchSize   = flag.Int("batch-size", 100, "max size of a batch")
		status      = flag.Bool("status", false, "show status of each broker")
		interactive = flag.Bool("i", false, "open a console to send multiple message")
	)
	flag.Parse()

	// build new client
	brokers := splitCSV(*brokersFlag)
	c := client.New(brokers)

	// if status mode we just print status of each broker
	if *status {
		printStatus(c)
		return
	}

	// getting number of partition
	cl, err := c.Metadata()
	if err != nil {
		log.Fatalf("service discovery impossible: %v", err)
	}
	n := cl.NumPartitions
	if n == 0 {
		n = 2
	}

	// if interactive : we start the console
	if *interactive {
		runInteractive(c, *topic, n)
		return
	}

	// if batch : we publish messages
	if *count > 0 {
		publishBatch(c, *topic, *count, n, *batchSize)
		return
	}

	// else we only send one message
	publish(c, *topic, *key, *value, n)
}

/**
 * This function generate N messages and send it
 *
 * @param c the client
 * @param topic the topic key
 * @param count the number of messages to generate
 * @param n the number of partitions
 * @param batchSize the maximum number of message for the batch
 */
func publishBatch(c *client.Client, topic string, count, n, batchSize int) {

	if batchSize < 1 {
		batchSize = 1
	}

	// generate message and partitionate
	byPart := make(map[int][]rpctypes.BatchRecord)
	for i := 1; i <= count; i++ {
		k := fmt.Sprintf("k%d", i)
		v := fmt.Sprintf("v%d", i)
		p := hashing.PartitionFor(k, n)
		byPart[p] = append(byPart[p], rpctypes.BatchRecord{Key: k, Value: v})
	}

	// send batchs per partition
	total, rpcs := 0, 0
	for p := 0; p < n; p++ {
		recs := byPart[p]
		for start := 0; start < len(recs); start += batchSize {

			end := start + batchSize
			if end > len(recs) {
				end = len(recs)
			}
			chunk := recs[start:end]

			// sending
			base, err := c.ProduceBatch(topic, p, chunk)
			if err != nil {
				log.Fatalf("error while sending (partition %d): %v", p, err)
			}
			rpcs++
			total += len(chunk)
			fmt.Printf("batch sent: topic=%s partition=%d base=%d n=%d \n", topic, p, base, len(chunk))
		}
	}
	fmt.Printf("=> %d message sent on %d batch\n", total, rpcs)
}

/**
 * This function read message and send
 *
 * @param c the client
 * @param topic the topic key
 * @param n the number of partitions
 */
func runInteractive(c *client.Client, topic string, n int) {

	// print the small help of the interactive mode
	fmt.Printf("Console to send (topic=%q, %d partitions). Send a message :\n", topic, n)

	// read next
	sc := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !sc.Scan() {
			break
		}

		// trimming
		line := strings.TrimRight(sc.Text(), "\r\n")
		if line == "" {
			continue
		}

		// spliting key value
		key, value := "", line
		if i := strings.Index(line, ":"); i >= 0 {
			key = strings.TrimSpace(line[:i])
			value = strings.TrimSpace(line[i+1:])
		}

		// publishing message
		p := hashing.PartitionFor(key, n)
		off, err := c.Produce(topic, p, key, value)
		if err != nil {
			fmt.Printf("failed: %v\n", err)
			continue
		}
		fmt.Printf("sent to partition=%d offset=%d key=%q\n", p, off, key)
	}
	fmt.Println("\nClosing the app..")
}

/**
 * This function send a message
 *
 * @param c the client
 * @param topic the topic id
 * @param key the message key
 * @param value the message content
 * @param n the number of partitions
 */
func publish(c *client.Client, topic, key, value string, n int) {

	p := hashing.PartitionFor(key, n)
	off, err := c.Produce(topic, p, key, value)
	if err != nil {
		log.Fatalf("error in sending message : %v", err)
	}
	fmt.Printf("OK topic=%s key=%q partition=%d offset=%d value=%q\n", topic, key, p, off, value)
}

/**
 * This function is print status of each broker
 *
 * @param c the client whose brokers are asked for health
 */
func printStatus(c *client.Client) {

	// get else of each node (broker)
	for _, addr := range c.Brokers() {
		h, err := c.Health(addr)
		if err != nil {
			fmt.Printf("broker=%s state=Unreachable err=%v\n", addr, err)
			continue
		}
		fmt.Printf("broker=%s id=%s state=%s\n", addr, h.ID, h.RaftState)
	}
}

/**
 * This function split a comma list into array
 *
 * @param s the comma list
 * @return the larray
 */
func splitCSV(s string) []string {

	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
