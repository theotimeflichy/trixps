package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"trixps/internal/broker"
)

func main() {

	// we read the arguments
	var (
		id         = flag.String("id", "", "Id of broker")
		raftAddr   = flag.String("raft-addr", "", "Raft (host:port)")
		rpcAddr    = flag.String("rpc-addr", "", "RPC (host:port)")
		peersFlag  = flag.String("peers", "", "brokers (id@raftAddr@rpcAddr,...)")
		dataDir    = flag.String("data-dir", "/data", "docker volum")
		partitions = flag.Int("partitions", 2, "partition numbers")
		bootstrap  = flag.Bool("bootstrap", false, "First node to be started ?")
	)
	flag.Parse()

	// we check if we have the min config asked
	if *id == "" || *raftAddr == "" || *rpcAddr == "" || *peersFlag == "" {
		log.Fatal("Missing --id and --raft-addr and --rpc-addr an --peers")
	}

	// we parse the peers
	peers, err := parsePeers(*peersFlag)
	if err != nil {
		log.Fatalf("--peers is invalid : %v", err)
	}

	// we open the broker
	b, err := broker.Open(broker.Config{
		ID:            *id,
		RaftAddr:      *raftAddr,
		RPCAddr:       *rpcAddr,
		DataDir:       *dataDir,
		Peers:         peers,
		Bootstrap:     *bootstrap,
		NumPartitions: *partitions,
	})
	if err != nil {
		log.Fatalf("Starting broker : %v", err)
	}
	log.Printf("broker %s is up (raft=%s rpc=%s data=%s)", *id, *raftAddr, *rpcAddr, *dataDir)

	// we close the node when asked
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Printf("Stoping brokerr %s", *id)
	b.Close()
}

/**
 * This function parse the flag --peers in a list of brokers
 *
 * @param s the content of --peers flag
 * @return the parsed list
 */
func parsePeers(s string) ([]broker.Peer, error) {

	// we split the peers
	var peers []broker.Peer
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)

		// skip if it is empty
		if part == "" {
			continue
		}

		// we chekc validity of the peer
		fields := strings.Split(part, "@")
		if len(fields) != 3 {
			return nil, errf(part)
		}
		peers = append(peers, broker.Peer{ID: fields[0], RaftAddr: fields[1], RPCAddr: fields[2]})
	}
	return peers, nil
}

/**
 * This function build a parse error for a bad peers flag input
 *
 * @param p the bad entry
 * @return an error
 */
func errf(p string) error { return &parseErr{p} }

/**
 * parseErr represent one bad --peers entry
 *
 * part is the failed entry
 */
type parseErr struct{ part string }

/**
 * This function turn the parse error into a readable message for the user
 *
 * @return a message
 */
func (e *parseErr) Error() string {
	return "format asked id@raftAddr@rpcAddr, received: " + e.part
}
