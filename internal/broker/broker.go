package broker

import (
	"encoding/json"
	"fmt"
	"net"
	netrpc "net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"

	"trixps/internal/metadata"
	"trixps/internal/raftfsm"
	"trixps/internal/storage"
)

/**
 * Peer represent one broker of the cluster
 *
 * ID is the id of the broker
 * RaftAddr is the raft address
 * RPCAddr is the rpc address
 */
type Peer struct {
	ID       string
	RaftAddr string
	RPCAddr  string
}

/**
 * Config represent the config of one broker
 *
 * ID is the id of the broker
 * RaftAddr is the raft address (controle plane)
 * RPCAddr is the rpc address (data plane)
 * DataDir is the folder to store
 * Peers is the list of brokers
 * Bootstrap is true if first broker to start
 * NumPartitions is the number of partitions
 */
type Config struct {
	ID            string
	RaftAddr      string
	RPCAddr       string
	DataDir       string
	Peers         []Peer
	Bootstrap     bool
	NumPartitions int
}

/**
 * Broker represent a running node
 *
 * cfg is the config
 * fsm is the raft state machine
 * raft is the raft node
 * peers is the map [id -> peer]
 * mu is the mutex
 * logs is the map [topic/partition -> log]
 * prodLocks is the map [topic/partition -> lock] to serialize writes
 * clientsMu is the mutex protecting clients
 * clients is the cache of rpc clients to the followers
 */
type Broker struct {
	cfg   Config
	fsm   *raftfsm.FSM
	raft  *raft.Raft
	peers map[string]Peer

	mu        sync.Mutex
	logs      map[string]*storage.Log
	prodLocks map[string]*sync.Mutex

	clientsMu sync.Mutex
	clients   map[string]*netrpc.Client
}

/**
 * This function start a broker node
 *
 * @param cfg the config
 * @return the broker
 */
func Open(cfg Config) (*Broker, error) {

	// default to 2 partitions
	if cfg.NumPartitions == 0 {
		cfg.NumPartitions = 2
	}

	// build the broker object
	b := &Broker{
		cfg:       cfg,
		fsm:       raftfsm.New(),
		peers:     map[string]Peer{},
		logs:      map[string]*storage.Log{},
		prodLocks: map[string]*sync.Mutex{},
		clients:   map[string]*netrpc.Client{},
	}

	// fill the map of the peers
	for _, p := range cfg.Peers {
		b.peers[p.ID] = p
	}

	// start raft
	if err := b.setupRaft(); err != nil {
		return nil, err
	}

	// start RPC
	if err := b.serveRPC(); err != nil {
		return nil, err
	}

	// start the control loop
	go b.controlLoop()

	return b, nil
}

/**
 * This function build and start the raft node of the broker.
 *
 * @return nil if ok
 */
func (b *Broker) setupRaft() error {

	// base raft config
	rcfg := raft.DefaultConfig()
	rcfg.LocalID = raft.ServerID(b.cfg.ID)

	// setuptimeouts
	rcfg.HeartbeatTimeout = 1000 * time.Millisecond
	rcfg.ElectionTimeout = 1000 * time.Millisecond
	rcfg.LeaderLeaseTimeout = 500 * time.Millisecond
	rcfg.CommitTimeout = 200 * time.Millisecond

	// create the raft folder
	raftDir := filepath.Join(b.cfg.DataDir, "raft")
	if err := os.MkdirAll(raftDir, 0o755); err != nil {
		return err
	}

	// store on disk for the raft log
	store, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "raft.db"))
	if err != nil {
		return err
	}

	// store for the snapshots
	snaps, err := raft.NewFileSnapshotStore(raftDir, 2, os.Stderr)
	if err != nil {
		return err
	}

	// tcp transport on the raft address
	advertise, err := net.ResolveTCPAddr("tcp", b.cfg.RaftAddr)
	if err != nil {
		return err
	}
	transport, err := raft.NewTCPTransport(b.cfg.RaftAddr, advertise, 4, 10*time.Second, os.Stderr)
	if err != nil {
		return err
	}

	// create the raft node
	r, err := raft.NewRaft(rcfg, b.fsm, store, store, snaps, transport)
	if err != nil {
		return err
	}
	b.raft = r

	// only the bootstrap node start the cluster (with the --bootstrap flahg)
	if b.cfg.Bootstrap {
		var servers []raft.Server
		for _, p := range b.cfg.Peers {
			servers = append(servers, raft.Server{
				ID:      raft.ServerID(p.ID),
				Address: raft.ServerAddress(p.RaftAddr),
			})
		}
		b.raft.BootstrapCluster(raft.Configuration{Servers: servers})
	}

	return nil
}

/**
 * This function register the rpc service and accept the clients.
 *
 * @return nil if the listener is ready
 */
func (b *Broker) serveRPC() error {

	// register the rpc methods
	srv := netrpc.NewServer()
	if err := srv.RegisterName("RPC", &RPCService{b: b}); err != nil {
		return err
	}

	// listening
	ln, err := net.Listen("tcp", b.cfg.RPCAddr)
	if err != nil {
		return err
	}

	// accept the connections
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go srv.ServeConn(conn)
		}
	}()

	return nil
}

/**
 * This function build the map key of a partition log
 *
 * @param topic the topic id
 * @param p the partition
 * @return the key "topic/partition"
 */
func logKey(topic string, p int) string { return topic + "/" + strconv.Itoa(p) }

/**
 * This function turn a topic name into a valid name of a file
 *
 * @param t the topic name
 * @return the name of a file
 */
func sanitizeTopic(t string) string {

	var sb strings.Builder
	for _, r := range t {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			sb.WriteRune(r)
		default:
			sb.WriteRune('_')
		}
	}

	// if empty
	if sb.Len() == 0 {
		return "_"
	}
	return sb.String()
}

/**
 * This function return the log of a (topic, partition)
 *
 * @param topic the topic id
 * @param p the partition
 * @return the log of the (topic, partition)
 */
func (b *Broker) logFor(topic string, p int) (*storage.Log, error) {

	// no empty topic
	if topic == "" {
		return nil, fmt.Errorf("topic if empty")
	}
	key := logKey(topic, p)

	// close the lock
	b.mu.Lock()

	// open lock whenever func is over
	defer b.mu.Unlock()

	// if the log is already open
	if lg, ok := b.logs[key]; ok {
		return lg, nil
	}

	// else we open the file and keep it in the map
	fname := fmt.Sprintf("%s-partition-%d.log", sanitizeTopic(topic), p)
	lg, err := storage.Open(filepath.Join(b.cfg.DataDir, fname))
	if err != nil {
		return nil, err
	}
	b.logs[key] = lg
	return lg, nil
}

/**
 * This function return the lock for a specific partition of a topic
 *
 * @param topic the topic
 * @param p the partition
 * @return the lock
 */
func (b *Broker) produceLock(topic string, p int) *sync.Mutex {
	key := logKey(topic, p)

	// close the lock
	b.mu.Lock()

	// re open lock when function is over
	defer b.mu.Unlock()

	// create the lock
	m, ok := b.prodLocks[key]
	if !ok {
		m = &sync.Mutex{}
		b.prodLocks[key] = m
	}
	return m
}

/**
 * This function tell if this broker is the leader of a partition
 *
 * @param p the partition
 * @return true if is the leader of partition
 */
func (b *Broker) isLeaderOf(p int) bool {
	a, ok := b.fsm.Cluster().Partitions[p]
	return ok && a.Leader == b.cfg.ID
}

/**
 * This function return a cached rpc client to an address fi it is already existing
 *
 * @param addr the address
 * @return a rpc client to the address
 */
func (b *Broker) dial(addr string) (*netrpc.Client, error) {

	// close the lock
	b.clientsMu.Lock()

	// re open mutex when over
	defer b.clientsMu.Unlock()

	// check if a connection is already oepn
	if c, ok := b.clients[addr]; ok && c != nil {
		return c, nil
	}

	// else dial the address
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return nil, err
	}

	// keep the new client in the cache
	c := netrpc.NewClient(conn)
	b.clients[addr] = c
	return c, nil
}

/**
 * This function remove the cached client of an address
 *
 * @param addr the address
 */
func (b *Broker) dropClient(addr string) {

	// close the lock
	b.clientsMu.Lock()

	// re open mutex when over
	defer b.clientsMu.Unlock()

	// if the client exist, close it and remove it
	if c, ok := b.clients[addr]; ok {
		c.Close()
		delete(b.clients, addr)
	}
}

/**
 * This function return the raft state of the node (Leader/Follower/Candidate)
 *
 * @return the raft state
 */
func (b *Broker) raftState() string { return b.raft.State().String() }

/**
 * This function send a command to the raft log
 *
 * @param cmd the metadata command
 * @return nil if the command was applied
 */
func (b *Broker) applyCommand(cmd raftfsm.Command) error {

	// encode the command
	data, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	// propose it to raft and wait for the result
	f := b.raft.Apply(data, 5*time.Second)
	return f.Error()
}

/**
 * This function return the peer ids sorted
 *
 * @return the sorted peer ids (deterministic)
 */
func (b *Broker) sortedPeerIDs() []string {
	var ids []string
	for id := range b.peers {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

/**
 * This function return the current cluster metadata
 *
 * @return the cluster metadata
 */
func (b *Broker) Cluster() *metadata.Cluster { return b.fsm.Cluster() }

/**
 * This function stop the node cleanly
 *
 * @return nil if ok
 */
func (b *Broker) Close() error {

	// stop raft
	if b.raft != nil {
		b.raft.Shutdown().Error()
	}

	// close the lock
	b.mu.Lock()

	// re open lock mutex once func is over
	defer b.mu.Unlock()

	// close every open log
	for _, lg := range b.logs {
		lg.Close()
	}
	return nil
}
