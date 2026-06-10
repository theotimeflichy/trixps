package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"trixps/internal/client"
	"trixps/internal/hashing"
)

/**
 * gateway represent the program
 *
 * c is the client
 * mu the mutex
 * parts number of partitions
 */
type gateway struct {
	c     *client.Client
	mu    sync.Mutex
	parts int
}

func main() {

	// read args
	var (
		brokersFlag = flag.String("brokers", "broker1:9001,broker2:9002,broker3:9003", "list of brokers")
		listen      = flag.String("listen", ":8080", "http address of api")
	)
	flag.Parse()

	// building client
	g := &gateway{c: client.New(splitCSV(*brokersFlag))}

	// register api routtes
	mux := http.NewServeMux()
	mux.HandleFunc("/publish", g.handlePublish)
	mux.HandleFunc("/consume", g.handleConsume)
	mux.HandleFunc("/health", g.handleHealth)

	// startingserver
	log.Printf("API UP %s (brokers=%s)", *listen, *brokersFlag)
	if err := http.ListenAndServe(*listen, mux); err != nil {
		log.Fatalf("API Failed : %v", err)
	}
}

/**
 * This function return the number of partitions
 *
 * @return number of partitions
 */
func (g *gateway) numPartitions() int {

	// get from cache if possibble
	g.mu.Lock()
	n := g.parts
	g.mu.Unlock()
	if n > 0 {
		return n
	}

	// else get from metadata
	cl, err := g.c.Metadata()
	if err == nil && cl.NumPartitions > 0 {
		n = cl.NumPartitions
	} else {
		n = 2
	}

	// setup in cache
	g.mu.Lock()
	g.parts = n
	g.mu.Unlock()
	return n
}

/**
 * This function publish a message
 *
 * @param w the response
 * @param r the request, body = {topic, key, value}
 */
func (g *gateway) handlePublish(w http.ResponseWriter, r *http.Request) {

	// setup cors and check preflight
	if cors(w, r) {
		return
	}

	// check method post
	if r.Method != http.MethodPost {
		http.Error(w, "Only post method is allowed", http.StatusMethodNotAllowed)
		return
	}

	// get the body
	var body struct {
		Topic string `json:"topic"`
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "wrong format of a json", http.StatusBadRequest)
		return
	}

	// sending message
	p := hashing.PartitionFor(body.Key, g.numPartitions())
	off, err := g.c.Produce(body.Topic, p, body.Key, body.Value)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}

	// return offset
	writeJSON(w, map[string]any{"offset": off, "partition": p})
}

/**
 * This function read message
 *
 * @param w the response writer
 * @param r the request
 */
func (g *gateway) handleConsume(w http.ResponseWriter, r *http.Request) {

	// setup cors and check preflight
	if cors(w, r) {
		return
	}

	// reading url
	q := r.URL.Query()
	topic := q.Get("topic")
	key := q.Get("key")
	offset, _ := strconv.ParseUint(q.Get("offset"), 10, 64)
	max, _ := strconv.Atoi(q.Get("max"))
	if max <= 0 {
		max = 100
	}

	// get partition to listen and fetching
	p := hashing.PartitionFor(key, g.numPartitions())
	reply, err := g.c.Fetch(topic, p, offset, max)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}

	// translating
	recs := make([]map[string]any, 0, len(reply.Records))
	for _, rec := range reply.Records {
		recs = append(recs, map[string]any{
			"offset": rec.Offset,
			"key":    rec.Key,
			"value":  rec.Value,
			"tsNano": rec.TsNano,
		})
	}

	// return records
	writeJSON(w, map[string]any{"records": recs, "nextOffset": reply.NextOffset})
}

/**
 * This function is hear to check if up
 *
 * @param w the response writer
 * @param r the request
 */
func (g *gateway) handleHealth(w http.ResponseWriter, r *http.Request) {

	// setup cors
	if cors(w, r) {
		return
	}

	// reply we are up
	writeJSON(w, map[string]any{"ok": true})
}

/**
 * This function set the cors headers
 *
 * @param w the response writer
 * @param r the request
 * @return true if the request was a preflight already answered
 */
func cors(w http.ResponseWriter, r *http.Request) bool {

	// allow any origin so the browser app can call us
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

	// stop here on a preflight request
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return true
	}

	return false
}

/**
 * This function write a value as a json response
 *
 * @param w the response writer
 * @param v the value
 */
func writeJSON(w http.ResponseWriter, v any) {

	// encode the value as json
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

/**
 * This function split a comma list into array
 *
 * @param s the comma list
 * @return the array
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
