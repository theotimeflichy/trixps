package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"trixps/internal/client"
)

func main() {

	// read args
	var (
		brokersFlag = flag.String("brokers", "localhost:9001", "other rpc")
		topic       = flag.String("topic", "demo", "topic id")
		offsetFile  = flag.String("offset-file", "", "offset file")
		partitions  = flag.Int("partitions", 0, "number of partition")
		max         = flag.Int("max", 0, "number of maximum message to listen")
		follow      = flag.Bool("follow", false, "stay on listening ?")
	)
	flag.Parse()

	// build new client
	c := client.New(splitCSV(*brokersFlag))

	// find number of partitions
	n := *partitions
	if n == 0 {
		cl, err := c.Metadata()
		if err != nil {
			log.Fatalf("finding parition numbers failed : %v", err)
		}
		n = cl.NumPartitions
		if n == 0 {
			n = 2
		}
	}

	// setup offset
	offPath := *offsetFile
	if offPath == "" {
		offPath = fmt.Sprintf("/tmp/trixps-offsets-%s.json", *topic)
	}

	// load the saved offsets
	offsets := loadOffsets(offPath, n)
	read := 0
	if *follow {
		fmt.Printf("Listening topic %q (%d partitions). Waiting for messages...\n", *topic, n)
	}

	// fetch from every partition in a loop
	for {
		progressed := false
		for p := 0; p < n; p++ {

			// get record from offset
			reply, err := c.Fetch(*topic, p, offsets[p], 100)
			if err != nil {
				log.Printf("fetch partition %d: %v", p, err)
				continue
			}

			// print records and move offset
			for _, r := range reply.Records {
				fmt.Printf("topic=%s partition=%d offset=%d key=%q value=%q\n", *topic, p, r.Offset, r.Key, r.Value)
				offsets[p] = r.Offset + 1
				read++
				progressed = true

				// stop once the --max limit is reached
				if *max > 0 && read >= *max {
					saveOffsets(offPath, offsets)
					return
				}
			}

			// store new offset
			if reply.NextOffset > offsets[p] {
				offsets[p] = reply.NextOffset
			}
		}

		// save the offsets
		saveOffsets(offPath, offsets)

		// one message mode
		if !*follow {
			if !progressed {
				return
			}
			continue
		}

		// waiting for 1s
		time.Sleep(1 * time.Second)
	}
}

/**
 * This function read the offset file and give a map partition -> next offset
 *
 * @param path the file
 * @param n the number of partitions
 * @return the map
 */
func loadOffsets(path string, n int) map[int]uint64 {

	// init map to 0
	offsets := make(map[int]uint64, n)
	for p := 0; p < n; p++ {
		offsets[p] = 0
	}

	// reading fie
	data, err := os.ReadFile(path)
	if err != nil {
		return offsets
	}

	// translate from json to map
	raw := map[string]uint64{}
	if json.Unmarshal(data, &raw) == nil {
		for p := 0; p < n; p++ {
			if v, ok := raw[fmt.Sprintf("%d", p)]; ok {
				offsets[p] = v
			}
		}
	}
	return offsets
}

/**
 * This function save the map into the json
 *
 * @param path the path of file
 * @param offsets the map
 */
func saveOffsets(path string, offsets map[int]uint64) {

	// from map to string
	raw := map[string]uint64{}
	for p, o := range offsets {
		raw[fmt.Sprintf("%d", p)] = o
	}

	// save on file
	data, _ := json.Marshal(raw)
	_ = os.WriteFile(path, data, 0o644)
}

/**
 * This function split a comma string into list
 *
 *
 * @param s the comma separated input
 * @return list
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
