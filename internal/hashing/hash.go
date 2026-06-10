package hashing

import "hash/fnv"

/**
 * This function return the partition number from the key.
 *
 * @param key the key used in the topic
 * @param n the number of partition
 * @return the partition number where this key is going
 */
func PartitionFor(key string, n int) int {

	// if only 1 part -> partition number 0
	if n <= 1 {
		return 0
	}

	// calculate the partition number
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(n))
}
