package storage

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

/**
 * Record struct represent a message
 *
 * Offset is the offset in the file
 * key is the name of user
 * value is the message
 * tsnano is timestamp of sending
 */
type Record struct {
	Offset uint64
	Key    string
	Value  string
	TsNano int64
}

/**
 * Log represent the file of a partition
 *
 * mu is the mutex
 * f is the file object
 * path is the path to file in the system
 * nextOff is the offset of the next message
 * index is the map [offset -> byte]
 * size is the size of file
 */
type Log struct {
	mu      sync.Mutex
	f       *os.File
	path    string
	nextOff uint64
	index   map[uint64]int64
	size    int64
}

/**
 * This function Open a Log file, if needed it create the file.
 *
 * @param path the filepath
 * @return Object Log ready to be used
 */
func Open(path string) (*Log, error) {

	// Create file if needed
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}

	// Open file
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}

	// creating log
	l := &Log{
		f:     f,
		path:  path,
		index: make(map[uint64]int64),
	}

	// get info from file
	if err := l.replay(); err != nil {
		f.Close()
		return nil, err
	}

	return l, nil
}

/**
 * This function is replaying the full
 * file to build Log.
 *
 * @param l an objet Log
 */
func (l *Log) replay() error {

	// start at the beginning of the file
	if _, err := l.f.Seek(0, io.SeekStart); err != nil {
		return err
	}

	// read every message and building index
	r := bufio.NewReader(l.f)
	var pos int64
	for {
		rec, n, err := readRecord(r)
		if err == io.EOF {
			break
		}
		if err != nil {
			if terr := l.f.Truncate(pos); terr != nil {
				return terr
			}
			break
		}
		l.index[rec.Offset] = pos
		l.nextOff = rec.Offset + 1
		pos += int64(n)
	}

	// set cursor at the right place for next writing
	if _, err := l.f.Seek(pos, io.SeekStart); err != nil {
		return err
	}
	l.size = pos

	return nil
}

/**
 * This function is the here to close the mutex and call append.
 *
 * @param key is the user
 * @param value is the payload
 * @return the offset assigned to the new log
 */
func (l *Log) Append(key, value string) (uint64, error) {

	// close the lock
	l.mu.Lock()

	// open the lock once the function has ended
	defer l.mu.Unlock()

	// call the real append
	return l.appendLocked(l.nextOff, key, value, time.Now().UnixNano())
}

/**
 * This function is the here to close the mutex and call
 * append BUT with a required offset.
 *
 * @param offset where the record must be stored
 * @param key is the user
 * @param value is the payload
 * @param tsNano the timestamp
 */
func (l *Log) AppendAt(offset uint64, key, value string, tsNano int64) error {

	// close the lock
	l.mu.Lock()

	// open the lock once the function has ended
	defer l.mu.Unlock()

	// check if already written
	if _, ok := l.index[offset]; ok {
		return nil
	}

	// if offset is not the same as nextOff -> error
	if offset != l.nextOff {
		return fmt.Errorf("error with offset: expected %d, received %d", l.nextOff, offset)
	}

	// call the real append
	_, err := l.appendLocked(offset, key, value, tsNano)

	return err
}

/**
 * This function append a message in the log file
 *
 * @param offset of next writing
 * @param key to be written
 * @param value to be written
 * @param tsNano timestamp to be written
 */
func (l *Log) appendLocked(offset uint64, key, value string, tsNano int64) (uint64, error) {

	// encode message to write format.
	buf := encodeRecord(Record{Offset: offset, Key: key, Value: value, TsNano: tsNano})

	// write in file
	if _, err := l.f.WriteAt(buf, l.size); err != nil {
		return 0, err
	}

	// force to hard write on disk
	if err := l.f.Sync(); err != nil {
		return 0, err
	}

	// set new offset and size
	l.index[offset] = l.size
	l.size += int64(len(buf))
	if offset >= l.nextOff {
		l.nextOff = offset + 1
	}

	return offset, nil
}

/**
 * The function returns up to max records starting from the offset.
 *
 * @param offset of where to start reading
 * @param max the number of max record to read and return
 * @return file content between offset and max
 */
func (l *Log) ReadFrom(offset uint64, max int) ([]Record, error) {

	// close the lock
	l.mu.Lock()

	// open the lock once the function has ended
	defer l.mu.Unlock()

	// get where to read in the file (offset -> byte)
	pos, ok := l.index[offset]

	// if doesn't exist -> stop here
	if !ok {
		if offset >= l.nextOff {
			return nil, nil
		}
		pos = 0
	}

	// set the cursor at the right place
	if _, err := l.f.Seek(pos, io.SeekStart); err != nil {
		return nil, err
	}

	// reading the file
	r := bufio.NewReader(l.f)
	var out []Record
	for {

		// read the following record
		rec, _, err := readRecord(r)

		// if end of file -> stop
		if err == io.EOF {
			break
		}

		// if err -> stop
		if err != nil {
			return out, err
		}

		// if before wanted offset, jump reading
		if rec.Offset < offset {
			continue
		}

		// add message
		out = append(out, rec)
		if max > 0 && len(out) >= max {
			break
		}
	}

	return out, nil
}

/**
 * This function return the next offset
 *
 * @return the next offset
 */
func (l *Log) NextOffset() uint64 {

	// close the lock
	l.mu.Lock()

	// open the lock once the function has ended
	defer l.mu.Unlock()

	// return the next offset
	return l.nextOff
}

/**
 * This function is closing the file from Log object
 *
 * @return result of closing
 */
func (l *Log) Close() error {

	// close the lock
	l.mu.Lock()

	// open the lock once the function has ended
	defer l.mu.Unlock()

	// we close the file
	return l.f.Close()
}

/**
 * This function encode the message through the right format
 * final size if (8 + 4 + len(key) + 4 + len(val) + 8 + 4)
 *
 * @param the record
 * @return the encoded record in byte
 */
func encodeRecord(rec Record) []byte {
	key := []byte(rec.Key)
	val := []byte(rec.Value)
	buf := make([]byte, 0, 28+len(key)+len(val))
	var u64 [8]byte
	var u32 [4]byte

	binary.LittleEndian.PutUint64(u64[:], rec.Offset)
	buf = append(buf, u64[:]...)
	binary.LittleEndian.PutUint32(u32[:], uint32(len(key)))
	buf = append(buf, u32[:]...)
	buf = append(buf, key...)
	binary.LittleEndian.PutUint32(u32[:], uint32(len(val)))
	buf = append(buf, u32[:]...)
	buf = append(buf, val...)
	binary.LittleEndian.PutUint64(u64[:], uint64(rec.TsNano))
	buf = append(buf, u64[:]...)

	// build crc to check validity of data
	crc := crc32.ChecksumIEEE(buf)
	binary.LittleEndian.PutUint32(u32[:], crc)
	buf = append(buf, u32[:]...)
	return buf
}

/**
 * This function decode the message
 *
 * @param reader object
 * @return the Record object, size, error
 */
func readRecord(r *bufio.Reader) (Record, int, error) {
	header := make([]byte, 8+4)
	if _, err := io.ReadFull(r, header); err != nil {
		if err == io.EOF {
			return Record{}, 0, io.EOF
		}
		if err == io.ErrUnexpectedEOF {
			return Record{}, 0, io.ErrUnexpectedEOF
		}
		return Record{}, 0, err
	}
	offset := binary.LittleEndian.Uint64(header[0:8])
	keyLen := binary.LittleEndian.Uint32(header[8:12])

	key := make([]byte, keyLen)
	if _, err := io.ReadFull(r, key); err != nil {
		return Record{}, 0, io.ErrUnexpectedEOF
	}
	var vlBuf [4]byte
	if _, err := io.ReadFull(r, vlBuf[:]); err != nil {
		return Record{}, 0, io.ErrUnexpectedEOF
	}
	valLen := binary.LittleEndian.Uint32(vlBuf[:])
	val := make([]byte, valLen)
	if _, err := io.ReadFull(r, val); err != nil {
		return Record{}, 0, io.ErrUnexpectedEOF
	}
	var tsBuf [8]byte
	if _, err := io.ReadFull(r, tsBuf[:]); err != nil {
		return Record{}, 0, io.ErrUnexpectedEOF
	}
	tsNano := int64(binary.LittleEndian.Uint64(tsBuf[:]))
	var crcBuf [4]byte
	if _, err := io.ReadFull(r, crcBuf[:]); err != nil {
		return Record{}, 0, io.ErrUnexpectedEOF
	}
	gotCRC := binary.LittleEndian.Uint32(crcBuf[:])

	rec := Record{Offset: offset, Key: string(key), Value: string(val), TsNano: tsNano}

	// check crc
	want := crc32.ChecksumIEEE(encodeRecord(rec)[:8+4+int(keyLen)+4+int(valLen)+8])
	if gotCRC != want {
		return Record{}, 0, fmt.Errorf("invalide crc (offset %d)", offset)
	}
	total := 8 + 4 + int(keyLen) + 4 + int(valLen) + 8 + 4
	return rec, total, nil
}
