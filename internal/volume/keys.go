package volume

import (
	"encoding/binary"
	"fmt"
)

func PointKey(series string, timestamp int64) []byte {
	key := make([]byte, len(series)+8)
	copy(key, series)
	binary.BigEndian.PutUint64(key[len(series):], uint64(timestamp)^(1<<63))
	return key
}

func PointTime(key []byte) (int64, error) {
	if len(key) != 72 {
		return 0, fmt.Errorf("invalid point key length %d", len(key))
	}
	return int64(binary.BigEndian.Uint64(key[64:]) ^ (1 << 63)), nil
}
