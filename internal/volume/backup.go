package volume

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"

	bolt "go.etcd.io/bbolt"
)

type Image struct {
	Data     []byte
	Sequence uint64
	SHA256   string
}

func (d *DB) Snapshot() (Image, error) {
	var image Image
	// The image bytes and the advertised sequence must come from the same
	// database snapshot. tx.WriteTo serializes this read transaction's MVCC
	// view, so reading Current(tx) inside that same transaction guarantees
	// that every commit with sequence <= image.Sequence is actually present
	// in image.Data. A second transaction afterwards could observe commits
	// that landed after the snapshot, advertising a sequence the file does
	// not contain.
	err := d.Raw.View(func(tx *bolt.Tx) error {
		var output bytes.Buffer
		if _, err := tx.WriteTo(&output); err != nil {
			return err
		}
		image.Data = output.Bytes()
		image.Sequence = Current(tx)
		return nil
	})
	if err != nil {
		return Image{}, err
	}
	digest := sha256.Sum256(image.Data)
	image.SHA256 = hex.EncodeToString(digest[:])
	return image, nil
}
