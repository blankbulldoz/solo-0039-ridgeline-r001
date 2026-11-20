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
	err := d.Raw.View(func(tx *bolt.Tx) error {
		var output bytes.Buffer
		if _, err := tx.WriteTo(&output); err != nil {
			return err
		}
		image.Data = output.Bytes()
		return nil
	})
	if err != nil {
		return Image{}, err
	}
	digest := sha256.Sum256(image.Data)
	image.SHA256 = hex.EncodeToString(digest[:])
	if err := d.Raw.View(func(tx *bolt.Tx) error {
		image.Sequence = Current(tx)
		return nil
	}); err != nil {
		return Image{}, err
	}
	return image, nil
}
