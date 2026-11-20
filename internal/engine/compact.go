package engine

import (
	"ridgeline/internal/model"
	"ridgeline/internal/policy"
	"ridgeline/internal/volume"

	bolt "go.etcd.io/bbolt"
)

func compact(tx *bolt.Tx, b *bolt.Bucket, n model.Namespace) (model.Maintenance, error) {
	// Maintenance is bounded by the same scan policy as user queries.
	raw, err := scan(b, n, model.Query{Start: model.MinTime, End: model.MaxTime + 1}, volume.Current(tx))
	if err != nil {
		return model.Maintenance{}, err
	}
	for _, bucket := range [][]byte{volume.Head, volume.Blocks, volume.Payloads, volume.Deletes} {
		if err := clearBucket(b, bucket); err != nil {
			return model.Maintenance{}, err
		}
	}
	seq, err := volume.Next(tx)
	if err != nil {
		return model.Maintenance{}, err
	}
	receipt := model.Maintenance{Sequence: seq, Floor: n.Floor}
	for _, s := range raw.Series {
		if err := putBlock(b, s.Series.ID, seq, s.Points); err != nil {
			return receipt, err
		}
		receipt.Blocks++
		receipt.Points += len(s.Points)
	}
	n.LastSequence = seq
	return receipt, volume.PutJSON(b, volume.Config, n)
}

func (e *Engine) Compact(name string) (model.Maintenance, error) {
	var receipt model.Maintenance
	err := e.DB.Raw.Update(func(tx *bolt.Tx) error {
		b, n, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		receipt, err = compact(tx, b, n)
		return err
	})
	return receipt, err
}

func (e *Engine) Retain(name string, now int64) (model.Maintenance, error) {
	var receipt model.Maintenance
	err := e.DB.Raw.Update(func(tx *bolt.Tx) error {
		b, n, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		if n.ReadOnly {
			return model.Conflict("namespace is read-only")
		}
		n.Floor, err = policy.RetentionFloor(n, now)
		if err != nil {
			return err
		}
		receipt, err = compact(tx, b, n)
		return err
	})
	return receipt, err
}
