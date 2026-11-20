package engine

import (
	"ridgeline/internal/model"
	"ridgeline/internal/volume"

	bolt "go.etcd.io/bbolt"
)

func (e *Engine) Materialize(name, rollup string, q model.Query) (model.Materialization, error) {
	var out model.Materialization
	if !model.ValidName(rollup) {
		return out, model.Bad("invalid rollup name")
	}
	err := e.DB.Raw.Update(func(tx *bolt.Tx) error {
		b, n, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		if n.ReadOnly {
			return model.Conflict("namespace is read-only")
		}
		result, err := aggregate(b, n, q, volume.Current(tx))
		if err != nil {
			return err
		}
		out = model.Materialization{
			Name: rollup, Query: q, SourceSequence: n.LastSequence, Result: result,
		}
		// Rollup metadata does not change the source namespace sequence.
		if _, err := volume.Next(tx); err != nil {
			return err
		}
		return volume.PutJSON(b.Bucket(volume.Rollups), []byte(rollup), out)
	})
	return out, err
}

func (e *Engine) Rollup(name, rollup string) (model.RollupView, error) {
	var out model.RollupView
	err := e.DB.Raw.View(func(tx *bolt.Tx) error {
		b, n, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		if b.Bucket(volume.Rollups).Get([]byte(rollup)) == nil {
			return model.Missing("rollup does not exist")
		}
		if err := volume.GetJSON(b.Bucket(volume.Rollups), []byte(rollup), &out.Materialization); err != nil {
			return err
		}
		out.Stale = out.SourceSequence != n.LastSequence
		return nil
	})
	return out, err
}
