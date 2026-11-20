package engine

import (
	"encoding/json"

	"ridgeline/internal/model"
	"ridgeline/internal/volume"

	bolt "go.etcd.io/bbolt"
)

type Engine struct {
	DB *volume.DB
}

func Open(path string) (*Engine, error) {
	db, err := volume.Open(path)
	if err != nil {
		return nil, err
	}
	return &Engine{DB: db}, nil
}

func (e *Engine) Close() error {
	return e.DB.Close()
}

func loadNamespace(tx *bolt.Tx, name string) (*bolt.Bucket, model.Namespace, error) {
	b := volume.Namespace(tx, name)
	var n model.Namespace
	if b == nil {
		return nil, n, model.Missing("namespace does not exist")
	}
	if err := volume.GetJSON(b, volume.Config, &n); err != nil {
		return nil, n, err
	}
	return b, n, nil
}

func catalog(b *bolt.Bucket) ([]model.Series, error) {
	out := make([]model.Series, 0)
	err := b.Bucket(volume.Catalog).ForEach(func(k, v []byte) error {
		var s model.Series
		if err := json.Unmarshal(v, &s); err != nil {
			return err
		}
		out = append(out, s)
		return nil
	})
	return out, err
}

func (e *Engine) Create(n model.Namespace) (model.Namespace, error) {
	if err := model.ValidateNamespace(n); err != nil {
		return n, err
	}
	n.Floor, n.Watermark, n.HasWatermark = model.MinTime, 0, false
	err := e.DB.Raw.Update(func(tx *bolt.Tx) error {
		if volume.Namespace(tx, n.Name) != nil {
			return model.Conflict("namespace already exists")
		}
		b, err := volume.CreateNamespace(tx, n.Name)
		if err != nil {
			return err
		}
		n.LastSequence, err = volume.Next(tx)
		if err != nil {
			return err
		}
		return volume.PutJSON(b, volume.Config, n)
	})
	return n, err
}

func (e *Engine) Namespace(name string) (model.Namespace, error) {
	var n model.Namespace
	err := e.DB.Raw.View(func(tx *bolt.Tx) error {
		_, loaded, err := loadNamespace(tx, name)
		n = loaded
		return err
	})
	return n, err
}

func (e *Engine) ReadOnly(name string, readOnly bool) (model.Namespace, error) {
	var n model.Namespace
	err := e.DB.Raw.Update(func(tx *bolt.Tx) error {
		b, loaded, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		n = loaded
		n.ReadOnly = readOnly
		n.LastSequence, err = volume.Next(tx)
		if err != nil {
			return err
		}
		return volume.PutJSON(b, volume.Config, n)
	})
	return n, err
}
