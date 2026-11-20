package engine

import (
	"encoding/json"

	"ridgeline/internal/chunk"
	"ridgeline/internal/model"
	"ridgeline/internal/volume"

	bolt "go.etcd.io/bbolt"
)

func putBlock(b *bolt.Bucket, id string, seq uint64, points []model.Sample) error {
	block, payload, err := chunk.Build(id, seq, points)
	if err != nil {
		return err
	}
	if err := volume.PutJSON(b.Bucket(volume.Blocks), []byte(block.ID), block); err != nil {
		return err
	}
	return b.Bucket(volume.Payloads).Put([]byte(block.ID), payload)
}

func clearBucket(parent *bolt.Bucket, name []byte) error {
	if err := parent.DeleteBucket(name); err != nil {
		return err
	}
	_, err := parent.CreateBucket(name)
	return err
}

func (e *Engine) Seal(name string) (model.Maintenance, error) {
	var receipt model.Maintenance
	err := e.DB.Raw.Update(func(tx *bolt.Tx) error {
		b, n, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		grouped := map[string][]model.Sample{}
		err = b.Bucket(volume.Head).ForEach(func(k, v []byte) error {
			if len(k) != 72 {
				return model.Bad("invalid stored point key")
			}
			var point model.Sample
			if err := json.Unmarshal(v, &point); err != nil {
				return err
			}
			grouped[string(k[:64])] = append(grouped[string(k[:64])], point)
			return nil
		})
		if err != nil {
			return err
		}
		seq, err := volume.Next(tx)
		if err != nil {
			return err
		}
		for id, points := range grouped {
			if err := putBlock(b, id, seq, points); err != nil {
				return err
			}
			receipt.Blocks++
			receipt.Points += len(points)
		}
		if err := clearBucket(b, volume.Head); err != nil {
			return err
		}
		n.LastSequence = seq
		receipt.Sequence, receipt.Floor = seq, n.Floor
		return volume.PutJSON(b, volume.Config, n)
	})
	return receipt, err
}

func (e *Engine) Blocks(name string) ([]model.Block, error) {
	var blocks []model.Block
	err := e.DB.Raw.View(func(tx *bolt.Tx) error {
		b, _, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		blocks, err = readBlocks(b)
		return err
	})
	return blocks, err
}
