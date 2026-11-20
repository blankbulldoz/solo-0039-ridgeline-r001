package chunk

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"

	"ridgeline/internal/model"
)

const Format = 1

func Encode(points []model.Sample) ([]byte, string, error) {
	raw, err := json.Marshal(points)
	if err != nil {
		return nil, "", err
	}
	var out bytes.Buffer
	writer := gzip.NewWriter(&out)
	if _, err := writer.Write(raw); err != nil {
		return nil, "", err
	}
	if err := writer.Close(); err != nil {
		return nil, "", err
	}
	digest := sha256.Sum256(out.Bytes())
	return out.Bytes(), hex.EncodeToString(digest[:]), nil
}

func Decode(payload []byte, block model.Block) ([]model.Sample, error) {
	if block.Format != Format {
		return nil, fmt.Errorf("unsupported block format %d", block.Format)
	}
	digest := sha256.Sum256(payload)
	if hex.EncodeToString(digest[:]) != block.Checksum {
		return nil, fmt.Errorf("block %s checksum mismatch", block.ID)
	}
	reader, err := gzip.NewReader(bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	raw, err := io.ReadAll(io.LimitReader(reader, 64<<20))
	if err != nil {
		return nil, err
	}
	var points []model.Sample
	if err := json.Unmarshal(raw, &points); err != nil {
		return nil, err
	}
	if len(points) != block.Count {
		return nil, fmt.Errorf("block %s count mismatch", block.ID)
	}
	for i, p := range points {
		if !model.ValidTime(p.Time) || p.Time < block.Start || p.Time >= block.End ||
			(i > 0 && points[i-1].Time >= p.Time) {
			return nil, fmt.Errorf("block %s invalid point order or bounds", block.ID)
		}
	}
	return points, nil
}
