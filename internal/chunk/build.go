package chunk

import (
	"fmt"
	"sort"

	"ridgeline/internal/model"
)

func Build(series string, seq uint64, points []model.Sample) (model.Block, []byte, error) {
	if len(points) == 0 {
		return model.Block{}, nil, fmt.Errorf("cannot build empty block")
	}
	sort.Slice(points, func(i, j int) bool { return points[i].Time < points[j].Time })
	payload, checksum, err := Encode(points)
	if err != nil {
		return model.Block{}, nil, err
	}
	block := model.Block{
		ID:       fmt.Sprintf("%020d-%s", seq, series),
		SeriesID: series, Start: points[0].Time,
		End: points[len(points)-1].Time + 1, Count: len(points),
		Bytes: len(payload), Checksum: checksum, Format: Format,
	}
	return block, payload, nil
}
