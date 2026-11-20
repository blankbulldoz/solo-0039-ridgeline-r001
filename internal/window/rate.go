package window

import (
	"fmt"
	"math"

	"ridgeline/internal/model"
)

func Rates(series []model.SeriesPoints, q model.Query) ([]model.Cell, error) {
	cells := map[string]*model.Cell{}
	for _, s := range series {
		key, labels := Group(s.Series.Labels, q.GroupBy)
		windows := map[int64][]model.Sample{}
		for _, point := range s.Points {
			if point.Value < 0 {
				return nil, model.Bad("counter rate requires nonnegative values")
			}
			start := Bucket(point.Time, q.Origin, q.Step)
			windows[start] = append(windows[start], point)
		}
		for start, points := range windows {
			if len(points) < 2 {
				continue
			}
			increase := 0.0
			for i := 1; i < len(points); i++ {
				delta := points[i].Value - points[i-1].Value
				if delta < 0 {
					delta = points[i].Value
				}
				increase += delta
			}
			elapsed := float64(points[len(points)-1].Time-points[0].Time) / 1000
			value := increase / elapsed
			k := fmt.Sprintf("%s/%d", key, start)
			cell := cells[k]
			if cell == nil {
				cell = &model.Cell{Labels: labels, Start: start, End: start + q.Step}
				cells[k] = cell
			}
			cell.Value += value
			cell.Count += len(points)
			if math.IsInf(cell.Value, 0) || math.IsNaN(cell.Value) {
				return nil, model.Capacity("counter calculation overflow")
			}
		}
	}
	out := make([]model.Cell, 0, len(cells))
	for _, cell := range cells {
		out = append(out, *cell)
	}
	SortCells(out)
	return out, nil
}
