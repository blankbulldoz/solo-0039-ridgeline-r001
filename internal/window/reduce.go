package window

import (
	"fmt"
	"math"

	"ridgeline/internal/model"
)

type accumulator struct {
	cell     model.Cell
	sum      float64
	firstT   int64
	lastT    int64
	firstKey string
	lastKey  string
	first    float64
	last     float64
	min      float64
	max      float64
}

func Aggregate(series []model.SeriesPoints, q model.Query) ([]model.Cell, error) {
	if err := Validate(q); err != nil {
		return nil, err
	}
	if q.Function == "rate" {
		return Rates(series, q)
	}
	groups := map[string]*accumulator{}
	for _, s := range series {
		key, labels := Group(s.Series.Labels, q.GroupBy)
		for _, p := range s.Points {
			start := Bucket(p.Time, q.Origin, q.Step)
			k := fmt.Sprintf("%s/%d", key, start)
			a := groups[k]
			if a == nil {
				a = &accumulator{
					cell:   model.Cell{Labels: labels, Start: start, End: start + q.Step},
					firstT: p.Time, lastT: p.Time, first: p.Value, last: p.Value,
					firstKey: s.Series.ID, lastKey: s.Series.ID,
					min: p.Value, max: p.Value,
				}
				groups[k] = a
			}
			a.cell.Count++
			a.sum += p.Value
			a.min = math.Min(a.min, p.Value)
			a.max = math.Max(a.max, p.Value)
			if p.Time < a.firstT || (p.Time == a.firstT && s.Series.ID < a.firstKey) {
				a.firstT, a.firstKey, a.first = p.Time, s.Series.ID, p.Value
			}
			if p.Time > a.lastT || (p.Time == a.lastT && s.Series.ID > a.lastKey) {
				a.lastT, a.lastKey, a.last = p.Time, s.Series.ID, p.Value
			}
		}
	}
	out := make([]model.Cell, 0, len(groups))
	for _, a := range groups {
		switch q.Function {
		case "count":
			a.cell.Value = float64(a.cell.Count)
		case "sum":
			a.cell.Value = a.sum
		case "avg":
			a.cell.Value = a.sum / float64(a.cell.Count)
		case "min":
			a.cell.Value = a.min
		case "max":
			a.cell.Value = a.max
		case "first":
			a.cell.Value = a.first
		case "last":
			a.cell.Value = a.last
		}
		if math.IsInf(a.cell.Value, 0) || math.IsNaN(a.cell.Value) {
			return nil, model.Capacity("aggregation overflow")
		}
		out = append(out, a.cell)
	}
	SortCells(out)
	return out, nil
}
