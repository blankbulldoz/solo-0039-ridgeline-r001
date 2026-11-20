package model

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math"
	"regexp"
)

const MinTime int64 = -(1 << 52)
const MaxTime int64 = 1 << 52

var identifier = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_.:-]{0,95}$`)

func ValidName(s string) bool {
	return identifier.MatchString(s)
}

func ValidTime(t int64) bool {
	return t >= MinTime && t <= MaxTime
}

func ValidateLabels(labels Labels) error {
	if len(labels) == 0 || len(labels) > 32 {
		return Bad("labels must contain 1..32 entries")
	}
	for k, v := range labels {
		if !ValidName(k) || len(v) > 512 {
			return Bad("invalid label name or value")
		}
	}
	if labels["__name__"] == "" {
		return Bad("__name__ label is required")
	}
	return nil
}

func SeriesID(labels Labels) string {
	b, _ := json.Marshal(labels)
	hash := sha256.Sum256(b)
	return hex.EncodeToString(hash[:])
}

func ValidateObservation(p Observation) error {
	if err := ValidateLabels(p.Labels); err != nil {
		return err
	}
	if !ValidTime(p.Time) || math.IsNaN(p.Value) || math.IsInf(p.Value, 0) {
		return Bad("time outside supported range or value is not finite")
	}
	return nil
}

func ValidateNamespace(n Namespace) error {
	if !ValidName(n.Name) {
		return Bad("invalid namespace name")
	}
	if n.RetentionMS <= 0 || n.RetentionMS > MaxTime || n.LatenessMS < 0 ||
		n.LatenessMS > MaxTime || n.FutureMS < 0 || n.FutureMS > MaxTime {
		return Bad("invalid time policy")
	}
	if n.MaxSeries < 1 || n.MaxSeries > 100000 || n.MaxWrite < 1 ||
		n.MaxWrite > 50000 || n.MaxScan < n.MaxWrite || n.MaxScan > 2000000 {
		return Bad("invalid capacity policy")
	}
	return nil
}

func ValidateQuery(q Query) error {
	if !ValidTime(q.Start) || q.End < MinTime || q.End > MaxTime+1 || q.Start >= q.End {
		return Bad("query requires a supported nonempty half-open interval")
	}
	if q.Limit < 0 || q.Limit > 100000 || len(q.Matchers) > 32 {
		return Bad("invalid query limit or matcher count")
	}
	for _, label := range q.GroupBy {
		if !ValidName(label) {
			return Bad("invalid grouping label")
		}
	}
	return nil
}
