package selectseries

import (
	"regexp"
	"sort"

	"ridgeline/internal/model"
)

type predicate struct {
	matcher model.Matcher
	regex   *regexp.Regexp
}

type Selector struct {
	terms []predicate
}

func Compile(matchers []model.Matcher) (Selector, error) {
	var s Selector
	for _, m := range matchers {
		if !model.ValidName(m.Label) {
			return s, model.Bad("invalid matcher label")
		}
		p := predicate{matcher: m}
		switch m.Op {
		case "=", "!=":
		case "=~", "!~":
			if len(m.Value) > 4096 {
				return s, model.Bad("matcher expression is too long")
			}
			re, err := regexp.Compile("^(?:" + m.Value + ")$")
			if err != nil {
				return s, model.Bad("invalid matcher expression")
			}
			p.regex = re
		default:
			return s, model.Bad("unknown matcher operation")
		}
		s.terms = append(s.terms, p)
	}
	return s, nil
}

func (s Selector) Matches(labels model.Labels) bool {
	for _, p := range s.terms {
		value := labels[p.matcher.Label]
		matched := value == p.matcher.Value
		if p.regex != nil {
			matched = p.regex.MatchString(value)
		}
		if p.matcher.Op == "!=" || p.matcher.Op == "!~" {
			matched = !matched
		}
		if !matched {
			return false
		}
	}
	return true
}

func Pick(series []model.Series, selector Selector) []model.Series {
	out := make([]model.Series, 0)
	for _, s := range series {
		if selector.Matches(s.Labels) {
			out = append(out, s)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}
