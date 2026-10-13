package testplanning

import (
	"fmt"
	"slices"
)

func wholeCatalogUnit(id string) bool {
	switch id {
	case "catalog-runtime", "catalog-replay-1", "catalog-replay-2-3":
		return true
	default:
		return false
	}
}

// Only the three finite oracle roots and smoke have whole-catalog replacements.
// Other overlapping owners still fail the active-root census.
func (p Policy) composeCatalogCanaries(ids []string) ([]string, error) {
	runtime := slices.Contains(ids, "catalog-runtime")
	replay := slices.Contains(ids, "catalog-replay-1")
	composed := make([]string, 0, len(ids))
	for _, id := range ids {
		switch id {
		case "catalog-oracle-guards":
			switch {
			case runtime && replay:
				continue
			case runtime:
				id = "catalog-replay-oracle-guards"
			case replay:
				id = "catalog-runtime-oracle-guard"
			}
		case "catalog-required-smoke":
			if replay {
				continue
			}
		}
		if _, ok := p.Units[id]; !ok {
			return nil, fmt.Errorf("catalog composition requires declared unit %s", id)
		}
		composed = append(composed, id)
	}
	return composed, nil
}
