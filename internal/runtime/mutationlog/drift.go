package mutationlog

import (
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
)

type DriftRow struct {
	Kind          string  `json:"kind"`
	EntityID      string  `json:"entity_id"`
	Domain        Domain  `json:"domain,omitempty"`
	Path          *string `json:"path,omitempty"`
	FoldedPresent bool    `json:"folded_present"`
	StoredPresent bool    `json:"stored_present"`
	FoldedValue   any     `json:"folded_value"`
	StoredValue   any     `json:"stored_value"`
	FoldedType    string  `json:"folded_type"`
	StoredType    string  `json:"stored_type"`
}

type DriftReport struct {
	RunID           string     `json:"run_id"`
	EntitiesChecked int        `json:"entities_checked"`
	Rows            []DriftRow `json:"rows"`
}

// Nested atomic values need the same numeric evidence as scalar mismatches.
func (row DriftRow) MarshalJSON() ([]byte, error) {
	type wire DriftRow
	return canonicaljson.MarshalPreservingNumberKinds(wire(row))
}

// CompareEntityStateProjections is deliberately not the writer's diff: presence
// and runtime numeric kinds are evidence even when JSON encodes them alike.
func CompareEntityStateProjections(runID string, folded, stored map[string]EntityStateProjection) (DriftReport, error) {
	report := DriftReport{RunID: runID, Rows: []DriftRow{}}
	for _, entity := range projectionKeys(folded, stored) {
		before, beforeOK := folded[entity]
		after, afterOK := stored[entity]
		report.EntitiesChecked++
		if beforeOK != afterOK {
			report.Rows = append(report.Rows, DriftRow{Kind: "entity_presence", EntityID: entity,
				FoldedPresent: beforeOK, StoredPresent: afterOK, FoldedType: "entity", StoredType: "entity"})
			continue
		}
		add := func(domain Domain, path string, left any, leftOK bool, right any, rightOK bool) {
			if leftOK == rightOK && reflect.DeepEqual(left, right) {
				return
			}
			report.Rows = append(report.Rows, DriftRow{Kind: "value", EntityID: entity, Domain: domain, Path: &path,
				FoldedPresent: leftOK, StoredPresent: rightOK, FoldedValue: left, StoredValue: right,
				FoldedType: projectionValueType(left, leftOK), StoredType: projectionValueType(right, rightOK)})
		}
		add(DomainLifecycleState, "", before.CurrentState, true, after.CurrentState, true)
		for _, bucket := range []struct {
			domain      Domain
			left, right map[string]any
		}{
			{DomainAuthoredField, before.Fields, after.Fields},
			{DomainBookkeeping, before.Bookkeeping, after.Bookkeeping},
			{DomainGate, before.Gates, after.Gates},
			{DomainAccumulator, before.Accumulator, after.Accumulator},
		} {
			left, err := cloneProjectionBucket(bucket.left)
			if err != nil {
				return DriftReport{}, fmt.Errorf("entity %s folded %s: %w", entity, bucket.domain, err)
			}
			right, err := cloneProjectionBucket(bucket.right)
			if err != nil {
				return DriftReport{}, fmt.Errorf("entity %s stored %s: %w", entity, bucket.domain, err)
			}
			compareProjectionBucket(bucket.domain, "", left, right, add)
		}
	}
	return report, nil
}

func cloneProjectionBucket(value map[string]any) (map[string]any, error) {
	if value == nil {
		return map[string]any{}, nil
	}
	cloned, err := canonicaljson.CloneRuntimeValue(value)
	if err != nil {
		return nil, err
	}
	return cloned.(map[string]any), nil
}

func compareProjectionBucket(domain Domain, prefix string, left, right map[string]any, add func(Domain, string, any, bool, any, bool)) {
	for _, key := range projectionKeys(left, right) {
		path := key
		if prefix != "" {
			path = prefix + "." + key
		}
		a, aOK := left[key]
		b, bOK := right[key]
		am, aMap := a.(map[string]any)
		bm, bMap := b.(map[string]any)
		if domain == DomainAuthoredField && aOK && bOK && aMap && bMap {
			compareProjectionBucket(domain, path, am, bm, add)
		} else {
			add(domain, path, a, aOK, b, bOK)
		}
	}
}

func projectionKeys[V any](left, right map[string]V) []string {
	seen := make(map[string]struct{}, len(left)+len(right))
	for key := range left {
		seen[key] = struct{}{}
	}
	for key := range right {
		seen[key] = struct{}{}
	}
	keys := make([]string, 0, len(seen))
	for key := range seen {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func projectionValueType(value any, present bool) string {
	if !present {
		return "absent"
	}
	switch value.(type) {
	case nil:
		return "null"
	case int64:
		return "integer"
	case float64:
		return "double"
	case bool:
		return "boolean"
	case string:
		return "text"
	case []any:
		return "list"
	case map[string]any:
		return "object"
	default:
		return strings.TrimPrefix(fmt.Sprintf("%T", value), "mutationlog.")
	}
}
