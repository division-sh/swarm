package runforkrevision

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
)

type ledgerFact struct {
	fact    []byte
	present bool
}

type ledgerFactsByFamily map[Family]map[string]ledgerFact

type ledgerAdapter interface {
	projectionQueryer() queryer
	lockParents(context.Context, []string) error
	lockRevisionState(context.Context, []string) error
	latestRevision(context.Context, string) (int64, bool, error)
	latestFacts(context.Context, string) (ledgerFactsByFamily, error)
	allocate(context.Context, string) (int64, error)
	insertFact(context.Context, string, int64, Family, string, []byte, bool) error
}

func finalize(ctx context.Context, adapter ledgerAdapter, effects *Effects) (map[string]Result, error) {
	changes := effects.normalized()
	results := make(map[string]Result, len(changes))
	if len(changes) == 0 {
		return results, nil
	}
	runIDs := make([]string, len(changes))
	for i := range changes {
		runIDs[i] = changes[i].runID
	}
	if err := adapter.lockParents(ctx, runIDs); err != nil {
		return nil, err
	}
	if err := adapter.lockRevisionState(ctx, runIDs); err != nil {
		return nil, err
	}
	for _, change := range changes {
		latestByFamily, err := adapter.latestFacts(ctx, change.runID)
		if err != nil {
			return nil, fmt.Errorf("load latest revision facts: %w", err)
		}
		type familyChange struct {
			family  Family
			current []canonicalFact
			latest  map[string]ledgerFact
		}
		changed := make([]familyChange, 0, len(change.families))
		for _, family := range change.families {
			current, err := loadCanonicalProjection(ctx, adapter.projectionQueryer(), change.runID, family)
			if err != nil {
				return nil, err
			}
			latest := latestByFamily[family]
			if canonicalProjectionEqual(current, latest) {
				continue
			}
			changed = append(changed, familyChange{family: family, current: current, latest: latest})
		}
		if len(changed) == 0 {
			revision, ok, err := adapter.latestRevision(ctx, change.runID)
			if err != nil {
				return nil, err
			}
			if ok {
				results[change.runID] = Result{Revision: revision}
			}
			continue
		}
		revision, err := adapter.allocate(ctx, change.runID)
		if err != nil {
			return nil, err
		}
		for _, family := range changed {
			currentKeys := make(map[string]struct{}, len(family.current))
			for _, fact := range family.current {
				currentKeys[fact.key] = struct{}{}
				if err := adapter.insertFact(ctx, change.runID, revision, family.family, fact.key, fact.fact, true); err != nil {
					return nil, err
				}
			}
			for key, fact := range family.latest {
				if !fact.present {
					continue
				}
				if _, ok := currentKeys[key]; ok {
					continue
				}
				if err := adapter.insertFact(ctx, change.runID, revision, family.family, key, []byte(`{}`), false); err != nil {
					return nil, err
				}
			}
		}
		results[change.runID] = Result{Revision: revision, Changed: true}
	}
	return results, nil
}

func canonicalProjectionEqual(current []canonicalFact, latest map[string]ledgerFact) bool {
	if len(current) != countPresent(latest) {
		return false
	}
	for _, fact := range current {
		stored, ok := latest[fact.key]
		if !ok || !stored.present || !canonicalJSONEqual(fact.fact, stored.fact) {
			return false
		}
	}
	return true
}

func countPresent(facts map[string]ledgerFact) int {
	count := 0
	for _, fact := range facts {
		if fact.present {
			count++
		}
	}
	return count
}

func canonicalJSONEqual(left, right []byte) bool {
	var leftValue, rightValue any
	if json.Unmarshal(left, &leftValue) != nil || json.Unmarshal(right, &rightValue) != nil {
		return false
	}
	leftCanonical, leftErr := json.Marshal(leftValue)
	rightCanonical, rightErr := json.Marshal(rightValue)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftCanonical, rightCanonical)
}

func validateComplete(ctx context.Context, adapter ledgerAdapter, runID string) error {
	effects := NewEffects()
	if err := effects.Add(runID, allFamilies...); err != nil {
		return err
	}
	latestByFamily, err := adapter.latestFacts(ctx, runID)
	if err != nil {
		return fmt.Errorf("validate run fork revision facts: %w", err)
	}
	for _, change := range effects.normalized() {
		for _, family := range change.families {
			current, err := loadCanonicalProjection(ctx, adapter.projectionQueryer(), runID, family)
			if err != nil {
				return fmt.Errorf("validate run fork %s projection: %w", family, err)
			}
			latest := latestByFamily[family]
			if !canonicalProjectionEqual(current, latest) {
				return fmt.Errorf("run %s has unsupported unrevisioned %s facts; recreate the store and retry", runID, family)
			}
		}
	}
	return nil
}
