package runforkrevision

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
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
	latestFacts(context.Context, string, []Family) (ledgerFactsByFamily, error)
	allocate(context.Context, string) (int64, error)
	insertFacts(context.Context, string, int64, []revisionFactInsert) error
}

func finalize(ctx context.Context, adapter ledgerAdapter, effects *Effects) (map[string]Result, error) {
	phase := transactiontest.BeginRevision(ctx)
	defer phase.End()
	changes := effects.normalized()
	results := make(map[string]Result, len(changes))
	if len(changes) == 0 {
		return results, nil
	}
	runIDs := make([]string, len(changes))
	for i := range changes {
		runIDs[i] = changes[i].runID
	}
	if err := func() error {
		lockPhase := transactiontest.BeginRevisionLock(ctx)
		defer lockPhase.End()
		if err := adapter.lockParents(ctx, runIDs); err != nil {
			return err
		}
		return adapter.lockRevisionState(ctx, runIDs)
	}(); err != nil {
		return nil, err
	}
	for _, change := range changes {
		latestByFamily, err := readSelectedLatestFacts(ctx, adapter.projectionQueryer(), change)
		if err != nil {
			return nil, fmt.Errorf("load latest revision facts: %w", err)
		}
		type familyChange struct {
			family         Family
			current        []canonicalFact
			latest         map[string]ledgerFact
			comparedPrefix int
		}
		changed := make([]familyChange, 0, len(change.families))
		for _, family := range change.families {
			current, err := loadSelectedCanonicalProjection(ctx, adapter.projectionQueryer(), change.runID, family, change.exact[family])
			if err != nil {
				return nil, err
			}
			latest := latestByFamily[family]
			if refs, exact := change.exact[family]; exact {
				if err := validateExactCapture(change.runID, family, refs, current, latest); err != nil {
					return nil, err
				}
				if err := validateExactAbsence(ctx, adapter.projectionQueryer(), change, family, refs, current, changes); err != nil {
					return nil, err
				}
			}
			equal, comparedPrefix := compareFamilyCanonicalProjection(family, current, latest)
			if equal {
				continue
			}
			changed = append(changed, familyChange{family: family, current: current, latest: latest, comparedPrefix: comparedPrefix})
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
		pending := make([]revisionFactInsert, 0, revisionFactInsertBatch)
		flush := func() error {
			if err := adapter.insertFacts(ctx, change.runID, revision, pending); err != nil {
				return err
			}
			pending = pending[:0]
			return nil
		}
		appendFact := func(family Family, key string, fact []byte, present bool) error {
			pending = append(pending, revisionFactInsert{family: family, key: key, fact: fact, present: present})
			if len(pending) == revisionFactInsertBatch {
				return flush()
			}
			return nil
		}
		for _, family := range changed {
			currentKeys := make(map[string]struct{}, len(family.current))
			for i, fact := range family.current {
				currentKeys[fact.key] = struct{}{}
				// This exact slice prefix was already compared against the same
				// transaction-local ledger. Do not decode it a second time.
				if i < family.comparedPrefix {
					continue
				}
				stored, exists := family.latest[fact.key]
				if exists && stored.present && revisionFamilyJSONEqual(family.family, fact.fact, stored.fact) {
					continue
				}
				if err := appendFact(family.family, fact.key, fact.fact, true); err != nil {
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
				if err := appendFact(family.family, key, []byte(`{}`), false); err != nil {
					return nil, err
				}
			}
		}
		if err := flush(); err != nil {
			return nil, err
		}
		results[change.runID] = Result{Revision: revision, Changed: true}
	}
	return results, nil
}

func validateExactCapture(runID string, family Family, refs []FactRef, current []canonicalFact, latest map[string]ledgerFact) error {
	wanted := make(map[string]bool, len(refs))
	for _, ref := range refs {
		if err := ref.validate(runID); err != nil {
			return err
		}
		if ref.family != family || wanted[ref.key] {
			return fmt.Errorf("exact revision capture has duplicate or foreign coordinates")
		}
		wanted[ref.key] = true
	}
	present := make(map[string]bool, len(current))
	for _, fact := range current {
		if !wanted[fact.key] || present[fact.key] {
			return fmt.Errorf("exact revision projection returned duplicate or unrequested fact %s", fact.key)
		}
		present[fact.key] = true
	}
	for key, stored := range latest {
		if !wanted[key] {
			return fmt.Errorf("exact revision ledger returned unrequested fact %s", key)
		}
		if !stored.present {
			continue
		}
		actual, err := FactKey(family, stored.fact)
		if err != nil || actual != key {
			return fmt.Errorf("exact %s ledger fact %s has contradictory key: %v", family, key, err)
		}
		if family == FamilyEventDeliveries {
			if _, err := deliverylifecycle.DecodeHistoricalSnapshot(stored.fact); err != nil {
				return fmt.Errorf("invalid affected historical delivery: %w", err)
			}
		}
		switch family {
		case FamilyEvents, FamilyEventDeliveries, FamilyCommittedReplayScopes, FamilyTimers:
			var body struct {
				RunID string `json:"run_id"`
			}
			if err := json.Unmarshal(stored.fact, &body); err != nil || body.RunID != runID {
				return fmt.Errorf("exact %s ledger fact %s has foreign run ownership", family, key)
			}
		}
	}
	for key := range wanted {
		if !present[key] {
			if _, known := latest[key]; !known {
				return fmt.Errorf("exact %s fact %s is absent from run %s and has no prior fact; not a deletion", family, key, runID)
			}
		}
	}
	return nil
}

func canonicalProjectionEqual(current []canonicalFact, latest map[string]ledgerFact) bool {
	equal, _ := compareCanonicalProjection(current, latest)
	return equal
}

func compareCanonicalProjection(current []canonicalFact, latest map[string]ledgerFact) (bool, int) {
	return compareFamilyCanonicalProjection("", current, latest)
}

func compareFamilyCanonicalProjection(family Family, current []canonicalFact, latest map[string]ledgerFact) (bool, int) {
	if len(current) != countPresent(latest) {
		return false, 0
	}
	for i, fact := range current {
		stored, ok := latest[fact.key]
		if !ok || !stored.present || !revisionFamilyJSONEqual(family, fact.fact, stored.fact) {
			return false, i
		}
	}
	return true, len(current)
}

func revisionFamilyJSONEqual(family Family, left, right []byte) bool {
	if family != FamilyEntityMetadata {
		return canonicalJSONEqual(left, right)
	}
	// Receiver config is runtime data: 7 and 7.0 cannot share a revision
	// merely because the other families' semantic JSON comparison merges them.
	configBytes := func(raw []byte) ([]byte, error) {
		var fact map[string]any
		if err := canonicaljson.DecodePreservingNumberLexemes(raw, &fact); err != nil {
			return nil, err
		}
		return canonicaljson.MarshalPreservingNumberKinds(fact)
	}
	a, err := configBytes(left)
	b, otherErr := configBytes(right)
	return err == nil && otherErr == nil && bytes.Equal(a, b)
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
	if json.Unmarshal(left, &leftValue) != nil {
		return false
	}
	// Identical bytes still require the existing JSON admission (in particular,
	// float overflow must not become equal), but not a second decode/two encodes.
	if bytes.Equal(left, right) {
		return true
	}
	if json.Unmarshal(right, &rightValue) != nil {
		return false
	}
	return equalDecodedRevisionJSON(leftValue, rightValue)
}

// Both inputs have passed the existing JSON decoder. Compare that decoder's
// closed value set without allocating two complete canonical encodings. Signed
// zero stays distinct because the former Marshal comparison distinguished it.
func equalDecodedRevisionJSON(left, right any) bool {
	switch a := left.(type) {
	case nil:
		return right == nil
	case bool:
		b, ok := right.(bool)
		return ok && a == b
	case string:
		b, ok := right.(string)
		return ok && a == b
	case float64:
		b, ok := right.(float64)
		return ok && math.Float64bits(a) == math.Float64bits(b)
	case []any:
		b, ok := right.([]any)
		if !ok || len(a) != len(b) {
			return false
		}
		for i := range a {
			if !equalDecodedRevisionJSON(a[i], b[i]) {
				return false
			}
		}
		return true
	case map[string]any:
		b, ok := right.(map[string]any)
		if !ok || len(a) != len(b) {
			return false
		}
		for key, value := range a {
			other, present := b[key]
			if !present || !equalDecodedRevisionJSON(value, other) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func validateComplete(ctx context.Context, adapter ledgerAdapter, runID string) error {
	effects := NewEffects()
	if err := effects.Add(runID, allFamilies...); err != nil {
		return err
	}
	latestByFamily, err := adapter.latestFacts(ctx, runID, AllFamilies())
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
