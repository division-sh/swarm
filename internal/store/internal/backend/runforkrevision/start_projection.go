package runforkrevision

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"sort"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/google/uuid"
)

// StartProjection identifies the initial semantic facts within the creation
// commit. It contains coordinates, not another copy of those facts or payloads.
type StartProjection struct {
	Version          int         `json:"version"`
	SourceBundleHash string      `json:"source_bundle_hash"`
	FirstTurnEventID string      `json:"first_turn_event_id,omitempty"`
	Facts            []StartFact `json:"facts"`
}

type StartFact struct {
	Family Family `json:"family"`
	Key    string `json:"key"`
}

func (p StartProjection) Validate() error {
	if p.Version != 1 {
		return fmt.Errorf("run start projection requires version 1")
	}
	if _, err := correlation.NewSourceArtifactFact(p.SourceBundleHash); err != nil {
		return fmt.Errorf("run start projection source: %w", err)
	}
	if p.FirstTurnEventID != "" {
		id, err := uuid.Parse(p.FirstTurnEventID)
		if err != nil || id == uuid.Nil || id.String() != p.FirstTurnEventID {
			return fmt.Errorf("run start first turn requires a canonical nonzero event UUID")
		}
	}
	if p.Facts == nil {
		return fmt.Errorf("run start projection requires an explicit fact set")
	}
	for index, fact := range p.Facts {
		if !startFactFamily(fact.Family) || fact.Key == "" {
			return fmt.Errorf("run start projection contains an invalid or excluded fact")
		}
		if _, err := NewFactRef(fact.Family, fact.Key); err != nil {
			return fmt.Errorf("run start fact coordinates: %w", err)
		}
		if fact.Family == FamilyEvents && fact.Key == p.FirstTurnEventID {
			return fmt.Errorf("run start projection cannot admit its creating ingress")
		}
		if index > 0 && !startFactLess(p.Facts[index-1], fact) {
			return fmt.Errorf("run start projection facts must be unique and canonically ordered")
		}
	}
	return nil
}

func startFactFamily(family Family) bool {
	switch family {
	case FamilyEvents, FamilyEntityMutations, FamilyEntityMetadata,
		FamilyEventDeliveries, FamilyEventReceipts, FamilyDeadLetters,
		FamilyTimers, FamilyReplyContexts, FamilyFanOutObligations:
		return true
	default:
		return false
	}
}

func startFactLess(left, right StartFact) bool {
	return left.Family < right.Family || left.Family == right.Family && left.Key < right.Key
}

func DecodeStartProjection(raw []byte) (StartProjection, error) {
	var projection StartProjection
	if _, err := canonicaljson.Decode(raw); err != nil {
		return projection, fmt.Errorf("run start projection: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&projection); err != nil {
		return projection, fmt.Errorf("decode run start projection: %w", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return projection, fmt.Errorf("run start projection requires one object")
	}
	return projection, projection.Validate()
}

// DeclareStart is called only after canonical creation actually inserted a run.
// Initial domain writers contribute their owned coordinates before finalization.
func (e *Effects) DeclareStart(runID, bundleHash, firstTurnEventID string) error {
	projection := StartProjection{Version: 1, SourceBundleHash: bundleHash, FirstTurnEventID: firstTurnEventID, Facts: []StartFact{}}
	if err := projection.Validate(); err != nil {
		return err
	}
	if e == nil {
		return fmt.Errorf("run start requires revision effects")
	}
	if id, err := uuid.Parse(runID); err != nil || id == uuid.Nil {
		return fmt.Errorf("run start requires a nonzero run UUID")
	}
	if e.starts == nil {
		e.starts = make(map[string]StartProjection)
	}
	if old, exists := e.starts[runID]; exists {
		if old.SourceBundleHash != bundleHash || old.FirstTurnEventID != firstTurnEventID {
			return fmt.Errorf("run start declaration contradicts its creation identity")
		}
		return nil
	}
	e.starts[runID] = projection
	if e.byRun == nil {
		e.byRun = make(map[string]map[Family]*familySelection)
	}
	if e.byRun[runID] == nil {
		e.byRun[runID] = make(map[Family]*familySelection)
	}
	return nil
}

func (e *Effects) DeclaredStart(runID string) (StartProjection, bool) {
	if e == nil {
		return StartProjection{}, false
	}
	projection, exists := e.starts[runID]
	projection.Facts = append([]StartFact{}, projection.Facts...)
	return projection, exists
}

func (e *Effects) RequireOutsideStart(runID string, refs ...FactRef) error {
	if e == nil {
		return fmt.Errorf("run start requires revision effects")
	}
	projection, declared := e.starts[runID]
	if !declared {
		return nil
	}
	for _, ref := range refs {
		candidate := StartFact{Family: ref.family, Key: ref.key}
		index := sort.Search(len(projection.Facts), func(i int) bool { return !startFactLess(projection.Facts[i], candidate) })
		if index < len(projection.Facts) && projection.Facts[index] == candidate {
			return fmt.Errorf("post-construction write would replace an exclusive start fact in its creation revision")
		}
	}
	return nil
}

func (e *Effects) RequireWholeFamilyOutsideStart(runID string, family Family) error {
	if e == nil {
		return fmt.Errorf("run start requires revision effects")
	}
	for _, fact := range e.starts[runID].Facts {
		if fact.Family == family {
			return fmt.Errorf("whole-family contribution would replace initial facts in the same creation revision")
		}
	}
	return nil
}

func (e *Effects) AddStartFacts(runID string, refs ...FactRef) error {
	if e == nil {
		return fmt.Errorf("run start requires revision effects")
	}
	projection, declared := e.starts[runID]
	if !declared {
		return fmt.Errorf("initial facts require canonical run creation in this attempt")
	}
	facts := append([]StartFact{}, projection.Facts...)
	for _, ref := range refs {
		if err := ref.validate(runID); err != nil {
			return err
		}
		if !startFactFamily(ref.family) || ref.family == FamilyEvents && ref.key == projection.FirstTurnEventID {
			return fmt.Errorf("run start cannot contain its creating ingress or excluded operational facts")
		}
		facts = append(facts, StartFact{Family: ref.family, Key: ref.key})
	}
	sort.Slice(facts, func(i, j int) bool { return startFactLess(facts[i], facts[j]) })
	unique := facts[:0]
	for _, fact := range facts {
		if len(unique) == 0 || unique[len(unique)-1] != fact {
			unique = append(unique, fact)
		}
	}
	projection.Facts = unique
	if err := projection.Validate(); err != nil {
		return err
	}
	if len(refs) > 0 {
		if err := e.AddFacts(runID, refs...); err != nil {
			return err
		}
	}
	e.starts[runID] = projection
	return nil
}

func cloneStarts(starts map[string]StartProjection) map[string]StartProjection {
	cloned := make(map[string]StartProjection, len(starts))
	for runID, projection := range starts {
		projection.Facts = append([]StartFact{}, projection.Facts...)
		cloned[runID] = projection
	}
	return cloned
}

func publishStart(ctx context.Context, tx revisionSQL, postgres bool, runID string, revision int64, projection StartProjection) error {
	if err := projection.Validate(); err != nil {
		return err
	}
	raw, err := json.Marshal(projection)
	if err != nil {
		return err
	}
	query := `UPDATE run_fork_revision_heads SET start_revision=$2, start_projection=$3
		WHERE run_id=$1 AND last_revision=$2 AND start_revision IS NULL`
	var value any = string(raw)
	if postgres {
		query = `UPDATE run_fork_revision_heads SET start_revision=$2, start_projection=$3::jsonb
			WHERE run_id=$1 AND last_revision=$2 AND start_revision IS NULL`
		value = raw
	}
	result, err := tx.ExecContext(ctx, query, runID, revision, value)
	if err != nil {
		return fmt.Errorf("publish immutable run start: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return fmt.Errorf("run start requires exactly one new creation projection")
	}
	return nil
}

func LoadStartProjection(ctx context.Context, tx *sql.Tx, runID string) (int64, StartProjection, error) {
	if tx == nil {
		return 0, StartProjection{}, fmt.Errorf("run start lookup requires a selected snapshot")
	}
	var revision int64
	var raw []byte
	var origin, firstTurn string
	if err := tx.QueryRowContext(ctx, `SELECT h.start_revision, h.start_projection,
		r.origin_kind, COALESCE(CAST(r.trigger_event_id AS TEXT),'')
		FROM run_fork_revision_heads h JOIN runs r ON r.run_id=h.run_id
		WHERE h.run_id=$1 AND h.start_revision IS NOT NULL`, runID).Scan(&revision, &raw, &origin, &firstTurn); err != nil {
		return 0, StartProjection{}, fmt.Errorf("load committed run start: %w", err)
	}
	projection, err := DecodeStartProjection(raw)
	if revision <= 0 {
		return 0, StartProjection{}, fmt.Errorf("run start requires a positive committed revision")
	}
	if err != nil {
		return 0, StartProjection{}, err
	}
	if projection.FirstTurnEventID != firstTurn || (origin == "event") != (firstTurn != "") {
		return 0, StartProjection{}, fmt.Errorf("run start first-turn reference differs from immutable creation origin")
	}
	return revision, projection, nil
}
