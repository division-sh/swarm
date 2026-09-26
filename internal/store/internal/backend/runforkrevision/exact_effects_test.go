package runforkrevision

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/google/uuid"
)

func TestExactRevisionReadBatchesPreserveSelection(t *testing.T) {
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "exact.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, ddl := range []string{
		`CREATE TABLE run_fork_fact_revisions (run_id TEXT, family TEXT, fact_key TEXT, revision INTEGER, fact TEXT, present BOOLEAN)`,
		`CREATE TABLE entity_state (run_id TEXT, entity_id TEXT, flow_instance TEXT, entity_type TEXT, slug TEXT, name TEXT, created_at TEXT)`,
		`CREATE TABLE flow_instances (run_id TEXT, instance_path TEXT, config TEXT)`,
	} {
		if _, err := db.Exec(ddl); err != nil {
			t.Fatal(err)
		}
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	runID, foreignRun := uuid.NewString(), uuid.NewString()
	effects := NewEffects()
	for i := 0; i < exactFactReadBatch+1; i++ {
		id := uuid.NewString()
		if err := effects.AddFact(runID, FamilyEntityMetadata, id); err != nil {
			t.Fatal(err)
		}
		for _, run := range []string{runID, foreignRun} {
			if _, err := tx.Exec(`INSERT INTO entity_state VALUES ($1,$2,'','task','slug',$3,'2026-09-19T00:00:00Z')`, run, id, run); err != nil {
				t.Fatal(err)
			}
			for revision := 1; revision <= 2; revision++ {
				body := fmt.Sprintf(`{"entity_id":%q,"revision":%d}`, id, revision)
				if _, err := tx.Exec(`INSERT INTO run_fork_fact_revisions VALUES ($1,$2,$3,$4,$5,$6)`, run, string(FamilyEntityMetadata), id, revision, body, i%3 != 0); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	if err := effects.Add(runID, FamilyEvents); err != nil {
		t.Fatal(err)
	}
	eventID := uuid.NewString()
	if _, err := tx.Exec(`INSERT INTO run_fork_fact_revisions VALUES ($1,'events',$2,1,'{}',true)`, runID, eventID); err != nil {
		t.Fatal(err)
	}
	// Untouched corruption is intentionally outside ordinary exact writes. The
	// all-family reader must still reject it. This also detects hidden whole-run
	// fallback for the mixed whole-events contribution after SQL batching.
	if _, err := tx.Exec(`INSERT INTO run_fork_fact_revisions VALUES ($1,'unknown','hostile',1,'{}',false)`, runID); err != nil {
		t.Fatal(err)
	}
	change := effects.normalized()[0]
	current, err := loadSelectedCanonicalProjection(context.Background(), tx, runID, FamilyEntityMetadata, change.exact[FamilyEntityMetadata])
	if err != nil || len(current) != exactFactReadBatch+1 {
		t.Fatalf("bounded current read: count=%d err=%v", len(current), err)
	}
	for _, fact := range current {
		if !strings.Contains(string(fact.fact), runID) || strings.Contains(string(fact.fact), foreignRun) {
			t.Fatalf("current projection selected foreign state: %s", fact.fact)
		}
	}
	latest, err := readSelectedLatestFacts(context.Background(), tx, change)
	if err != nil || len(latest[FamilyEntityMetadata]) != exactFactReadBatch+1 || len(latest[FamilyEvents]) != 1 || len(latest) != 2 {
		t.Fatalf("bounded mixed ledger read: families=%d entity_count=%d event_count=%d err=%v", len(latest), len(latest[FamilyEntityMetadata]), len(latest[FamilyEvents]), err)
	}
	for _, fact := range latest[FamilyEntityMetadata] {
		if !strings.Contains(string(fact.fact), `"revision":2`) {
			t.Fatal("bounded ledger read lost latest revision")
		}
	}
	if _, err := readLatestFacts(context.Background(), tx, runID, AllFamilies()); err == nil {
		t.Fatal("full read accepted untouched corruption")
	}
	// Duplicate rows cannot become an arbitrary winner in either projection.
	first := change.exact[FamilyEntityMetadata][0].key
	if _, err := tx.Exec(`INSERT INTO entity_state SELECT * FROM entity_state WHERE run_id=$1 AND entity_id=$2`, runID, first); err != nil {
		t.Fatal(err)
	}
	if _, err := loadSelectedCanonicalProjection(context.Background(), tx, runID, FamilyEntityMetadata, change.exact[FamilyEntityMetadata]); err == nil {
		t.Fatal("duplicate current fact was hidden by batched reads")
	}
	if _, err := tx.Exec(`INSERT INTO run_fork_fact_revisions SELECT * FROM run_fork_fact_revisions WHERE run_id=$1 AND family='entity_metadata' AND fact_key=$2 AND revision=2`, runID, first); err != nil {
		t.Fatal(err)
	}
	if _, err := readSelectedLatestFacts(context.Background(), tx, change); err == nil {
		t.Fatal("duplicate latest fact was hidden by batched reads")
	}
}

func TestExactRevisionEffectsComposeWithoutFallback(t *testing.T) {
	runID := uuid.NewString()
	first, err := NewFactRef(FamilyEvents, uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewFactRef(FamilyEvents, uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	effects := NewEffects()
	if err := effects.AddFacts(runID, first, second, first); err != nil {
		t.Fatal(err)
	}
	changes := effects.normalized()
	if len(changes) != 1 || len(changes[0].exact[FamilyEvents]) != 2 {
		t.Fatalf("exact union: %+v", changes)
	}
	before := effects.normalized()
	if err := effects.AddFacts(runID, FactRef{}); err == nil {
		t.Fatal("zero coordinate was admitted")
	}
	if !reflect.DeepEqual(before, effects.normalized()) {
		t.Fatal("invalid declaration changed the effect set")
	}
	third, err := NewFactRef(FamilyEvents, uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	contradictory := first
	contradictory.coordinates.FlowPath = "not-a-scalar-coordinate"
	if err := effects.AddFacts(runID, third, contradictory); err == nil {
		t.Fatal("conflicting coordinate representation was accepted")
	}
	if !reflect.DeepEqual(before, effects.normalized()) {
		t.Fatal("late conflicting coordinate partially changed the effect set")
	}
	if err := effects.Add(runID, FamilyEvents); err != nil {
		t.Fatal(err)
	}
	if err := effects.AddFacts(runID, first); err != nil {
		t.Fatal(err)
	}
	if _, exact := effects.normalized()[0].exact[FamilyEvents]; exact {
		t.Fatal("exact contribution narrowed explicit whole-family selection")
	}
	if err := effects.AddFacts(runID); err == nil {
		t.Fatal("empty exact selection became a whole-family fallback")
	}
	if err := effects.AddFacts("foreign", first); err == nil {
		t.Fatal("invalid run accepted")
	}
}

func TestExactRevisionAttemptResetRetainsOnlyBaseline(t *testing.T) {
	runID := uuid.NewString()
	effects, err := ForRun(runID, FamilyTimers)
	if err != nil {
		t.Fatal(err)
	}
	seed, err := NewFactRef(FamilyEvents, uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	if err := effects.AddFacts(runID, seed); err != nil {
		t.Fatal(err)
	}
	baseline := effects.normalized()
	reset := effects.AttemptReset()
	for i := 0; i < 3; i++ {
		reset()
		if !reflect.DeepEqual(baseline, effects.normalized()) {
			t.Fatal("rolled-back contributions survived or predeclared effects disappeared")
		}
		if err := effects.AddFact(runID, FamilyEntityMutations, uuid.NewString()); err != nil {
			t.Fatal(err)
		}
		if err := effects.Add(runID, FamilyEvents); err != nil {
			t.Fatal(err)
		}
		if err := effects.AddFact(uuid.NewString(), FamilyEntityMetadata, uuid.NewString()); err != nil {
			t.Fatal(err)
		}
	}
	reset()
	if !reflect.DeepEqual(baseline, effects.normalized()) {
		t.Fatal("attempt reset mutated its immutable baseline")
	}
}

func TestExactRevisionReferencesAndCanonicalQueries(t *testing.T) {
	runID := uuid.NewString()
	key := fanoutobligation.IntentKey{RunID: runID, TriggeringDeliveryID: uuid.NewString(), ElementRef: contracts.FanOutElementRef{
		FlowPath: "parent/child", Family: "fan_out", SemanticPath: `nodes["producer|with.delimiters"].rules[0].fan_out[0]`,
	}}
	intent, err := FanOutIntentFact(key)
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := FanOutOutcomeFact(key, 7)
	if err != nil {
		t.Fatal(err)
	}
	barrier, err := FanOutBarrierFact(key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := FanOutOutcomeFact(key, -1); err == nil {
		t.Fatal("negative ordinal accepted")
	}
	if err := NewEffects().AddFacts(uuid.NewString(), intent); err == nil {
		t.Fatal("fan-out reference changed owning run")
	}
	if _, err := NewFactRef(FamilyFanOutObligations, intent.key); err == nil {
		t.Fatal("serialized fan-out key became coordinates")
	}
	for _, family := range AllFamilies() {
		t.Run(string(family), func(t *testing.T) {
			var refs []FactRef
			if family == FamilyFanOutObligations {
				refs = []FactRef{intent, outcome, barrier}
			} else {
				id := uuid.NewString()
				if family == FamilyReplyContexts {
					id = `opaque|reply'key.with.dots`
				}
				ref, err := NewFactRef(family, id)
				if err != nil {
					t.Fatal(err)
				}
				refs = []FactRef{ref}
			}
			spec, ok := canonicalProjectionSpec(family)
			if !ok {
				t.Fatal("canonical projection missing")
			}
			query, args, err := selectedProjectionQuery(spec, family, runID, refs)
			if err != nil || len(args) < 2 || args[0] != runID || !strings.Contains(query, " AND (") {
				t.Fatalf("query=%s args=%v err=%v", query, args, err)
			}
			if family == FamilyFanOutObligations && (strings.Contains(query, "b.origin_kind") || !strings.Contains(query, "i.origin_kind")) {
				t.Fatalf("fan-out exact projection lost typed intent origin or invented barrier origin: %s", query)
			}
			for _, ref := range refs {
				if strings.Contains(query, ref.key) {
					t.Fatal("fact coordinate interpolated into SQL")
				}
			}
			whole, wholeArgs, err := selectedProjectionQuery(spec, family, runID, nil)
			if err != nil || whole != spec.query || !reflect.DeepEqual(wholeArgs, []any{runID}) {
				t.Fatal("whole-family projection changed its canonical query")
			}
			if _, _, err := selectedProjectionQuery(spec, family, runID, []FactRef{}); err == nil {
				t.Fatal("empty exact projection became a full scan")
			}
		})
	}
	if _, err := NewFactRef(FamilyReplyContexts, string([]byte{0xff})); err == nil {
		t.Fatal("invalid UTF-8 coordinate was sanitized")
	}
}

func TestExactRevisionCaptureRejectsUnknownDeletionAndForeignEvidence(t *testing.T) {
	runID, eventID := uuid.NewString(), uuid.NewString()
	ref, err := NewFactRef(FamilyEvents, eventID)
	if err != nil {
		t.Fatal(err)
	}
	refs := []FactRef{ref}
	if err := validateExactCapture(runID, FamilyEvents, refs, nil, nil); err == nil {
		t.Fatal("unknown fact was silently treated as deletion")
	}
	body := []byte(`{"event_id":"` + eventID + `","run_id":"` + runID + `"}`)
	prior := map[string]ledgerFact{eventID: {fact: body, present: true}}
	if err := validateExactCapture(runID, FamilyEvents, refs, nil, prior); err != nil {
		t.Fatalf("known owning-run fact cannot be deleted: %v", err)
	}
	if err := validateExactCapture(runID, FamilyEvents, refs, []canonicalFact{{key: eventID, fact: body}}, nil); err != nil {
		t.Fatalf("new exact fact rejected: %v", err)
	}
	prior[eventID] = ledgerFact{fact: []byte(`{"event_id":"` + eventID + `","run_id":"` + uuid.NewString() + `"}`), present: true}
	if err := validateExactCapture(runID, FamilyEvents, refs, nil, prior); err == nil {
		t.Fatal("foreign owning-run ledger body accepted")
	}
	prior[eventID] = ledgerFact{fact: []byte(`{"event_id":"` + uuid.NewString() + `","run_id":"` + runID + `"}`), present: true}
	if err := validateExactCapture(runID, FamilyEvents, refs, nil, prior); err == nil {
		t.Fatal("ledger key/body contradiction accepted")
	}
	if err := validateExactCapture(runID, FamilyEvents, append(refs, ref), nil, nil); err == nil {
		t.Fatal("duplicate selection coordinates accepted")
	}
	if err := validateExactCapture(runID, FamilyEvents, refs, []canonicalFact{{key: eventID}, {key: eventID}}, nil); err == nil {
		t.Fatal("duplicate projection row accepted")
	}
}
