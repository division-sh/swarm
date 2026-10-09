package runforkrevision

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
)

func TestRunStartProjectionHasOnlySemanticCoordinates(t *testing.T) {
	projection := StartProjection{Version: 1, SourceBundleHash: ("bundle-v2:sha256:" + strings.Repeat("a", 64)),
		FirstTurnEventID: uuid.NewString(), Facts: []StartFact{{Family: FamilyEntityMetadata, Key: uuid.NewString()}}}
	raw, err := json.Marshal(projection)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeStartProjection(raw)
	if err != nil || !reflect.DeepEqual(got, projection) {
		t.Fatalf("start projection round trip: got=%+v err=%v", got, err)
	}
	for _, fragment := range []string{`"payload"`, `"claim"`, `"lease"`, `"readiness"`, `"session"`} {
		if strings.Contains(string(raw), fragment) {
			t.Fatalf("start duplicated operational or payload evidence: %s", raw)
		}
	}
}

func TestRunStartProjectionRejectsIngressAndOperationalFacts(t *testing.T) {
	firstTurn := uuid.NewString()
	base := StartProjection{Version: 1, SourceBundleHash: ("bundle-v2:sha256:" + strings.Repeat("b", 64)), FirstTurnEventID: firstTurn, Facts: []StartFact{}}
	for _, family := range []Family{FamilyAgentSessions, FamilyAgentTurns, FamilyAgentConversationAudits, FamilyCommittedReplayScopes, "unknown"} {
		t.Run(string(family), func(t *testing.T) {
			projection := base
			projection.Facts = []StartFact{{Family: family, Key: uuid.NewString()}}
			if err := projection.Validate(); err == nil {
				t.Fatal("excluded family admitted as initial facts")
			}
		})
	}
	base.Facts = []StartFact{{Family: FamilyEvents, Key: firstTurn}}
	if err := base.Validate(); err == nil {
		t.Fatal("creating ingress admitted in its exclusive start")
	}
}

func TestRunStartProjectionRequiresClosedCanonicalShape(t *testing.T) {
	for name, raw := range map[string]string{
		"missing facts":   `{"version":1,"source_bundle_hash":"` + ("bundle-v2:sha256:" + strings.Repeat("c", 64)) + `"}`,
		"duplicate field": `{"version":1,"version":1,"source_bundle_hash":"` + ("bundle-v2:sha256:" + strings.Repeat("c", 64)) + `","facts":[]}`,
		"unknown field":   `{"version":1,"source_bundle_hash":"` + ("bundle-v2:sha256:" + strings.Repeat("c", 64)) + `","facts":[],"payload":{}}`,
		"trailing":        `{"version":1,"source_bundle_hash":"` + ("bundle-v2:sha256:" + strings.Repeat("c", 64)) + `","facts":[]} {}`,
		"foreign version": `{"version":2,"source_bundle_hash":"` + ("bundle-v2:sha256:" + strings.Repeat("c", 64)) + `","facts":[]}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeStartProjection([]byte(raw)); err == nil {
				t.Fatal("malformed start projection admitted")
			}
		})
	}
}

func TestRunStartProjectionContributionIsExplicitAndRetryLocal(t *testing.T) {
	runID, firstTurn := uuid.NewString(), uuid.NewString()
	effects := NewEffects()
	reset := effects.AttemptReset()
	metadata, err := NewFactRef(FamilyEntityMetadata, uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	if err := effects.AddStartFacts(runID, metadata); err == nil {
		t.Fatal("ordinary mutation minted a run start")
	}
	if err := effects.DeclareStart(runID, ("bundle-v2:sha256:" + strings.Repeat("d", 64)), firstTurn); err != nil {
		t.Fatal(err)
	}
	if err := effects.AddStartFacts(runID, metadata, metadata); err != nil {
		t.Fatal(err)
	}
	if len(effects.starts[runID].Facts) != 1 || !effects.HasDeclarations() {
		t.Fatal("start contribution not deduplicated or finalized")
	}
	before := cloneStarts(effects.starts)
	creatingIngress, err := NewFactRef(FamilyEvents, firstTurn)
	if err != nil {
		t.Fatal(err)
	}
	if err := effects.AddStartFacts(runID, metadata, creatingIngress); err == nil || !reflect.DeepEqual(before, effects.starts) {
		t.Fatal("failed contribution changed the start selection")
	}
	if err := effects.DeclareStart(runID, ("bundle-v2:sha256:" + strings.Repeat("e", 64)), firstTurn); err == nil {
		t.Fatal("conflicting start replaced source identity")
	}
	reset()
	if len(effects.starts) != 0 || effects.HasDeclarations() {
		t.Fatal("rolled-back start contribution survived the next attempt")
	}
	if err := effects.DeclareStart(runID, ("bundle-v2:sha256:" + strings.Repeat("e", 64)), firstTurn); err != nil {
		t.Fatal(err)
	}
	if len(effects.starts[runID].Facts) != 0 {
		t.Fatal("retry inherited old initial coordinates")
	}
}

func TestRunStartPublicationRequiresOneCreationOnBothDialects(t *testing.T) {
	for _, postgres := range []bool{false, true} {
		for _, affected := range []int64{0, 1, 2} {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			mock.ExpectBegin()
			tx, err := db.Begin()
			if err != nil {
				t.Fatal(err)
			}
			projection := StartProjection{Version: 1, SourceBundleHash: ("bundle-v2:sha256:" + strings.Repeat("f", 64)), Facts: []StartFact{}}
			mock.ExpectExec(`UPDATE run_fork_revision_heads SET start_revision`).WithArgs("run", int64(3), sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(0, affected))
			err = publishStart(context.Background(), tx, postgres, "run", 3, projection)
			if (err == nil) != (affected == 1) {
				t.Fatalf("postgres=%v affected=%d err=%v", postgres, affected, err)
			}
			mock.ExpectRollback()
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
			_ = db.Close()
		}
	}
}
