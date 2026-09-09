package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/diaglog"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/google/uuid"
)

func lifecycleCausalFixture(t *testing.T, sqlite bool, kind string) (lifecycleDiagnosticTestStore, *sql.DB, context.Context, diaglog.LifecycleDiagnostic, string) {
	t.Helper()
	store, db := newLifecycleDiagnosticTestStore(t, sqlite)
	ctx := testAuthorActivityContext()
	seed := createNamedLifecycleDiagnostic(t, ctx, store, "lineage-seed")
	ctx = runtimecorrelation.WithRunID(ctx, seed.Identity.RunID)
	logger := runtimepkg.NewRuntimeLogger(store, executionposture.Live, storeTestPayloadAdmitter)
	if err := logger.Log(ctx, runtimepkg.RuntimeLogEntry{Level: diaglog.LevelInfo, Component: "lineage", Action: "parent"}); err != nil {
		t.Fatal(err)
	}
	var parent string
	if err := db.QueryRow("SELECT event_id FROM events WHERE event_name='platform.runtime_log'").Scan(&parent); err != nil {
		t.Fatal(err)
	}
	lineage := runtimecorrelation.RuntimeLineage{Owner: "diagnostic-producer", RunID: seed.Identity.RunID, RowCategory: runtimecorrelation.RuntimeLineageRowCategoryRuntimeContainer}
	switch kind {
	case "explicit":
		lineage.ParentEventID = parent
	case "subject":
		lineage.SubjectEventID = parent
	case "missing":
		lineage.ParentEventID = uuid.NewString()
	case "malformed":
		lineage.ParentEventID = "not-an-event-id"
	case "missing_subject":
		lineage.SubjectEventID = uuid.NewString()
	case "foreign":
		lineage.ParentEventID = uuid.NewString()
		foreign := eventtest.RunCreatingRootIngress(lineage.ParentEventID, "test.reference", "test-ingress", "", json.RawMessage(`{}`), 0, uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC())
		if err := commitSemanticEventFixture(ctx, store, foreign); err != nil {
			t.Fatal(err)
		}
	}
	producer := runtimecorrelation.WithRuntimeLineage(ctx, lineage)
	item := createNamedLifecycleDiagnostic(t, producer, store, "lineage-worker")
	return store, db, ctx, item, parent
}

func diagnosticProjectionLineage(t *testing.T, db *sql.DB, sqlite bool, item diaglog.LifecycleDiagnostic) (string, string, string) {
	t.Helper()
	query := "SELECT run_id,source_event_id FROM events WHERE json_extract(payload,'$.details.outbox_id')=?"
	receiptQuery := "SELECT projection FROM agent_lifecycle_diagnostic_outbox WHERE outbox_id=?"
	if !sqlite {
		query = "SELECT run_id::text,source_event_id::text FROM events WHERE payload->'details'->>'outbox_id'=$1"
		receiptQuery = "SELECT projection FROM agent_lifecycle_diagnostic_outbox WHERE outbox_id=$1"
	}
	var run, parent sql.NullString
	if err := db.QueryRow(query, item.OutboxID).Scan(&run, &parent); err != nil {
		t.Fatal(err)
	}
	var raw []byte
	if err := db.QueryRow(receiptQuery, item.OutboxID).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var receipt struct {
		RunID              string `json:"run_id"`
		ParentEventID      string `json:"parent_event_id"`
		LineageDisposition string `json:"lineage_disposition"`
	}
	if err := json.Unmarshal(raw, &receipt); err != nil {
		t.Fatal(err)
	}
	if receipt.RunID != run.String || receipt.ParentEventID != parent.String {
		t.Fatalf("receipt lineage differs from persisted event: %s; run=%v parent=%v", raw, run, parent)
	}
	return run.String, parent.String, receipt.LineageDisposition
}

func TestLifecycleDiagnosticPersistsProducerCausalLineage(t *testing.T) {
	for _, sqlite := range []bool{true, false} {
		for _, kind := range []string{"explicit", "subject", "parentless", "missing", "malformed", "missing_subject", "foreign"} {
			t.Run(fmt.Sprintf("sqlite=%t/%s", sqlite, kind), func(t *testing.T) {
				store, db, ctx, item, parent := lifecycleCausalFixture(t, sqlite, kind)
				logger := runtimepkg.NewRuntimeLogger(store, executionposture.Live, storeTestPayloadAdmitter)
				if kind == "explicit" || kind == "subject" {
					lineage, err := item.ProducerLineage()
					if err != nil {
						t.Fatal(err)
					}
					control := runtimecorrelation.WithRuntimeLineage(ctx, lineage)
					if err := logger.Log(control, runtimepkg.RuntimeLogEntry{Level: diaglog.LevelInfo, Component: "lineage", Action: "ordinary-control"}); err != nil {
						t.Fatal(err)
					}
					var controls int
					if err := db.QueryRow("SELECT count(*) FROM events WHERE source_event_id=$1", parent).Scan(&controls); err != nil || controls != 1 {
						t.Fatalf("ordinary causal control=%d err=%v", controls, err)
					}
				}
				before, err := store.ListPendingAgentLifecycleDiagnostics(ctx, 100)
				if err != nil {
					t.Fatal(err)
				}
				foreignConsumer := runtimecorrelation.WithRuntimeLineage(ctx, runtimecorrelation.RuntimeLineage{RunID: uuid.NewString(), ParentEventID: uuid.NewString()})
				err = logger.ProjectLifecycleDiagnostic(foreignConsumer, item)
				if kind == "missing" || kind == "malformed" || kind == "missing_subject" || kind == "foreign" {
					if err == nil {
						t.Fatal("invalid causal lineage was acknowledged")
					}
					if diagnosticLogCount(t, db, item.OutboxID) != 0 {
						t.Fatal("rejected lineage left a log")
					}
					pending, err := store.ListPendingAgentLifecycleDiagnostics(ctx, 100)
					if err != nil || len(pending) != len(before) {
						t.Fatalf("pending=%d err=%v", len(pending), err)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				wantDisposition := "causal_" + kind
				if kind == "parentless" {
					parent, wantDisposition = "", "parentless"
				}
				for i := 0; i < 2; i++ {
					if err := logger.ProjectLifecycleDiagnostic(foreignConsumer, item); err != nil {
						t.Fatal(err)
					}
					run, actual, disposition := diagnosticProjectionLineage(t, db, sqlite, item)
					if run != item.Identity.RunID || actual != parent || disposition != wantDisposition {
						t.Fatalf("persisted run=%s parent=%s disposition=%s; want %s/%s/%s", run, actual, disposition, item.Identity.RunID, parent, wantDisposition)
					}
				}
				if diagnosticLogCount(t, db, item.OutboxID) != 1 {
					t.Fatal("replay duplicated log")
				}
			})
		}
	}
}

func TestLifecycleDiagnosticCausalHistoryAfterReset(t *testing.T) {
	for _, sqlite := range []bool{true, false} {
		for _, kind := range []string{"explicit", "subject"} {
			for _, before := range []bool{false, true} {
				t.Run(fmt.Sprintf("sqlite=%t/%s/projected=%t", sqlite, kind, before), func(t *testing.T) {
					store, db, ctx, item, _ := lifecycleCausalFixture(t, sqlite, kind)
					logger := runtimepkg.NewRuntimeLogger(store, executionposture.Live, nil)
					if before {
						if err := logger.ProjectLifecycleDiagnostic(ctx, item); err != nil {
							t.Fatal(err)
						}
					}
					var receiptBefore []byte
					if before {
						if err := db.QueryRow("SELECT projection FROM agent_lifecycle_diagnostic_outbox WHERE outbox_id=$1", item.OutboxID).Scan(&receiptBefore); err != nil {
							t.Fatal(err)
						}
					}
					capability, err := agentfixture.ProcessCapability(t, ctx, store)
					if err != nil {
						t.Fatal(err)
					}
					request := admitRetainedResetCleanupProof(t, capability, store, item.Identity.RunID, false)
					if _, err := capability.ApplyDestructiveResetCleanup(ctx, request, nil); err != nil {
						t.Fatal(err)
					}
					for i := 0; i < 2; i++ {
						if err := logger.ProjectLifecycleDiagnostic(ctx, item); err != nil {
							t.Fatal(err)
						}
					}
					if before {
						var receiptAfter []byte
						if err := db.QueryRow("SELECT projection FROM agent_lifecycle_diagnostic_outbox WHERE outbox_id=$1", item.OutboxID).Scan(&receiptAfter); err != nil {
							t.Fatal(err)
						}
						if string(receiptBefore) != string(receiptAfter) {
							t.Fatal("historical replay changed the original causal receipt")
						}
						if diagnosticLogCount(t, db, item.OutboxID) != 0 {
							t.Fatal("receipt replay resurrected event")
						}
					} else {
						run, parent, disposition := diagnosticProjectionLineage(t, db, sqlite, item)
						if run != "" || parent != "" || disposition != "historical_cleanup" {
							t.Fatalf("historical lineage=%s/%s/%s", run, parent, disposition)
						}
					}
					var runs int
					if err := db.QueryRow("SELECT count(*) FROM runs").Scan(&runs); err != nil || runs != 0 {
						t.Fatalf("runs=%d err=%v", runs, err)
					}
				})
			}
		}
	}
}
