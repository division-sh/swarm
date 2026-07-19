package store

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/google/uuid"
)

func TestEventOwnedReferencesRequireExistingSameRunEventParity(t *testing.T) {
	type referenceCase struct {
		name   string
		make   func(eventID, runID, referenceID string, at time.Time) events.Event
		append func(context.Context, diagnosticRuntimeLogFixtureStore, events.Event) error
	}
	cases := []referenceCase{
		{name: "child", make: func(eventID, runID, referenceID string, at time.Time) events.Event {
			return eventtest.ChildWithLineage(eventID, "test.child", "worker", "", json.RawMessage(`{}`), 1, events.EventLineage{
				RunID: runID, ParentEventID: referenceID, ExecutionMode: executionmode.Live,
			}, events.EventEnvelope{}, at)
		}},
		{name: "replay", make: func(eventID, runID, referenceID string, at time.Time) events.Event {
			return eventtest.Replay(eventID, "test.replay", "worker", "", json.RawMessage(`{}`), 1, events.EventLineage{
				RunID: runID, ParentEventID: referenceID, ExecutionMode: executionmode.Live,
			}, events.EventEnvelope{}, at)
		}},
		{name: "runtime_control", make: func(eventID, runID, referenceID string, at time.Time) events.Event {
			return eventtest.RuntimeControl(eventID, "platform.paused", "runtime", "", json.RawMessage(`{}`), 1, runID, referenceID, events.EventEnvelope{}, at)
		}},
		{name: "runtime_diagnostic", make: func(eventID, runID, referenceID string, at time.Time) events.Event {
			return eventtest.RuntimeDiagnostic(eventID, "platform.agent_started", "runtime", "", json.RawMessage(`{}`), 1, runID, referenceID, events.EventEnvelope{}, at)
		}},
		{name: "diagnostic_direct", make: func(eventID, runID, referenceID string, at time.Time) events.Event {
			return eventtest.DiagnosticDirect(eventID, events.EventTypePlatformRuntimeLog, "runtime", "", json.RawMessage(`{"message":"causal evidence"}`), 1, runID, referenceID, events.EventEnvelope{}, at)
		}, append: func(ctx context.Context, selected diagnosticRuntimeLogFixtureStore, event events.Event) error {
			return commitDiagnosticRuntimeLogFixture(ctx, selected, event)
		}},
		{name: "operator_reference", make: func(eventID, runID, referenceID string, at time.Time) events.Event {
			reference, err := events.NewOperatorReferenceProvenance(referenceID)
			if err != nil {
				panic(err)
			}
			return eventtest.OperatorInjected(eventID, "test.operator", "operator", "", json.RawMessage(`{}`), 0, runID, &reference, events.EventEnvelope{}, at)
		}},
	}

	for _, backend := range []struct {
		name string
		open func(*testing.T) authorActivityReceiptFixture
	}{
		{name: "sqlite", open: openSQLiteAuthorActivityReceiptFixture},
		{name: "postgres", open: openPostgresAuthorActivityReceiptFixture},
	} {
		for _, test := range cases {
			backend, test := backend, test
			t.Run(backend.name+"/"+test.name, func(t *testing.T) {
				t.Run("same_run", func(t *testing.T) {
					fixture := backend.open(t)
					ctx := testAuthorActivityContext()
					at := time.Date(2026, 7, 19, 18, 0, 0, 0, time.UTC)
					runID := uuid.NewString()
					referenceID := uuid.NewString()
					seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
					parent := eventtest.RootIngress(referenceID, "test.reference", "test-ingress", "", json.RawMessage(`{}`), 0, runID, "", events.EventEnvelope{}, at)
					if err := insertCanonicalEventRecordFixture(ctx, fixture.store, parent); err != nil {
						t.Fatalf("seed same-run reference: %v", err)
					}
					event := test.make(uuid.NewString(), runID, referenceID, at.Add(time.Second))
					var err error
					if test.append != nil {
						selected, ok := fixture.store.(diagnosticRuntimeLogFixtureStore)
						if !ok {
							t.Fatalf("store %T cannot commit runtime logs", fixture.store)
						}
						err = test.append(ctx, selected, event)
					} else {
						err = commitSemanticEventFixtureWithAgents(ctx, fixture.store, event, []string{"reference-proof"})
					}
					if err != nil {
						t.Fatalf("same-run reference rejected: %v", err)
					}
				})
				for _, hostile := range []string{"missing", "cross_run"} {
					t.Run(hostile, func(t *testing.T) {
						fixture := backend.open(t)
						ctx := testAuthorActivityContext()
						at := time.Date(2026, 7, 19, 18, 0, 0, 0, time.UTC)
						targetRunID := uuid.NewString()
						seedAuthorActivityReceiptRun(t, fixture, ctx, targetRunID)
						referenceID := uuid.NewString()
						if hostile == "cross_run" {
							referenceRunID := uuid.NewString()
							seedAuthorActivityReceiptRun(t, fixture, ctx, referenceRunID)
							parent := eventtest.RootIngress(referenceID, "test.reference", "test-ingress", "", json.RawMessage(`{}`), 0, referenceRunID, "", events.EventEnvelope{}, at)
							if err := insertCanonicalEventRecordFixture(ctx, fixture.store, parent); err != nil {
								t.Fatalf("seed cross-run reference: %v", err)
							}
						}
						before := eventMutationSurfaceCounts(t, fixture.db, ctx)
						event := test.make(uuid.NewString(), targetRunID, referenceID, at.Add(time.Second))
						var err error
						if test.append != nil {
							selected, ok := fixture.store.(diagnosticRuntimeLogFixtureStore)
							if !ok {
								t.Fatalf("store %T cannot commit runtime logs", fixture.store)
							}
							err = test.append(ctx, selected, event)
						} else {
							err = commitSemanticEventFixtureWithAgents(ctx, fixture.store, event, []string{"reference-proof"})
						}
						if err == nil {
							t.Fatalf("%s reference was accepted", hostile)
						}
						want := "does not exist"
						if hostile == "cross_run" {
							want = "belongs to run"
						}
						if !strings.Contains(err.Error(), want) {
							t.Fatalf("error = %v, want %q", err, want)
						}
						after := eventMutationSurfaceCounts(t, fixture.db, ctx)
						for table, count := range before {
							if after[table] != count {
								t.Fatalf("%s rows changed from %d to %d after rejected %s reference", table, count, after[table], hostile)
							}
						}
					})
				}
			})
		}
	}
}
