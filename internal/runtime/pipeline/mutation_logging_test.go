package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"

	runtimemutationlog "github.com/division-sh/swarm/internal/runtime/mutationlog"
	"github.com/google/uuid"
)

func VerifyNativeUpdateEntityState_LogsMutationRowForStateTransitionForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture, pc, ctx, mutations := nativeLifecycleComponentForTest(t, backend, lifecycleStateFixtureForTest(t, ".", "queued", "done", "flow.transitioned"), "queued", open)
			event, err := executeNativeLifecycleTransitionForTest(t, fixture, mutations, pc, ctx, "flow.transitioned")
			if err != nil {
				t.Fatal(err)
			}
			run := runtimeRunID(ctx)
			var rows []PipelineNativeMutationRowForTest
			for _, row := range fixture.MutationHistory(ctx, run, run) {
				if row.Domain == "lifecycle_state" && row.CausedByEvent == event.ID() {
					rows = append(rows, row)
				}
			}
			if len(rows) != 1 || rows[0].Path != "" || string(rows[0].OldValue) != "\"queued\"" || string(rows[0].NewValue) != "\"done\"" || rows[0].WriterType == "" || rows[0].HandlerStep == "" {
				t.Fatalf("exact native transition mutation=%+v, want lifecycle_state empty-path queued->done with writer and step", rows)
			}
		})
	}
}

func VerifyNativeWorkflowInstanceStoreTracksFieldsGatesAndAccumulatorInMutationLogForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture, pc, ctx := nativeMutationLoggingFixtureForTest(t, backend, open)
			run := runtimeRunID(ctx)
			owner := testRunScopedWorkflowInstanceFromContext(ctx, run)
			if err := pc.workflowStore.mutate(ctx, owner, func(instance *WorkflowInstance) {
				instance.Fields = map[string]any{"status": "open", "business_status": "pending"}
				instance.Gates = map[string]bool{"g_ready": true}
				instance.StateBuckets = map[string]any{"evidence": map[string]any{"score": 1}}
			}); err != nil {
				t.Fatalf("initial native tracked-state write: %v", err)
			}
			if err := pc.workflowStore.mutate(ctx, owner, func(instance *WorkflowInstance) {
				instance.Fields = map[string]any{"status": "closed", "business_status": "approved"}
				instance.Gates = map[string]bool{"g_done": true}
				instance.StateBuckets = map[string]any{"evidence": map[string]any{"score": 2}, "notes": map[string]any{"count": 1}}
			}); err != nil {
				t.Fatalf("update native tracked state: %v", err)
			}
			inbound := nativeWorkflowJoinEventForTest(ctx, ".", run, run, "flow.transitioned", []byte(`{}`), time.Now().UTC())
			route := publishNativeWorkflowJoinEventForTest(t, fixture, pc, ctx, inbound, "lifecycle-owner")
			node := pipelineNode(t, ".", "lifecycle-owner")
			handler := pc.SemanticSource().ExecutableNodeEventHandlers(node)["flow.transitioned"]
			if _, err := executeNativeClaimedPipelineHandlerForTest(t, pc, withWorkflowNodeDeliveryRoute(ctx, route), node, handler, workflowTriggerContext{Event: inbound, State: mustCurrentWorkflowState(t, pc, ctx, owner.Route, run), HandlerEventKey: "flow.transitioned"}); err != nil {
				t.Fatalf("native compiled stage transition: %v", err)
			}
			assertNativeTrackedMutationProjectionForTest(t, fixture, ctx, run, "lifecycle_state:", "authored_field:status", "authored_field:business_status", "gate:g_ready", "gate:g_done", "accumulator:evidence", "accumulator:notes")
		})
	}
}

func VerifyNativeWorkflowInstanceStoreReplaysContainedStateMapListProjectionForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture, pc, ctx := nativeMutationLoggingFixtureForTest(t, backend, open)
			run := runtimeRunID(ctx)
			owner := testRunScopedWorkflowInstanceFromContext(ctx, run)
			if err := pc.workflowStore.mutate(ctx, owner, func(instance *WorkflowInstance) {
				instance.Fields = map[string]any{"verticals": map[string]any{"north": map[string]any{"status": "active", "active_jobs": []any{}}}, "tags": []any{"new"}}
			}); err != nil {
				t.Fatalf("initial native contained state: %v", err)
			}
			contained := map[string]any{"north": map[string]any{"status": "busy", "active_jobs": []any{map[string]any{"id": "job-1", "title": "Build"}}}}
			if err := pc.workflowStore.mutate(ctx, owner, func(instance *WorkflowInstance) {
				instance.Fields = map[string]any{"verticals": contained, "tags": []any{"new", "vip"}}
			}); err != nil {
				t.Fatalf("update native contained state: %v", err)
			}
			loaded, found, err := pc.workflowStore.Load(ctx, owner)
			if err != nil || !found {
				t.Fatalf("load native contained state: found=%t err=%v", found, err)
			}
			if got := mustCanonicalJSON(t, loaded.Fields["verticals"]); got != mustCanonicalJSON(t, contained) {
				t.Fatalf("loaded verticals = %s, want %s", got, mustCanonicalJSON(t, contained))
			}
			if got := mustCanonicalJSON(t, loaded.Fields["tags"]); got != `["new","vip"]` {
				t.Fatalf("loaded tags = %s, want [new,vip]", got)
			}
			assertNativeTrackedMutationProjectionForTest(t, fixture, ctx, run, "authored_field:verticals", "authored_field:tags")
		})
	}
}

func nativeMutationLoggingFixtureForTest(t *testing.T, backend string, open pipelineDeliveryNativeOpenerForTest) (*PipelineDeliveryNativeFixtureForTest, *PipelineCoordinator, context.Context) {
	t.Helper()
	bundle := loadWorkflowTempBundle(t, map[string]string{
		"schema.yaml":   "name: native-mutation-log\nstages:\n  queued: {}\n  done: {}\n",
		"entities.yaml": "test_entity:\n  status: text\n  business_status: text\n  verticals: map[text]Vertical\n  tags: \"[text]\"\n",
		"types.yaml":    "types:\n  Job:\n    id: text\n    title: text\n  Vertical:\n    status: text\n    active_jobs: \"[Job]\"\n",
		"events.yaml":   "flow.transitioned:\n",
		"nodes.yaml":    "lifecycle-owner:\n  execution_type: system_node\n  event_handlers:\n    flow.transitioned: {advances_to: done}\n",
	})
	fixture, pc, ctx := nativePilotPipelineForTest(t, backend, bundle, open)
	if err := fixture.Construct(ctx, constructedScenarioInstanceForTest(t, pc.SemanticSource(), ctx, ".")); err != nil {
		t.Fatal(err)
	}
	return fixture, pc, ctx
}

func VerifyNativeApplyWorkflowGateMutation_LogsMutationRowForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture, pc, ctx, _ := nativeLifecycleComponentForTest(t, backend, lifecycleStateFixtureForTest(t, ".", "queued", "done", "flow.transitioned"), "queued", open)
			run := runtimeRunID(ctx)
			if err := pc.applyWorkflowGateForTest(ctx, testWorkflowInstanceRoute(run), "workflow.ready", "g_ready", false); err != nil {
				t.Fatalf("native gate mutation: %v", err)
			}
			assertNativeTrackedMutationProjectionForTest(t, fixture, ctx, run, "gate:g_ready")
		})
	}
}

func VerifyNativeAccumulatorAppend_LogsMutationRowForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture, pc, ctx, _ := nativeLifecycleComponentForTest(t, backend, lifecycleStateFixtureForTest(t, ".", "queued", "done", "flow.transitioned"), "queued", open)
			run := runtimeRunID(ctx)
			if err := commitAccumulatorAppendForTest(ctx, pc, testWorkflowInstanceRoute(run), run, ".", "research", map[string]any{"summary": "done"}); err != nil {
				t.Fatalf("native accumulator append: %v", err)
			}
			assertNativeTrackedMutationProjectionForTest(t, fixture, ctx, run, "accumulator:journal")
		})
	}
}

func assertNativeTrackedMutationProjectionForTest(t *testing.T, fixture *PipelineDeliveryNativeFixtureForTest, ctx context.Context, entity string, fields ...string) {
	t.Helper()
	want, records, err := fixture.TrackedMutationProjection(ctx, runtimeRunID(ctx), entity)
	if err != nil {
		t.Fatal(err)
	}
	keys := make([]string, 0, len(records))
	for _, record := range records {
		keys = append(keys, string(record.Domain)+":"+record.Path)
	}
	for _, field := range fields {
		if !containsMutationField(keys, field) {
			t.Fatalf("mutation fields missing %q: %v", field, keys)
		}
	}
	got, err := runtimemutationlog.ReconstructEntityStateProjection(records)
	if err != nil || !trackedStatesEqual(got, want) {
		t.Fatalf("paired native mutation reconstruction mismatch: got=%s want=%s err=%v", mustCanonicalJSON(t, got), mustCanonicalJSON(t, want), err)
	}
}

func VerifyNativeMutationLogSchemaRejectsMissingDomainPathForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture, _, ctx := nativeMutationLoggingFixtureForTest(t, backend, open)
			run := runtimeRunID(ctx)
			before, history, err := fixture.TrackedMutationProjection(ctx, run, run)
			if err != nil {
				t.Fatal(err)
			}
			if err := fixture.ProbeMutationMissingDomainPath(ctx, run, run); err == nil || !strings.Contains(strings.ToLower(err.Error()), "check constraint") {
				t.Fatalf("missing-domain-path probe err=%v, want database check-constraint refusal", err)
			}
			if err := fixture.ProbeMutationMissingDomainPath(ctx, uuid.NewString(), run); !errors.Is(err, sql.ErrNoRows) {
				t.Fatalf("foreign run probe err=%v, want missing exact entity", err)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if err := fixture.ProbeMutationMissingDomainPath(cancelled, run, run); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled probe err=%v, want cancellation", err)
			}
			after, records, err := fixture.TrackedMutationProjection(ctx, run, run)
			if err != nil || !trackedStatesEqual(before, after) || mustCanonicalJSON(t, records) != mustCanonicalJSON(t, history) {
				t.Fatalf("schema probe changed exact entity or mutation history: err=%v", err)
			}
			if counts := fixture.Transactions(); counts.Active != 0 {
				t.Fatalf("schema probe leaked native transaction: %+v", counts)
			}
		})
	}
}

func VerifyNativeMutationLoggedPipelineWritesFailClosedWithoutEntityMutationsTableForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	t.Run("state transition", func(t *testing.T) {
		for _, backend := range []string{"sqlite", "postgres"} {
			t.Run(backend, func(t *testing.T) {
				fixture, pc, ctx, mutations := nativeLifecycleComponentForTest(t, backend, lifecycleStateFixtureForTest(t, ".", "queued", "done", "flow.transitioned"), "queued", open)
				run := runtimeRunID(ctx)
				event := nativeWorkflowJoinEventForTest(ctx, ".", run, run, "flow.transitioned", []byte("{}"), time.Now().UTC())
				node := pipelineNode(t, ".", "lifecycle-owner")
				route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustExistingEntityTarget(event.Envelope().Target)}
				if err := fixture.PublishNode(ctx, event, route); err != nil {
					t.Fatal(err)
				}
				if err := fixture.HideMutationTable(ctx, true); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := fixture.HideMutationTable(ctx, false); err != nil {
						t.Error(err)
					}
				})
				handler := pc.SemanticSource().ExecutableNodeEventHandlers(node)["flow.transitioned"]
				_, err := executeNativeClaimedPipelineHandlerForTest(t, pc, withWorkflowNodeDeliveryRoute(ctx, route), node, handler, workflowTriggerContext{Event: event, State: mustCurrentWorkflowState(t, pc, ctx, testWorkflowInstanceRoute(run), run), HandlerEventKey: "flow.transitioned"})
				if err == nil || !strings.Contains(err.Error(), "entity_mutations") {
					t.Fatalf("missing journal did not reject native writer: %v", err)
				}
				loaded, found, err := fixture.Persistence.LoadWorkflowInstance(ctx, testRunScopedWorkflowInstanceFromContext(ctx, run))
				if err != nil || !found || loaded.CurrentState != "queued" {
					t.Fatalf("missing journal mutated state: %+v found=%t error=%v", loaded, found, err)
				}
				if upserts, cancellations := mutations.schedules(); len(upserts)+len(cancellations) != 0 {
					t.Fatal("rejected mutation published lifecycle receipts")
				}
			})
		}
	})
}

func VerifyNativeMutationLoggedGateAndAccumulatorWritesFailClosedWithoutJournalForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, operation := range []string{"gate mutation", "accumulator append"} {
		for _, backend := range []string{"sqlite", "postgres"} {
			t.Run(operation+"/"+backend, func(t *testing.T) {
				fixture, pc, ctx, _ := nativeLifecycleComponentForTest(t, backend, lifecycleStateFixtureForTest(t, ".", "queued", "done", "flow.transitioned"), "queued", open)
				run := runtimeRunID(ctx)
				owner := testRunScopedWorkflowInstanceFromContext(ctx, run)
				before, found, err := fixture.Persistence.LoadWorkflowInstance(ctx, owner)
				if err != nil || !found {
					t.Fatalf("load before missing-journal cut: found=%t err=%v", found, err)
				}
				if err := fixture.HideMutationTable(ctx, true); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := fixture.HideMutationTable(ctx, false); err != nil {
						t.Error(err)
					}
				})
				if operation == "gate mutation" {
					err = pc.applyWorkflowGateForTest(ctx, owner.Route, "workflow.ready", "g_ready", false)
				} else {
					err = commitAccumulatorAppendForTest(ctx, pc, owner.Route, run, ".", "research", map[string]any{"summary": "done"})
				}
				if err == nil || !strings.Contains(err.Error(), "entity_mutations") {
					t.Fatalf("native %s err=%v, want entity_mutations failure", operation, err)
				}
				after, found, err := fixture.Persistence.LoadWorkflowInstance(ctx, owner)
				if err != nil || !found || mustCanonicalJSON(t, after) != mustCanonicalJSON(t, before) {
					t.Fatalf("rejected %s changed exact native state: before=%+v after=%+v found=%t err=%v", operation, before, after, found, err)
				}
				if after.Gates["g_ready"] {
					t.Fatal("rejected mutation set g_ready")
				}
				if _, present := after.StateBuckets["journal"]; present {
					t.Fatal("rejected append left the journal bucket")
				}
				if counts := fixture.Transactions(); counts.Active != 0 {
					t.Fatalf("rejected mutation leaked transaction: %+v", counts)
				}
			})
		}
	}
}

func trackedStatesEqual(left, right runtimemutationlog.EntityStateProjection) bool {
	return mustCanonicalJSONForCompare(left) == mustCanonicalJSONForCompare(right)
}

func mustCanonicalJSON(t *testing.T, value any) string {
	t.Helper()
	return mustCanonicalJSONForCompare(value)
}

func mustCanonicalJSONForCompare(value any) string {
	raw, _ := json.Marshal(value)
	return string(raw)
}

func containsMutationField(items []string, want string) bool {
	for _, item := range items {
		if strings.TrimSpace(item) == strings.TrimSpace(want) {
			return true
		}
	}
	return false
}
