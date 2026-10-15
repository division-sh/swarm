package runtimepersistence

import (
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestSelectedForkRefusalObservationsRetainExactPhysicalScopeBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			selected, ctx := backend.open(t).store, testAuthorActivityContext()
			source := runForkSnapshotFixtureEvent(t, ctx, selected)
			foreign := runForkSnapshotFixtureEvent(t, ctx, selected)
			for _, coordinate := range []struct{ run, parent string }{
				{source.RunID(), source.ID()},
				{foreign.RunID(), foreign.ID()},
			} {
				envelope := source.Envelope()
				envelope.Source = events.RouteIdentity{FlowID: ".", FlowInstance: coordinate.run}
				replay := eventtest.ReplayForProducer(uuid.NewString(), source.Type(),
					eventtest.Producer(events.EventProducerExternal, "fixture"), "", source.Payload(), 0,
					events.EventLineage{RunID: coordinate.run, ParentEventID: coordinate.parent, ExecutionMode: source.ExecutionMode()}, envelope, source.CreatedAt())
				if err := commitSemanticEventFixture(ctx, selected, replay); err != nil {
					t.Fatal(err)
				}
			}
			// A physical observer must retain run inequality even when the requested
			// source coordinate is foreign; it must not infer source ownership.
			envelope := source.Envelope()
			envelope.Source = events.RouteIdentity{FlowID: ".", FlowInstance: foreign.RunID()}
			invalid := eventtest.ReplayForProducer(uuid.NewString(), source.Type(),
				eventtest.Producer(events.EventProducerExternal, "fixture"), "", source.Payload(), 0,
				events.EventLineage{RunID: foreign.RunID(), ParentEventID: source.ID(), ExecutionMode: source.ExecutionMode()}, envelope, source.CreatedAt())
			if err := commitSemanticEventFixture(ctx, selected, invalid); err == nil || !strings.Contains(err.Error(), "causal source event") {
				t.Fatalf("foreign causal source bypassed native admission: %v", err)
			}
			probe, restore, err := InstallTransactionProbeForTest(selected, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			for _, coordinate := range []struct {
				run, event string
				want       int
			}{
				{source.RunID(), source.ID(), 0},
				{foreign.RunID(), source.ID(), 1},
				{source.RunID(), foreign.ID(), 1},
				{source.RunID(), uuid.NewString(), 0},
			} {
				got, err := ReadSelectedSourceRefusalStorageForTest(ctx, selected, coordinate.run, coordinate.event)
				want := map[string]int{
					"fork_runs": 0, "selected_contract_bindings": 0, "selected_contract_executions": 0,
					"selected_contract_branch_divergences": 0, "selected_contract_route_recoveries": 0,
					"delivery_event_replays": 0, "fork_events": coordinate.want,
				}
				if err != nil || !reflect.DeepEqual(got, want) {
					t.Fatalf("source refusal lost exact physical predicates: got=%+v want=%+v err=%v", got, want, err)
				}
			}
			for _, coordinate := range []struct {
				run  string
				want int
			}{
				{source.RunID(), 2}, {foreign.RunID(), 2}, {uuid.NewString(), 0},
			} {
				got, err := ReadSelectedForkRefusalStorageForTest(ctx, selected, coordinate.run)
				want := map[string]int{
					"events": coordinate.want, "event_deliveries": 0,
					"run_fork_selected_contract_executions": 0, "run_fork_selected_contract_divergences": 0,
					"run_fork_selected_contract_route_rows": 0, "run_fork_delivery_event_replay_lineages": 0,
				}
				if err != nil || !reflect.DeepEqual(got, want) {
					t.Fatalf("fork refusal lost exact physical predicates: got=%+v want=%+v err=%v", got, want, err)
				}
			}
			if got, err := CountSelectedForkRunsForSourceForTest(ctx, selected, source.RunID()); err != nil || got != 0 {
				t.Fatalf("ordinary source acquired fork lineage: %d/%v", got, err)
			}
			if counts := probe.Snapshot(); counts.Total.ReadCommits != 8 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("refusal reads escaped original coordinator: %+v", counts)
			}
		})
	}
}

func TestSelectedForkRefusalObservationsDetectGenuineMaterializedDescendantsBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := newSnapshotOwnershipFixture(t, backend, false, false)
			fork, err := fixture.store.MaterializeRunFork(fixture.ctx, runfork.RunForkMaterializeRequest{SourceRunID: fixture.runID, At: fixture.eventID})
			if err != nil {
				t.Fatal(err)
			}
			got, err := ReadSelectedSourceRefusalStorageForTest(fixture.ctx, fixture.store, fixture.runID, fixture.eventID)
			if err != nil || got["fork_runs"] != 1 {
				t.Fatalf("refusal witness hid a genuinely materialized descendant: %+v/%v", got, err)
			}
			if count, err := CountSelectedForkRunsForSourceForTest(fixture.ctx, fixture.store, fixture.runID); err != nil || count != 1 {
				t.Fatalf("scalar refusal witness hid a genuinely materialized descendant: %d/%v", count, err)
			}
			if count, err := CountSelectedForkRunsForSourceForTest(fixture.ctx, fixture.store, fork.ForkRunID); err != nil || count != 0 {
				t.Fatalf("fork counted itself as its descendant: %d/%v", count, err)
			}
		})
	}
}
