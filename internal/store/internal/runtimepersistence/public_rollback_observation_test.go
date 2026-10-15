package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

type rollbackSnapshotFixture interface {
	InspectSnapshot(context.Context, func(context.Context) error) error
}

func TestPublicInputRollbackObservationsPreserveAllEventScopesBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			selected, ctx := backend.open(t).store, testAuthorActivityContext()
			direct, foreign := runForkSnapshotFixtureEvent(t, ctx, selected), runForkSnapshotFixtureEvent(t, ctx, selected)
			subscribed := seedPublicRollbackSubscribedEvent(t, ctx, selected, foreign.RunID())
			probe, restore, err := InstallTransactionProbeForTest(selected, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			assertPublicRollbackPredicates(t, ctx, selected, direct, subscribed, foreign)
			if counts := probe.Snapshot(); counts.Total.ReadCommits != 6 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("rollback observations merged independent cuts or wrote: %+v", counts)
			}
			assertPublicRollbackSnapshotRefusals(t, ctx, selected, direct.ID())
		})
	}
}

func assertPublicRollbackPredicates(t *testing.T, ctx context.Context, selected any, direct, subscribed, foreign events.Event) {
	t.Helper()
	for _, event := range []events.Event{direct, subscribed, foreign} {
		if count, err := CountTemplateEventReplayScopesForTest(ctx, selected, event.ID()); err != nil || count != 1 {
			t.Fatalf("exact event count lost direct/subscribed scope: %d %v", count, err)
		}
	}
	if count, err := CountTemplateEventReplayScopesForTest(ctx, selected, uuid.NewString()); err != nil || count != 0 {
		t.Fatalf("event count included foreign events: %d %v", count, err)
	}
	if count, err := CountTemplateSubscribedReplayScopeForTest(ctx, selected, direct.ID()); err != nil || count != 0 {
		t.Fatalf("different subscribed-only owner was broadened: %d %v", count, err)
	}
	if count, err := CountTemplateSubscribedReplayScopeForTest(ctx, selected, subscribed.ID()); err != nil || count != 1 {
		t.Fatalf("subscribed positive control missing: %d %v", count, err)
	}
}

func seedPublicRollbackSubscribedEvent(t *testing.T, ctx context.Context, selected any, run string) events.Event {
	t.Helper()
	event := eventtest.ExistingRunRootIngress(uuid.NewString(), "snapshot.fixture", "fixture", "", []byte(`{}`), 0, run, events.EventEnvelope{}, time.Now().UTC())
	if err := commitSemanticEventFixtureWithAgents(ctx, selected, event, []string{"rollback-agent"}); err != nil {
		t.Fatal(err)
	}
	row, err := ReadHistoricalReplayScopeStorageForTest(ctx, selected, event.ID())
	if err != nil || row.Scope != string(pipelineobligation.ScopeSubscribed) {
		t.Fatalf("native setup did not persist subscribed scope: %+v %v", row, err)
	}
	return event
}

func assertPublicRollbackSnapshotRefusals(t *testing.T, ctx context.Context, selected any, event string) {
	t.Helper()
	owner := selected.(rollbackSnapshotFixture)
	var retained context.Context
	if err := owner.InspectSnapshot(ctx, func(scoped context.Context) error {
		retained = scoped
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, read := range []func(context.Context, any) (int, error){
		func(ctx context.Context, selected any) (int, error) {
			return CountTemplateEventReplayScopesForTest(ctx, selected, event)
		},
	} {
		if count, err := read(retained, selected); err == nil || count != 0 {
			t.Fatalf("expired snapshot returned provisional rollback evidence: %d %v", count, err)
		}
		if err := owner.InspectSnapshot(ctx, func(scoped context.Context) error {
			if count, err := read(scoped, &sql.DB{}); err == nil || count != 0 {
				t.Fatalf("foreign raw owner returned provisional rollback evidence: %d %v", count, err)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		if count, err := read(canceled, selected); !errors.Is(err, context.Canceled) || count != 0 {
			t.Fatalf("canceled rollback observation returned evidence: %d %v", count, err)
		}
	}
}

func TestPublicInputRollbackObservationsRefuseLiveForeignQueryAndReadAckFailuresBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			probe := &operatorSnapshotProbe{readOnlyGate: true}
			selected, _, _ := newStopCommitStore(t, backend.name, probe)
			foreign := backend.open(t).store.(rollbackSnapshotFixture)
			ctx := testAuthorActivityContext()
			direct := runForkSnapshotFixtureEvent(t, ctx, selected)
			for _, operation := range []publicRollbackFailureCase{
				{"all_event_scopes", 1, func(ctx context.Context) (int, error) {
					return CountTemplateEventReplayScopesForTest(ctx, selected, direct.ID())
				}},
			} {
				t.Run(operation.name, func(t *testing.T) {
					provePublicRollbackNativeFailures(t, ctx, selected.(rollbackSnapshotFixture), foreign, probe, operation)
				})
			}
		})
	}
}

type publicRollbackFailureCase struct {
	name string
	want int
	read func(context.Context) (int, error)
}

func provePublicRollbackNativeFailures(t *testing.T, ctx context.Context, selected, foreign rollbackSnapshotFixture, probe *operatorSnapshotProbe, operation publicRollbackFailureCase) {
	t.Helper()
	if err := foreign.InspectSnapshot(ctx, func(scoped context.Context) error {
		if count, err := operation.read(scoped); err == nil || count != 0 {
			t.Fatalf("different live native snapshot supplied evidence: %d %v", count, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	queryFailure := errors.New("exact rollback observation query failed")
	_, release := probe.arm("other", 1, queryFailure, nil)
	release()
	count, err := operation.read(ctx)
	if !errors.Is(err, queryFailure) || count != 0 {
		t.Fatalf("failed query supplied successful-zero/nonzero evidence: %d %v", count, err)
	}
	assertPublicRollbackFaultReceipt(t, probe)
	probe.disarm()
	ackFailure := errors.New("exact rollback observation outer read acknowledgment lost")
	_, release = probe.arm("other", 1, nil, ackFailure)
	release()
	count, err = operation.read(ctx)
	if !errors.Is(err, ackFailure) || count != 0 {
		t.Fatalf("failed read acknowledgment supplied provisional evidence: %d %v", count, err)
	}
	assertPublicRollbackFaultReceipt(t, probe)
	probe.disarm()
	// A borrowed snapshot may observe the nonzero row, but its caller
	// must reject that value when the outer acknowledgment fails.
	_, release = probe.arm("other", 1, nil, ackFailure)
	release()
	observed := 0
	err = selected.InspectSnapshot(ctx, func(scoped context.Context) error {
		var err error
		observed, err = operation.read(scoped)
		if err != nil || observed != operation.want {
			t.Fatalf("missing nonzero provisional positive control: %d %v", observed, err)
		}
		return err
	})
	if !errors.Is(err, ackFailure) || observed != operation.want {
		t.Fatalf("outer acknowledgment fault missed the nonzero cut: %d %v", observed, err)
	}
	assertPublicRollbackFaultReceipt(t, probe)
	probe.disarm()
	if count, err := operation.read(ctx); err != nil || count != operation.want {
		t.Fatalf("fault damaged live owner or erased the positive row: %d %v", count, err)
	}
}

func assertPublicRollbackFaultReceipt(t *testing.T, probe *operatorSnapshotProbe) {
	t.Helper()
	probe.mu.Lock()
	defer probe.mu.Unlock()
	if !probe.fired || probe.seen["other"] != 1 || probe.outside != 0 || probe.writes != 0 || probe.active != 0 || probe.finished != 1 {
		t.Fatalf("rollback fault missed its exact native read or leaked work: %+v", probe)
	}
}
