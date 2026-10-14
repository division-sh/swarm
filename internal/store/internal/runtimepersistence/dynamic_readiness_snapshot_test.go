package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

type readinessSnapshotFixture struct {
	receiverConfigActivationFixture
	reader pipeline.DynamicFlowRuntimeReadinessPersistence
	probe  *operatorSnapshotProbe
	plan   pipeline.CommittedFlowInstanceActivation
	fact   correlation.SourceArtifactFact
}

func newReadinessSnapshotFixture(t *testing.T, backend string) readinessSnapshotFixture {
	t.Helper()
	probe := &operatorSnapshotProbe{readOnlyGate: true}
	selected, readerDB, connector := newStopCommitStore(t, backend, probe)
	writerDB := sql.OpenDB(connector)
	t.Cleanup(func() { _ = writerDB.Close() })
	writer := newStopCommitStoreOwner(t, backend, writerDB).(agentFixtureFlowStore)
	f := newReceiverConfigActivationFixtureForStore(t, writer, false, map[string]string{"schema.yaml": "name: readiness-snapshot\n"}, nil, ownStoreTestAgentManager, nil)
	runID := correlation.RunIDFromContext(f.ctx)
	request := sqliteFlowActivationRequest(f.bundle, ".", runID, "", runID)
	request.Instance = flowidentity.Stored(request.ContractBundle, ".", runID, runID, runID, "")
	plan, err := f.manager.PrepareFlowInstanceActivation(f.ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	committed, err := (agentFixtureFlowActivationCommitter{store: writer}).CommitFlowInstanceActivation(f.ctx, plan)
	if err != nil || !committed.Acknowledged || !committed.Created {
		t.Fatalf("native pending construction: %+v %v", committed, err)
	}
	// An escaped component read cannot get another connection and hide its cut.
	readerDB.SetMaxOpenConns(1)
	readerDB.SetMaxIdleConns(1)
	return readinessSnapshotFixture{receiverConfigActivationFixture: f, reader: selected.(pipeline.DynamicFlowRuntimeReadinessPersistence), probe: probe, plan: committed, fact: mustStoreTestSourceArtifactFact(f.bundle.SourceArtifact.BundleHash())}
}

func (f readinessSnapshotFixture) inspect(ctx context.Context, surface string) (pipeline.DynamicFlowRuntimeReadinessProjection, error) {
	if surface == "source" {
		return f.reader.InspectDynamicFlowRuntimeReadinessForSource(ctx, f.fact)
	}
	items, err := f.reader.InspectDynamicFlowRuntimeReadinessForRun(ctx, correlation.RunIDFromContext(f.ctx), f.fact)
	projection := pipeline.DynamicFlowRuntimeReadinessProjection{}
	for _, item := range items {
		if item.Pending() {
			projection.CurrentPending = append(projection.CurrentPending, item)
		} else {
			projection.CurrentCompleted = append(projection.CurrentCompleted, item)
		}
	}
	return projection, err
}

func TestR7ReadinessInspectionOwnsCoherentSnapshotBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, surface := range []string{"source", "run"} {
			for _, cut := range []string{"readiness-runs", "readiness-headers", "readiness-construction"} {
				t.Run(backend+"/"+surface+"/"+cut, func(t *testing.T) {
					f := newReadinessSnapshotFixture(t, backend)
					before, err := f.inspect(f.ctx, surface)
					if err != nil || len(before.CurrentPending) != 1 || len(before.CurrentCompleted) != 0 {
						t.Fatalf("canonical pending baseline: %+v %v", before, err)
					}
					ctx, cancel := context.WithTimeout(f.ctx, 15*time.Second)
					defer cancel()
					entered, release := f.probe.arm(cut, 1, nil, nil)
					defer release()
					defer f.probe.disarm()
					var observed pipeline.DynamicFlowRuntimeReadinessProjection
					done := make(chan error, 1)
					go func() {
						var err error
						observed, err = f.inspect(ctx, surface)
						done <- err
					}()
					awaitOperatorSnapshotBarrier(t, ctx, entered)
					// The constructor's attachment owner commits while the old cut is
					// held. This proves real WAL/PG progress, not a launched writer.
					if err := f.manager.FinalizeCommittedFlowInstanceActivation(ctx, f.plan); err != nil {
						t.Fatal(err)
					}
					release()
					if err := <-done; err != nil || !reflect.DeepEqual(before, observed) {
						t.Fatalf("mixed diagnostic cut: before=%+v observed=%+v err=%v", before, observed, err)
					}
					assertOperatorSnapshotTransaction(t, f.probe, backend == "postgres")
					for _, component := range []string{"readiness-runs", "readiness-headers", "readiness-construction", "readiness-attachment"} {
						if f.probe.seen[component] != 1 {
							t.Fatalf("component escaped the single diagnostic snapshot: %s/%v", component, f.probe.seen)
						}
					}
					f.probe.disarm()
					fresh, err := f.inspect(ctx, surface)
					if err != nil || len(fresh.CurrentPending) != 0 || len(fresh.CurrentCompleted) != 1 || fresh.CurrentCompleted[0].Plan.Identity != before.CurrentPending[0].Plan.Identity {
						t.Fatalf("fresh invocation retained the old cut: %+v %v", fresh, err)
					}
				})
			}
		}
	}
}

func TestR7ReadinessInspectionFailureReleasesSnapshotBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, surface := range []string{"source", "run"} {
			for _, failure := range []string{"read", "commit", "cancel"} {
				t.Run(backend+"/"+surface+"/"+failure, func(t *testing.T) {
					f := newReadinessSnapshotFixture(t, backend)
					before, err := ReadSelectedForkApplicationStorageSnapshotForTest(f.ctx, f.store)
					if err != nil {
						t.Fatal(err)
					}
					ctx, cancel := context.WithTimeout(f.ctx, 15*time.Second)
					defer cancel()
					want := errors.New("readiness-snapshot-failure")
					var readFailure, commitFailure error
					switch failure {
					case "read":
						readFailure = want
					case "commit":
						commitFailure = want
					case "cancel":
						want = context.Canceled
					}
					entered, release := f.probe.arm("readiness-construction", 1, readFailure, commitFailure)
					defer release()
					defer f.probe.disarm()
					var result pipeline.DynamicFlowRuntimeReadinessProjection
					done := make(chan error, 1)
					go func() {
						var err error
						result, err = f.inspect(ctx, surface)
						done <- err
					}()
					awaitOperatorSnapshotBarrier(t, ctx, entered)
					if failure == "cancel" {
						cancel()
					}
					release()
					if err := <-done; !errors.Is(err, want) || !reflect.DeepEqual(result, pipeline.DynamicFlowRuntimeReadinessProjection{}) {
						t.Fatalf("failure returned partial diagnostic evidence: %+v %v", result, err)
					}
					assertOperatorSnapshotTransaction(t, f.probe, backend == "postgres")
					f.probe.disarm()
					if after, err := ReadSelectedForkApplicationStorageSnapshotForTest(f.ctx, f.store); err != nil || !reflect.DeepEqual(before, after) {
						t.Fatalf("failed read changed native evidence: %v", err)
					}
					if fresh, err := f.inspect(f.ctx, surface); err != nil || len(fresh.CurrentPending) != 1 {
						t.Fatalf("failed read leaked its pool connection: %+v %v", fresh, err)
					}
				})
			}
		}
	}
}
