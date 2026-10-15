package runforkexecution

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestSelectedOperationReplayRetainsOriginalLatestCutBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx, request := ownerlessSelectedOperationFixture(t, backend)
			request.ForkOperation.ForkEventID, request.At = "", ""
			materialized := abandonSelectedOperationPreparation(t, ctx, request)
			selected := request.SourceLoader.(SourceArtifactSelectedContractSourceLoader).Store
			advanced := eventtest.ExistingRunRootIngressWithRoutingSourceAndMode(uuid.NewString(), "item.received", "source-runtime", "", []byte(`{}`), 0,
				request.SourceRunID, events.EventEnvelope{}, events.NoRoutingSource(), time.Unix(1700002300, 0).UTC(), executionmode.Mock)
			storetest.CommitSemanticEventWithRoutes(t, ctx, selected, advanced,
				[]events.DeliveryRoute{selectedExecutionEntitylessNodeRoute("source-only-node")}, pipelineobligation.ScopeSubscribed)
			storetest.CaptureRunForkSnapshot(t, ctx, selected, request.SourceRunID)
			latest, err := request.Owner.ports.fork.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: request.SourceRunID})
			if err != nil || latest.ForkPoint.EventID != advanced.ID() || latest.ForkPoint.SameIdentity(materialized.ForkPoint) {
				t.Fatalf("source advancement precondition failed: point=%+v err=%v", latest.ForkPoint, err)
			}
			replay := *request.ForkOperation
			replay.OperationID, replay.TargetBundleHash = uuid.NewString(), "sha256:stale-loser-alias"
			request.ForkOperation, request.ExpectedBundleHash = &replay, replay.TargetBundleHash
			result, err := ExecuteSelectedContractRunFork(ctx, request)
			if err != nil || !result.Activation.Activated || result.Materialization.ForkRunID != materialized.ForkRunID ||
				!result.Materialization.ForkPoint.SameIdentity(materialized.ForkPoint) || !reflect.DeepEqual(result.Materialization.DataPins, materialized.DataPins) {
				t.Fatalf("replay adopted the later source/losing selection: result=%+v err=%v", result, err)
			}
			record, found, err := request.Owner.ports.fork.LoadForkOperation(ctx, replay.Actor, replay.IdempotencyKey, replay.TransportHash)
			availability, bundleErr := request.Owner.ports.fork.LoadRunBundleAvailability(ctx, materialized.ForkRunID)
			if err != nil || !found || bundleErr != nil || record.Request.TargetBundleHash == replay.TargetBundleHash ||
				availability.BundleHash != record.Request.TargetBundleHash || !record.Request.ResolvedPoint.SameIdentity(materialized.ForkPoint) {
				t.Fatalf("replay lost original durable cut/bundle: record=%+v bundle=%+v err=%v/%v", record, availability, err, bundleErr)
			}
		})
	}
}

func TestSelectedOperationSuccessorReturnsRecoveredWinnerBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx, request := ownerlessSelectedOperationFixture(t, backend)
			materialized := abandonSelectedOperationPreparation(t, ctx, request)
			selected := request.SourceLoader.(SourceArtifactSelectedContractSourceLoader).Store
			if err := request.Owner.RetireSelectedContexts(ctx); err != nil {
				t.Fatal(err)
			}
			if err := request.AgentRuntime.ProcessCapability.Release(ctx); err != nil {
				t.Fatal(err)
			}
			process := worklifetime.NewProcess()
			t.Cleanup(func() {
				process.Retire()
				if _, err := process.Join(context.Background()); err != nil {
					t.Error(err)
				}
			})
			ctx = worklifetime.WithProcess(ctx, process)
			capability := selectedContractTestProcessCapability(t, ctx, selected.(startupownership.Store))
			var successor SelectedContractExecutionOwner
			switch native := selected.(type) {
			case *store.SQLiteRuntimeStore:
				successor = newSelectedContractSQLiteExecutionOwnerForTest(t, native)
			case *store.PostgresStore:
				successor = newSelectedContractExecutionOwnerForTest(t, native)
			}
			if err := successor.BindSelectedProcess(ctx, process, capability); err != nil {
				t.Fatal(err)
			}
			execution := &liveWinnerExecutionProbe{SelectedContractRuntimeExecutionLifecycle: successor.ports.runtimeExecution,
				entered: make(chan struct{}), allow: make(chan struct{})}
			close(execution.allow)
			successor.ports.runtimeExecution = execution
			request.AgentRuntime.ProcessCapability = capability
			recovered, err := successor.RecoverSelectedForkContexts(ctx, effects.NewRecoveryRequest(time.Now().UTC(), executionposture.MockOnly),
				SelectedForkRecoveryEnvironment{SourceLoader: request.SourceLoader, AgentRuntime: request.AgentRuntime})
			if err != nil || len(recovered) != 1 || recovered[0].RunID != materialized.ForkRunID || recovered[0].Disposition != runfork.SelectedForkRecoveryResume {
				t.Fatalf("successor did not consume exact native recovery: %+v err=%v", recovered, err)
			}
			duplicate := *request.ForkOperation
			duplicate.OperationID = uuid.NewString()
			request.Owner, request.ForkOperation = successor, &duplicate
			result, err := ExecuteSelectedContractRunFork(ctx, request)
			if err != nil || !result.Activation.Activated || result.Materialization.ForkRunID != materialized.ForkRunID || execution.issues.Load() != 1 {
				t.Fatalf("successor replay executed another child: result=%+v issues=%d err=%v", result, execution.issues.Load(), err)
			}
			evidence, err := storetest.ReadSelectedExecutionStorage(ctx, selected, materialized.ForkRunID)
			if err != nil || evidence.Occurrences != 1 {
				t.Fatalf("successor duplicated native execution: %+v err=%v", evidence, err)
			}
		})
	}
}
