package serveapp

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestIssue642OriginalStartFirstTurnExecutesBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			var selected *selectedStoreOwner
			previous := projectRuntimePersistenceForServe
			projectRuntimePersistenceForServe = func(owner *selectedStoreOwner) serveRuntimePersistence {
				selected = owner
				return previous(owner)
			}
			t.Cleanup(func() { projectRuntimePersistenceForServe = previous })
			publication := newExactOriginPublicationProbe(t)
			rt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, canonicalrouting.CopyExample(t, canonicalrouting.RootIngress))
			var started struct {
				RunID string `json:"run_id"`
			}
			payload := map[string]any{"item_id": "original-first-turn"}
			requireServedJSONRPCResult(t, rt.Endpoint, "run.start", map[string]any{
				"bundle_hash": rt.BundleHash, "event_name": "item.received", "payload": payload,
			}, &started)
			var receipt durabledata.OperationSummary
			requireServedJSONRPCResult(t, rt.Endpoint, "data.show", map[string]any{
				"view": "operation", "detail": "summary", "operation_ref": map[string]any{"kind": "run_creation", "run_id": started.RunID},
			}, &receipt)
			if receipt.RunCreation == nil || receipt.RunCreation.Outcome != "created" || receipt.RunCreation.EventID == "" {
				t.Fatalf("first turn lacks acknowledged native creation: %+v", receipt)
			}
			initial := receipt.RunCreation.EventID
			requireServedRunStatus(t, rt.Endpoint, started.RunID, "completed")
			waitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, started.RunID)
			if err := waitExactOriginPublications(t.Context(), publication, initial); err != nil {
				t.Fatal(err)
			}
			family, ok := selected.RunFork()
			if !ok {
				t.Fatal("start execution requires the actual selected fork family")
			}
			ctx := servedRuntimeProofAuthorActivityContext(t, rt.Runtime, rt.BundleHash)
			plan, err := family.Plan(ctx, runfork.RunForkPlanRequest{SourceRunID: started.RunID, AtStart: true})
			if err != nil || plan.ForkPoint.Kind != runfork.RunForkPointRunStart || plan.ForkPoint.Revision != 1 ||
				plan.ForkPoint.EventID != "" || plan.StartFirstTurn == nil || plan.EventCountAtFork != 0 || len(plan.PendingWork) != 0 {
				t.Fatalf("original start is not exclusive of the creating event/deliveries: %+v error=%v", plan, err)
			}
			first, found := plan.StartFirstTurn.Event()
			if !found || first.ID() != initial || first.RunID() != started.RunID || first.Type() != "item.received" {
				t.Fatalf("start first turn substituted its immutable origin: %+v", plan.StartFirstTurn.Coordinates())
			}
			before, err := storetest.ReadSelectedForkSourceDomain(ctx, selected.RuntimeDeps().DeliveryStore, started.RunID)
			if err != nil {
				t.Fatal(err)
			}
			baseline := rt.Runtime.Options.ProcessWorkOwner.ActiveCount()
			selection := runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeSelectedContracts}
			result, err := family.Execute(ctx, runforkexecution.SelectedContractExecutionRequest{
				SourceRunID: started.RunID, AtStart: true, ExpectedBundleHash: rt.BundleHash,
				ContractSelection: selection,
				ForkOperation: &runfork.ForkOperationRequest{
					OperationID: uuid.NewString(), Actor: "original-first-turn-proof", IdempotencyKey: uuid.NewString(),
					TransportHash: "original-first-turn-proof", SourceRunID: started.RunID, AtStart: true,
					TargetBundleHash: rt.BundleHash, ContractSelection: selection,
				},
				SourceLoader: runforkexecution.SourceArtifactSelectedContractSourceLoader{
					RepoRoot: repoRootForTest(), PlatformSpecPath: filepath.Join(repoRootForTest(), defaultPlatformSpecPath), Store: selected.SourceArtifactStore(),
				}, AgentRuntime: rt.ForkRuntime,
			})
			if err != nil || !result.Activation.Activated || result.Activation.SourceFrozen ||
				result.Materialization.ForkPoint != plan.ForkPoint || result.Materialization.ForkRunID == "" {
				t.Fatalf("execute actual original start: %+v error=%v", result, err)
			}
			child := result.Materialization.ForkRunID
			requireServedRunStatus(t, rt.Endpoint, child, "completed")
			waitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, child)
			requireLifecycleFlowEntity(t, rt, child, "", "done")
			page, err := selected.Observability().ListOperatorEvents(ctx, operatorread.OperatorEventListOptions{
				Filter: operatorread.OperatorEventListFilter{RunID: child}, Limit: 10, ExcludeRuntimeLogs: true,
			})
			if err != nil || page.NextCursor != "" || len(page.Events) != 2 {
				t.Fatalf("first turn invented, copied or duplicated an event: %+v error=%v", page, err)
			}
			seen := make(map[string]bool)
			for _, event := range page.Events {
				if seen[event.EventName] || (event.EventName != "item.received" && event.EventName != "item.processed") ||
					event.EventID == initial || event.RunID != child || !reflect.DeepEqual(event.Payload, payload) ||
					len(event.Deliveries) != 1 || event.Deliveries[0].Status != "delivered" || event.Deliveries[0].ClaimVersion != 1 ||
					event.Deliveries[0].Target.EntityID != child || len(event.DeadLetters) != 0 {
					t.Fatalf("first turn failed exact fresh child publication/receiver: %+v", event)
				}
				seen[event.EventName] = true
			}
			childStart, err := family.Plan(ctx, runfork.RunForkPlanRequest{SourceRunID: child, AtStart: true})
			if err != nil || childStart.EventCountAtFork != 0 || len(childStart.PendingWork) != 0 || childStart.StartFirstTurn != nil {
				t.Fatalf("child start copied source historical execution: %+v error=%v", childStart, err)
			}
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			defer cancel()
			if err := family.RetireSelectedContexts(cleanup); err != nil || rt.Runtime.Options.ProcessWorkOwner.ActiveCount() != baseline {
				t.Fatalf("first-turn execution did not join its own retained work: %v", err)
			}
			after, err := storetest.ReadSelectedForkSourceDomain(ctx, selected.RuntimeDeps().DeliveryStore, started.RunID)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, after) {
				t.Fatal("selected first turn changed original source execution/history")
			}
		})
	}
}
