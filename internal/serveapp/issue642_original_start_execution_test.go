package serveapp

import (
	"context"
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"
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
			nativeBackend := "sqlite"
			if backend == servedparity.BackendExplicitPostgres {
				nativeBackend = "postgres"
			}
			forkRuntime := captureServedForkRuntimeOptions(t)
			publication := newExactOriginPublicationProbe(t)
			start := newServedBoundaryFixtureLifetime(t, nativeBackend, canonicalrouting.CopyExample(t, canonicalrouting.RootIngress))
			process, rt := start()
			selected := rt.Selected
			deliveries := selected.RuntimeDeps().DeliveryStore.SummarizeRun
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
			waitServedRunDeliveryQuiescence(t, deliveries, started.RunID)
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
			operation := runfork.ForkOperationRequest{
				OperationID: uuid.NewString(), Actor: "original-first-turn-proof", IdempotencyKey: uuid.NewString(),
				TransportHash: "original-first-turn-proof", SourceRunID: started.RunID, AtStart: true,
				TargetBundleHash: rt.BundleHash, ContractSelection: selection,
			}
			request := runforkexecution.SelectedContractExecutionRequest{
				SourceRunID: started.RunID, AtStart: true, ExpectedBundleHash: rt.BundleHash,
				ContractSelection: selection,
				ForkOperation:     &operation,
				SourceLoader: runforkexecution.SourceArtifactSelectedContractSourceLoader{
					RepoRoot: repoRootForTest(), PlatformSpecPath: filepath.Join(repoRootForTest(), defaultPlatformSpecPath), Store: selected.SourceArtifactStore(),
				}, AgentRuntime: *forkRuntime,
			}
			result, err := family.Execute(ctx, request)
			if err != nil || !result.Activation.Activated || result.Activation.SourceFrozen ||
				result.Materialization.ForkPoint != plan.ForkPoint || result.Materialization.ForkRunID == "" {
				t.Fatalf("execute actual original start: %+v error=%v", result, err)
			}
			child := result.Materialization.ForkRunID
			t.Cleanup(func() {
				if t.Failed() {
					var diagnosis json.RawMessage
					requireServedJSONRPCResult(t, rt.Endpoint, "run.diagnose", map[string]any{"run_id": child}, &diagnosis)
					t.Logf("original-start child failure: %s", diagnosis)
				}
			})
			requireServedRunStatus(t, rt.Endpoint, child, "completed")
			waitServedRunDeliveryQuiescence(t, deliveries, child)
			_, childHeader := requireReceiverTargetState(t, rt.ReceiverStateReader, child, ".", child, child)
			if childHeader.CurrentState != "done" {
				t.Fatalf("exact first-turn child did not reach done: %+v", childHeader)
			}
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
			ack, found, err := family.Operations().LoadForkOperationByID(ctx, operation.OperationID)
			if err != nil || !found || ack.Validate() != nil || ack.Status != runfork.ForkOperationActivated || ack.Result == nil ||
				ack.ForkRunID != child || !ack.Request.AtStart || ack.Request.ForkEventID != "" || ack.Request.ResolvedPoint == nil || *ack.Request.ResolvedPoint != plan.ForkPoint {
				t.Fatalf("first-turn operation lost its immutable original-start ACK: %+v error=%v", ack, err)
			}
			childBefore, err := storetest.ReadSelectedForkSourceDomain(ctx, selected.RuntimeDeps().DeliveryStore, child)
			if err != nil {
				t.Fatal(err)
			}
			replay, err := family.Execute(ctx, request)
			if err != nil || !replay.Activation.Activated || replay.Materialization.ForkRunID != ack.Result.ForkRunID ||
				replay.Materialization.ForkPoint != ack.Result.ForkPoint || replay.Materialization.ForkRunStatus != ack.Result.ForkRunStatus ||
				!reflect.DeepEqual(replay.Materialization.DataPins, ack.Result.DataPins) || replay.ExecutedEventCount != ack.Result.ExecutedEventCount ||
				replay.Activation.SourceRunStatus != ack.Result.SourceRunStatus || replay.Activation.SourceFrozen != ack.Result.SourceFrozen {
				t.Fatalf("original-start retry reinterpreted the recorded outcome: result=%+v ACK=%+v error=%v", replay, ack.Result, err)
			}
			tamperedOperation := operation
			tamperedOperation.AllowSourceFreeze = !operation.AllowSourceFreeze
			tamperedRequest := request
			tamperedRequest.ForkOperation = &tamperedOperation
			tamperedRequest.AllowSourceFreeze = tamperedOperation.AllowSourceFreeze
			if _, err := family.Execute(ctx, tamperedRequest); err == nil || !strings.Contains(err.Error(), "original invocation semantics") {
				t.Fatalf("same operation accepted changed invocation semantics or hit the wrong refusal: %v", err)
			}
			ackAfter, found, err := family.Operations().LoadForkOperationByID(ctx, operation.OperationID)
			if err != nil || !found || !reflect.DeepEqual(ackAfter, ack) {
				t.Fatalf("retry or tamper changed the durable original-start operation: %+v error=%v", ackAfter, err)
			}
			childAfter, err := storetest.ReadSelectedForkSourceDomain(ctx, selected.RuntimeDeps().DeliveryStore, child)
			if err != nil || !reflect.DeepEqual(childAfter, childBefore) {
				t.Fatalf("retry or refused tamper changed the completed child: %v", err)
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
			if code := process.stop(); code != 0 {
				t.Fatalf("join original process before reopening: exit=%d\n%s", code, process.outputString())
			}
			_, reopened := start()
			if reopened.Selected == selected || reopened.Runtime == rt.Runtime {
				t.Fatal("reopen retained the original native owner/runtime")
			}
			rt = reopened
			freshContext := servedRuntimeProofAuthorActivityContext(t, rt.Runtime, rt.BundleHash)
			freshFamily, ok := rt.Selected.RunFork()
			if !ok {
				t.Fatal("reopened native process lacks the selected fork family")
			}
			freshBaseline := rt.Runtime.Options.ProcessWorkOwner.ActiveCount()
			persisted, found, err := freshFamily.Operations().LoadForkOperationByID(freshContext, operation.OperationID)
			if err != nil || !found || !reflect.DeepEqual(persisted, ack) {
				t.Fatalf("reopened process changed the immutable original-start ACK: %+v found=%t error=%v", persisted, found, err)
			}
			reopenedRequest := request
			reopenedRequest.AgentRuntime = *forkRuntime
			reopenedRequest.SourceLoader = runforkexecution.SourceArtifactSelectedContractSourceLoader{
				RepoRoot: repoRootForTest(), PlatformSpecPath: filepath.Join(repoRootForTest(), defaultPlatformSpecPath), Store: rt.Selected.SourceArtifactStore(),
			}
			coldReplay, err := freshFamily.Execute(freshContext, reopenedRequest)
			if err != nil || !reflect.DeepEqual(coldReplay, replay) || coldReplay.Materialization.ForkRunID != persisted.ForkRunID {
				t.Fatalf("cold original-start replay did not consume its original ACK: %+v error=%v", coldReplay, err)
			}
			coldTamper := reopenedRequest
			coldTamper.ForkOperation = &tamperedOperation
			coldTamper.AllowSourceFreeze = tamperedOperation.AllowSourceFreeze
			if _, err := freshFamily.Execute(freshContext, coldTamper); err == nil || !strings.Contains(err.Error(), "original invocation semantics") {
				t.Fatalf("reopened process admitted changed operation semantics or hit the wrong refusal: %v", err)
			}
			requireServedRunStatus(t, rt.Endpoint, persisted.ForkRunID, "completed")
			_, restoredHeader := requireReceiverTargetState(t, rt.ReceiverStateReader, persisted.ForkRunID, ".", persisted.ForkRunID, persisted.ForkRunID)
			if !reflect.DeepEqual(restoredHeader, childHeader) {
				t.Fatalf("cold replay changed the exact completed receiver: before=%+v after=%+v", childHeader, restoredHeader)
			}
			coldEvents, err := rt.Selected.Observability().ListOperatorEvents(freshContext, operatorread.OperatorEventListOptions{
				Filter: operatorread.OperatorEventListFilter{RunID: persisted.ForkRunID}, Limit: 10, ExcludeRuntimeLogs: true,
			})
			if err != nil || !reflect.DeepEqual(coldEvents, page) {
				t.Fatalf("cold replay changed exact events, lineage or settled claims: %+v error=%v", coldEvents, err)
			}
			persistedAfter, found, err := freshFamily.Operations().LoadForkOperationByID(freshContext, operation.OperationID)
			if err != nil || !found || !reflect.DeepEqual(persistedAfter, ack) {
				t.Fatalf("cold replay/tamper changed its durable ACK: %+v error=%v", persistedAfter, err)
			}
			coldChild, err := storetest.ReadSelectedForkSourceDomain(freshContext, rt.Selected.RuntimeDeps().DeliveryStore, persisted.ForkRunID)
			if err != nil || !reflect.DeepEqual(coldChild, childBefore) {
				t.Fatalf("cold replay/tamper changed the completed child domain: %v", err)
			}
			coldSource, err := storetest.ReadSelectedForkSourceDomain(freshContext, rt.Selected.RuntimeDeps().DeliveryStore, started.RunID)
			if err != nil || !reflect.DeepEqual(coldSource, before) {
				t.Fatalf("cold replay/tamper changed the original source domain: %v", err)
			}
			if rt.Runtime.Options.ProcessWorkOwner.ActiveCount() != freshBaseline {
				t.Fatal("cold ACK replay/tamper retained new runtime work")
			}
		})
	}
}
