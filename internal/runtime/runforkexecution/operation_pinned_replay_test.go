package runforkexecution

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestSelectedOperationReplayRetainsPinsAfterResourceHeadAdvanceBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) { proveSelectedPinnedOperationReplay(t, backend, 1, true) })
	}
}

func TestSelectedOperationFreshPinFeedsSettleBeforeExactReplayBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) { proveSelectedPinnedOperationReplay(t, backend, 1, false) })
	}
}

func TestSelectedOperationEmptyPinnedFeedActivatesAndReplaysBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) { proveSelectedPinnedOperationReplay(t, backend, 0, true) })
	}
}

func proveSelectedPinnedOperationReplay(t *testing.T, backend string, rows int, recover bool) {
	t.Helper()
	ctx, request := ownerlessSelectedOperationFixtureFromSource(t, backend, canonicalrouting.CopySelectedOperationPinnedResource(t), "fixture.seed.requested")
	loader := request.SourceLoader.(SourceArtifactSelectedContractSourceLoader)
	loaded, err := loader.InspectRunForkSelectedContractSourceForRequest(ctx, SelectedContractSourceLoadRequest{
		SourceRunID: request.SourceRunID, Selection: request.ContractSelection,
	})
	if err != nil {
		t.Fatal(err)
	}
	bundle, found := semanticview.Bundle(loaded.Source)
	if !found || bundle == nil {
		t.Fatal("pin proof requires the actual admitted source bundle")
	}
	storetest.RequireBundleDataCatalog(t, ctx, loader.Store.(storetest.DurableDataCatalogStore), bundle)
	data := loader.Store.(interface {
		ExecuteDataSourceOperation(context.Context, durabledata.SourceCommand) (durabledata.SourceOperationResult, error)
		LoadPinnedSource(context.Context, string, string, durabledata.DeclarationRef) (durabledata.PinnedSource, error)
		LoadPinnedRowsRange(context.Context, string, string, durabledata.PinnedSource, int, int) ([]any, error)
	})
	ref, err := durabledata.ParseDeclarationRef(".", "root.ready")
	if err != nil {
		t.Fatal(err)
	}
	command := durabledata.SourceCommand{Operation: "import", SourceInvocationID: uuid.NewString(), Actor: "operation-pin-proof",
		BundleHash: loaded.SourceArtifactFact.BundleHash(), Declaration: ref, ExpectedHead: durabledata.AbsentHead(),
		InputFormat: "jsonl", Input: bytes.Repeat([]byte("{\"account_id\":\"original\"}\n"), rows)}
	original, err := data.ExecuteDataSourceOperation(ctx, command)
	if err != nil || original.Candidate.VersionID == "" {
		t.Fatalf("import original immutable version: %+v err=%v", original, err)
	}
	pins := []durabledata.ExplicitPin{{Declaration: ref, VersionID: original.Candidate.VersionID}}
	request.ForkOperation.DataPinOverrides, request.DataPinOverrides = pins, slices.Clone(pins)
	var materialized runfork.RunForkMaterialization
	if recover {
		materialized = abandonSelectedOperationPreparation(t, ctx, request)
		if len(materialized.DataPins) != 1 || materialized.DataPins[0].VersionID != original.Candidate.VersionID || materialized.DataPins[0].Declaration != ref {
			t.Fatalf("pin proof did not materialize its exact nonempty selection: %+v", materialized.DataPins)
		}
	}
	if rows > 0 {
		command.SourceInvocationID, command.ExpectedHead, command.Input = uuid.NewString(), durabledata.VersionHead(original.Candidate.VersionID), bytes.Repeat([]byte("{\"account_id\":\"new-head\"}\n"), rows+1)
		advanced, err := data.ExecuteDataSourceOperation(ctx, command)
		if err != nil || advanced.Candidate.VersionID == "" || advanced.Candidate.VersionID == original.Candidate.VersionID {
			t.Fatalf("resource head did not genuinely advance: %+v err=%v", advanced, err)
		}
	}
	if recover {
		duplicate := *request.ForkOperation
		duplicate.OperationID = uuid.NewString()
		request.ForkOperation = &duplicate
	}
	result, err := ExecuteSelectedContractRunFork(ctx, request)
	if !recover {
		materialized = result.Materialization
		if len(materialized.DataPins) != 1 || materialized.DataPins[0].VersionID != original.Candidate.VersionID || materialized.DataPins[0].Declaration != ref {
			t.Fatalf("fresh execution did not retain its exact explicit pin: %+v err=%v", materialized.DataPins, err)
		}
	}
	if err != nil || !result.Activation.Activated || result.Materialization.ForkRunID != materialized.ForkRunID ||
		!slices.Equal(result.Materialization.DataPins, materialized.DataPins) {
		t.Fatalf("recovered operation adopted the new resource head: %+v err=%v", result, err)
	}
	pinned, err := data.LoadPinnedSource(ctx, materialized.ForkRunID, command.BundleHash, ref)
	if err != nil || pinned.VersionID != original.Candidate.VersionID || pinned.RowCount != rows {
		t.Fatalf("native child pin changed after head advancement: %+v err=%v", pinned, err)
	}
	values, err := data.LoadPinnedRowsRange(ctx, materialized.ForkRunID, command.BundleHash, pinned, 0, rows)
	want := make([]any, rows)
	for i := range want {
		want[i] = map[string]any{"account_id": "original"}
	}
	if err != nil || !slices.EqualFunc(values, want, reflect.DeepEqual) {
		t.Fatalf("native child pin lost original immutable contents: %+v err=%v", values, err)
	}
	feeds := loader.Store.(interface {
		FanOutRunSummary(context.Context, string, time.Time) (fanoutobligation.RunSummary, error)
	})
	for {
		summary, err := feeds.FanOutRunSummary(ctx, materialized.ForkRunID, time.Now().UTC())
		pending, pendingErr := storetest.ReadServedIncompletePipelineHandoffCount(ctx, loader.Store, materialized.ForkRunID)
		if err != nil || pendingErr != nil || summary.Intents != 1 || summary.Cardinality != rows {
			t.Fatalf("read actual pin-feed settlement: %+v pending=%d err=%v/%v", summary, pending, err, pendingErr)
		}
		request.Owner.ports.contexts.mu.Lock()
		retained := len(request.Owner.ports.contexts.entries)
		request.Owner.ports.contexts.mu.Unlock()
		run, runErr := storetest.ReadRuntimeLogRunSnapshot(ctx, loader.Store, materialized.ForkRunID)
		if runErr != nil || run.Failure != nil || retained == 0 && (summary.Owed != 0 || summary.Unsettled != 0 || pending != 0) {
			if run.Failure != nil {
				t.Logf("pin-feed retained failure: %+v", *run.Failure)
			}
			diagnostic, diagnosticErr := storetest.ReadSelectedForkLifecycleDiagnosticStorage(ctx, loader.Store, materialized.ForkRunID)
			for _, log := range diagnostic.Logs {
				t.Logf("pin-feed serving diagnostic: %s", log.Payload)
			}
			t.Fatalf("pin-feed serving stopped before exact settlement: run=%+v summary=%+v pending=%d retained=%d err=%v/%v", run, summary, pending, retained, runErr, diagnosticErr)
		}
		if summary.Owed == 0 && summary.Unsettled == 0 && pending == 0 {
			if summary.Committed != rows || summary.Settled != rows || summary.SemanticRejected != 0 || summary.Canceled != 0 {
				t.Fatalf("pin feed did not deliver its exact row set: %+v", summary)
			}
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("actual pin-feed work did not settle before replay snapshot: %+v pending=%d retained=%d err=%v", summary, pending, retained, ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
	outcomes, err := storetest.ReadFanOutPublishedOutcomeStorage(ctx, loader.Store, materialized.ForkRunID)
	if err != nil || len(outcomes) != rows {
		t.Fatalf("exact pin-feed publication count: outcomes=%+v err=%v", outcomes, err)
	}
	receiver, err := identity.AdmitExecutableNodeDeclaration(semanticview.RootExecutionFlowID(loaded.Source), "root-collector")
	if err != nil {
		t.Fatal(err)
	}
	for ordinal, outcome := range outcomes {
		var payload any
		if err := json.Unmarshal(outcome.Payload, &payload); err != nil || outcome.Ordinal != ordinal || outcome.Kind != "committed" || !reflect.DeepEqual(payload, want[ordinal]) {
			t.Fatalf("actual feed publication lost its immutable row: outcome=%+v payload=%+v err=%v", outcome, payload, err)
		}
		deliveries, err := storetest.ReadEventDeliveryDiagnosticRows(ctx, loader.Store, outcome.EventID)
		if err != nil || len(deliveries) != 1 || deliveries[0].SubscriberType != "node" || deliveries[0].SubscriberID != receiver.Key() ||
			deliveries[0].Status != "delivered" || deliveries[0].FlowInstance != materialized.ForkRunID {
			t.Fatalf("actual pinned row did not settle through its exact receiver: deliveries=%+v err=%v", deliveries, err)
		}
	}
	request.Owner.ports.contexts.mu.Lock()
	var runtime *selectedContractAgentRuntime
	for _, entry := range request.Owner.ports.contexts.entries {
		if entry.binding.ForkRunID == materialized.ForkRunID && entry.retained != nil {
			runtime = entry.retained.retainedRuntime
		}
	}
	request.Owner.ports.contexts.mu.Unlock()
	if runtime == nil {
		t.Fatal("exact root receiver retired before replay without terminal business work")
	}
	if err := runtime.WaitForQuiescence(ctx, runtime.bus); err != nil {
		t.Fatalf("join actual receiver/publication work before the replay baseline: %v", err)
	}
	before, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, loader.Store)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := ExecuteSelectedContractRunFork(ctx, request)
	activatedPins := slices.Clone(materialized.DataPins)
	for i := range activatedPins {
		activatedPins[i].RunState = runfork.RunForkActivatedStatus
	}
	if err != nil || !replayed.Activation.Activated || replayed.Materialization.ForkRunID != materialized.ForkRunID ||
		!slices.Equal(replayed.Materialization.DataPins, activatedPins) {
		t.Fatalf("activated replay changed immutable pin evidence: %+v err=%v", replayed, err)
	}
	after, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, loader.Store)
	if err != nil || !reflect.DeepEqual(before, after) {
		for table, previous := range before {
			if !reflect.DeepEqual(previous, after[table]) {
				t.Logf("replay table delta %s: before=%+v after=%+v", table, previous, after[table])
			}
		}
		t.Fatalf("activated pin replay mutated durable state: %v", err)
	}
	if err := request.Owner.RetireSelectedContexts(ctx); err != nil {
		t.Fatalf("join the root receiver after exact feed settlement and replay: %v", err)
	}
	request.Owner.ports.contexts.mu.Lock()
	remaining := len(request.Owner.ports.contexts.entries)
	request.Owner.ports.contexts.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("settled pin feed leaked %d retained contexts after joined retirement", remaining)
	}
}
