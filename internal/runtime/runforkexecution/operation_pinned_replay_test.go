package runforkexecution

import (
	"bytes"
	"context"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestSelectedOperationReplayRetainsPinsAfterResourceHeadAdvanceBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) { proveSelectedPinnedOperationReplay(t, backend, 1) })
	}
}

func TestSelectedOperationEmptyPinnedFeedActivatesAndReplaysBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) { proveSelectedPinnedOperationReplay(t, backend, 0) })
	}
}

func proveSelectedPinnedOperationReplay(t *testing.T, backend string, rows int) {
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
	materialized := abandonSelectedOperationPreparation(t, ctx, request)
	if len(materialized.DataPins) != 1 || materialized.DataPins[0].VersionID != original.Candidate.VersionID || materialized.DataPins[0].Declaration != ref {
		t.Fatalf("pin proof did not materialize its exact nonempty selection: %+v", materialized.DataPins)
	}
	if rows > 0 {
		command.SourceInvocationID, command.ExpectedHead, command.Input = uuid.NewString(), durabledata.VersionHead(original.Candidate.VersionID), bytes.Repeat([]byte("{\"account_id\":\"new-head\"}\n"), rows+1)
		advanced, err := data.ExecuteDataSourceOperation(ctx, command)
		if err != nil || advanced.Candidate.VersionID == "" || advanced.Candidate.VersionID == original.Candidate.VersionID {
			t.Fatalf("resource head did not genuinely advance: %+v err=%v", advanced, err)
		}
	}
	duplicate := *request.ForkOperation
	duplicate.OperationID = uuid.NewString()
	request.ForkOperation = &duplicate
	result, err := ExecuteSelectedContractRunFork(ctx, request)
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
	if err := request.Owner.RetireSelectedContexts(ctx); err != nil {
		t.Fatalf("join the root receiver after exact feed settlement: %v", err)
	}
	request.Owner.ports.contexts.mu.Lock()
	remaining := len(request.Owner.ports.contexts.entries)
	request.Owner.ports.contexts.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("settled pin feed leaked %d retained contexts after joined retirement", remaining)
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
		t.Fatalf("activated pin replay mutated durable state: %v", err)
	}
}
