package runforkexecution

import (
	"context"
	"reflect"
	"slices"
	"testing"

	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestSelectedOperationReplayRetainsPinsAfterResourceHeadAdvanceBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx, request := ownerlessSelectedOperationFixture(t, backend)
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
			})
			ref, err := durabledata.ParseDeclarationRef(".", "item.received")
			if err != nil {
				t.Fatal(err)
			}
			command := durabledata.SourceCommand{Operation: "import", SourceInvocationID: uuid.NewString(), Actor: "operation-pin-proof",
				BundleHash: loaded.SourceArtifactFact.BundleHash(), Declaration: ref, ExpectedHead: durabledata.AbsentHead(),
				InputFormat: "jsonl", Input: []byte("{}\n")}
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
			command.SourceInvocationID, command.ExpectedHead, command.Input = uuid.NewString(), durabledata.VersionHead(original.Candidate.VersionID), []byte("{}\n{}\n")
			advanced, err := data.ExecuteDataSourceOperation(ctx, command)
			if err != nil || advanced.Candidate.VersionID == "" || advanced.Candidate.VersionID == original.Candidate.VersionID {
				t.Fatalf("resource head did not genuinely advance: %+v err=%v", advanced, err)
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
			if err != nil || pinned.VersionID != original.Candidate.VersionID || pinned.RowCount != 1 {
				t.Fatalf("native child pin changed after head advancement: %+v err=%v", pinned, err)
			}
			before, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, loader.Store)
			if err != nil {
				t.Fatal(err)
			}
			replayed, err := ExecuteSelectedContractRunFork(ctx, request)
			if err != nil || !replayed.Activation.Activated || replayed.Materialization.ForkRunID != materialized.ForkRunID ||
				!slices.Equal(replayed.Materialization.DataPins, materialized.DataPins) {
				t.Fatalf("activated replay changed immutable pin evidence: %+v err=%v", replayed, err)
			}
			after, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, loader.Store)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("activated pin replay mutated durable state: %v", err)
			}
		})
	}
}
