package store_test

import (
	"bytes"
	"context"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestDurableDataEmptyHeadAdvancePreservesReceiptsAndPinsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var selected durableDataSelectedStore
			if backend == "postgres" {
				selected = storetest.StartPostgresRuntimeStore(t)
			} else {
				selected = storetest.StartSQLiteRuntimeStore(t)
			}
			ctx := context.Background()
			catalog, ref := durableDataTestCatalog(t)
			if err := registerDurableDataTestCatalog(ctx, selected, catalog); err != nil {
				t.Fatal(err)
			}
			head := durabledata.AbsentHead()
			var commands []durabledata.SourceCommand
			var results []durabledata.SourceOperationResult
			var originalAccess []durabledata.ResourceAccessItem
			var originalPins []durabledata.Pin
			accessStore := selected.(durabledata.ResourceAccessStore)
			pinnedStore := selected.(interface {
				LoadPinnedSource(context.Context, string, string, durabledata.DeclarationRef) (durabledata.PinnedSource, error)
				LoadPinnedRowsRange(context.Context, string, string, durabledata.PinnedSource, int, int) ([]any, error)
			})
			sourceRun, pinnedRun := uuid.NewString(), uuid.NewString()
			for index, test := range []struct {
				operation string
				input     string
				revision  uint64
				changed   bool
				added     uint64
				removed   uint64
			}{
				{"import", "", 1, true, 0, 0},
				{"check", "", 1, false, 0, 0},
				{"import", "", 1, false, 0, 0},
				{"import", "{\"slug\":\"alpha\",\"score\":1}\n", 2, true, 1, 0},
				{"import", "", 3, true, 0, 1},
				{"import", "{\"slug\":\"alpha\",\"score\":1}\n", 4, true, 1, 0},
			} {
				command := durabledata.SourceCommand{
					Operation: test.operation, SourceInvocationID: uuid.NewString(), Actor: "operator", BundleHash: testDataBundle,
					Declaration: ref, ExpectedHead: head, InputFormat: "jsonl", Input: []byte(test.input),
				}
				result, err := selected.ExecuteDataSourceOperation(ctx, command)
				if err != nil || result.Outcome != "accepted" || result.Head.Changed != test.changed || result.Head.Revision != test.revision ||
					!result.Head.Before.Equal(head) || result.Delta.Summary == nil || result.Delta.Summary.Added != test.added ||
					result.Delta.Summary.Removed != test.removed || result.Delta.Summary.Changed != 0 {
					t.Fatalf("step %d (%s %q): %#v, %v", index, test.operation, test.input, result, err)
				}
				if index > 0 && test.input == "" && result.Candidate.VersionID != results[0].Candidate.VersionID {
					t.Fatalf("step %d changed immutable empty version identity", index)
				}
				head = result.Head.After
				commands, results = append(commands, command), append(results, result)
				if index == 0 {
					for _, id := range []string{sourceRun, pinnedRun} {
						storetest.RequireRun(t, ctx, selected, storetest.RunFixture{
							RunID: id, BundleHash: testDataBundle, Origin: storetest.ScenarioSetupOrigin(),
						})
					}
					pins, err := storetest.MaterializeDataForkPins(ctx, selected, sourceRun, pinnedRun, testDataBundle,
						[]durabledata.ExplicitPin{{Declaration: ref, VersionID: result.Candidate.VersionID}}, false)
					if err != nil || len(pins) != 1 || pins[0].VersionID != result.Candidate.VersionID {
						t.Fatalf("original zero-row pin: %#v, %v", pins, err)
					}
					originalPins, err = selected.ListDataPins(ctx, result.Candidate.VersionID, "", 10)
					if err != nil || len(originalPins) != 1 {
						t.Fatalf("original pins: %#v, %v", originalPins, err)
					}
					originalAccess, err = accessStore.LoadRunResourceAccess(ctx, pinnedRun, []durabledata.DeclarationRef{ref})
					if err != nil || len(originalAccess) != 1 || originalAccess[0].RowCount != 0 || originalAccess[0].Content == nil || len(originalAccess[0].Content) != 0 {
						t.Fatalf("original empty resource access: %#v, %v", originalAccess, err)
					}
				}
			}
			for index, command := range commands {
				record, err := selected.LoadDataSourceOperation(ctx, command.SourceInvocationID)
				if err != nil || !reflect.DeepEqual(record.Result, results[index]) {
					t.Fatalf("receipt %d: %#v, %v", index, record, err)
				}
				if index > 0 && index < 4 && record.Evaluation.Base.CanonicalJSONL == nil {
					t.Fatalf("receipt %d lost present-empty historical base", index)
				}
				if index == 0 && (record.Evaluation.Base.State != "absent" || record.Evaluation.Base.CanonicalJSONL != nil) {
					t.Fatal("absent historical base acquired version content")
				}
				replay, err := selected.ExecuteDataSourceOperation(ctx, command)
				if err != nil || !reflect.DeepEqual(replay, results[index]) {
					t.Fatalf("replay %d: %#v, %v", index, replay, err)
				}
			}
			snapshot, err := selected.ShowDataResource(ctx, testDataBundle, ref)
			if err != nil || len(snapshot.Versions) != 2 || snapshot.Head.Revision != 4 || !snapshot.Head.After.Equal(head) {
				t.Fatalf("advanced head: %#v, %v", snapshot, err)
			}
			for index, content := range [][]byte{[]byte{}, []byte("{\"score\":1,\"slug\":\"alpha\"}\n")} {
				version := snapshot.Versions[index]
				_, payload, err := selected.ResolveDataVersionPayload(ctx, ref, durabledata.VersionSelector{Kind: "version", VersionID: version.VersionID})
				if err != nil || payload.CanonicalJSONL == nil || version.CanonicalJSONL == nil ||
					!bytes.Equal(payload.CanonicalJSONL, content) || !bytes.Equal(version.CanonicalJSONL, content) ||
					!reflect.DeepEqual(payload.Manifest, version.Manifest) {
					t.Fatalf("immutable version %d: %#v, %v", index, payload, err)
				}
			}
			pins, err := selected.ListDataPins(ctx, results[0].Candidate.VersionID, "", 10)
			if err != nil || !reflect.DeepEqual(pins, originalPins) {
				t.Fatalf("head advance mutated original pin: %#v, %v", pins, err)
			}
			access, err := accessStore.LoadRunResourceAccess(ctx, pinnedRun, []durabledata.DeclarationRef{ref})
			if err != nil || !reflect.DeepEqual(access, originalAccess) {
				t.Fatalf("head advance mutated pinned content: %#v, %v", access, err)
			}
			pinned, err := pinnedStore.LoadPinnedSource(ctx, pinnedRun, testDataBundle, ref)
			if err != nil || pinned.VersionID != results[0].Candidate.VersionID || pinned.RowCount != 0 {
				t.Fatalf("head advance changed pinned source: %#v, %v", pinned, err)
			}
			rows, err := pinnedStore.LoadPinnedRowsRange(ctx, pinnedRun, testDataBundle, pinned, 0, 0)
			if err != nil || rows == nil || len(rows) != 0 {
				t.Fatalf("original zero-row pin readback: %#v, %v", rows, err)
			}
			history, err := selected.ListDataHeadHistory(ctx, ref, 0, 10)
			if err != nil || len(history) != 4 {
				t.Fatalf("head history: %#v, %v", history, err)
			}
			for index, result := range []durabledata.SourceOperationResult{results[0], results[3], results[4], results[5]} {
				if history[index].Revision != result.Head.Revision || !history[index].Before.Equal(result.Head.Before) || !history[index].After.Equal(result.Head.After) {
					t.Fatalf("head history %d: %#v, want %#v", index, history[index], result.Head)
				}
			}
		})
	}
}
