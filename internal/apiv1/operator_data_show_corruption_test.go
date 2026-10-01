package apiv1

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/store"
	"github.com/google/uuid"
)

// Selected-store results cross an interface; the API's own integrity checks
// must still reject a hostile projection without adding another store read.
type dataShowCorruptStore struct {
	DurableDataStore
	arm string
}

func (s dataShowCorruptStore) GetDeclarationImportShape(ctx context.Context, bundle string, ref durabledata.DeclarationRef) (durabledata.ImportShape, error) {
	shape, err := s.DurableDataStore.GetDeclarationImportShape(ctx, bundle, ref)
	if err == nil {
		shape.Fields = nil
	}
	return shape, err
}

func (s dataShowCorruptStore) ListDataVersionSummaries(ctx context.Context, ref durabledata.DeclarationRef, after uint64, limit int) ([]durabledata.VersionSummary, error) {
	items, err := s.DurableDataStore.ListDataVersionSummaries(ctx, ref, after, limit)
	if err == nil && len(items) > 0 {
		items[0].Alias = "v0"
	}
	return items, err
}

func (s dataShowCorruptStore) ResolveDataVersionSummary(ctx context.Context, ref durabledata.DeclarationRef, selector durabledata.VersionSelector) (durabledata.VersionSummary, error) {
	summary, err := s.DurableDataStore.ResolveDataVersionSummary(ctx, ref, selector)
	if err == nil && s.arm == "version" {
		summary.Alias = "v0"
	}
	return summary, err
}

func (s dataShowCorruptStore) ResolveDataVersionPayload(ctx context.Context, ref durabledata.DeclarationRef, selector durabledata.VersionSelector) (durabledata.VersionSummary, durabledata.Version, error) {
	summary, version, err := s.DurableDataStore.ResolveDataVersionPayload(ctx, ref, selector)
	if err == nil {
		version.CanonicalJSONL = []byte("not JSONL")
	}
	return summary, version, err
}

func (s dataShowCorruptStore) ListDataVersionProvenance(ctx context.Context, id durabledata.VersionID, after uint64, limit int) ([]durabledata.Provenance, error) {
	items, err := s.DurableDataStore.ListDataVersionProvenance(ctx, id, after, limit)
	if err == nil && len(items) > 0 {
		items[0].Actor = ""
	}
	return items, err
}

func (s dataShowCorruptStore) ListDataPins(context.Context, durabledata.VersionID, string, int) ([]durabledata.Pin, error) {
	return []durabledata.Pin{{RunID: "invalid"}}, nil
}

func (s dataShowCorruptStore) LoadDataSourceOperation(ctx context.Context, id string) (durabledata.SourceOperationRecord, error) {
	record, err := s.DurableDataStore.LoadDataSourceOperation(ctx, id)
	if err == nil {
		record.Result.Outcome = "invalid"
	}
	return record, err
}

func (s dataShowCorruptStore) LoadDataPruneOperation(ctx context.Context, id string) (durabledata.PruneOperationResult, error) {
	result, err := s.DurableDataStore.LoadDataPruneOperation(ctx, id)
	if err == nil {
		result.Outcome = "invalid"
	}
	return result, err
}

func TestDataShowCorruptReadFamilyAcrossSelectedStores(t *testing.T) {
	forEachDataRunLifecycleStore(t, func(t *testing.T, fixture dataRunLifecycleFixture) {
		ctx := context.Background()
		catalog, ref := dataHTTPProbeCatalog(t)
		if _, err := fixture.primary.EnsureSourceArtifactWithData(ctx, dataProbeSourceArtifact, catalog); err != nil {
			t.Fatal(err)
		}
		first := dataShowSource(t, fixture.primary, dataProbeBundleHash, ref, durabledata.AbsentHead(), "import", "{\"slug\":\"alpha\"}\n")
		second := dataShowSource(t, fixture.primary, dataProbeBundleHash, ref, first.Head.After, "import", "{\"slug\":\"beta\"}\n")
		prune, err := fixture.primary.PruneDataResource(ctx, durabledata.PruneCommand{PruneInvocationID: uuid.NewString(), Actor: "operator", Declaration: ref, VersionID: first.Candidate.VersionID, ExpectedHead: second.Head.After})
		if err != nil {
			t.Fatal(err)
		}
		for _, arm := range []string{"import_shape", "versions", "version", "rows", "row", "export_chunk", "provenance", "pins", "source", "prune"} {
			t.Run(arm, func(t *testing.T) {
				corrupt := dataShowCorruptStore{DurableDataStore: fixture.reconstructed, arm: arm}
				trace := &dataShowTraceStore{DurableDataStore: corrupt}
				handler := testHandler(t, Options{AuthTokens: []string{testToken}, Handlers: OperatorDataHandlers(DataHandlerOptions{Store: trace})})
				params := dataShowResourceParams(arm)
				if arm == "import_shape" {
					params = map[string]any{"view": arm, "bundle_hash": dataProbeBundleHash, "declaration": dataRunDeclaration(ref), "schema_digest": first.SchemaDigest}
				}
				if arm == "source" {
					params = dataShowOperationParams(arm, first.SourceInvocationID, "summary")
				}
				if arm == "prune" {
					params = dataShowOperationParams(arm, prune.PruneInvocationID, "summary")
				}
				status, response, _ := callReadOnlyProbeRPC(t, handler, "data.show", params, "Bearer "+testToken)
				if status != 200 || response.Error == nil || asMap(t, response.Error.Data)["code"] != string(durabledata.CodeIntegrity) {
					t.Fatalf("hostile %s result accepted: %#v", arm, response)
				}
				wantCalls := 1
				if arm == "provenance" || arm == "pins" {
					wantCalls = 2
				}
				if len(trace.calls) != wantCalls {
					t.Fatalf("integrity validation reread the store: %#v", trace.calls)
				}
			})
		}
		// Actual stored corruption is separate from hostile interface-result proof.
		query := "UPDATE resource_versions SET canonical_jsonl = ? WHERE version_id = ?"
		if _, ok := fixture.primary.(*store.PostgresStore); ok {
			query = "UPDATE resource_versions SET canonical_jsonl = $1 WHERE version_id = $2"
		}
		if _, err := fixture.db.ExecContext(ctx, query, []byte("not JSONL"), second.Candidate.VersionID); err != nil {
			t.Fatal(err)
		}
		handler := testHandler(t, Options{AuthTokens: []string{testToken}, Handlers: OperatorDataHandlers(DataHandlerOptions{Store: fixture.reconstructed})})
		_, response, _ := callReadOnlyProbeRPC(t, handler, "data.show", dataShowResourceParams("rows"), "Bearer "+testToken)
		if response.Error == nil || asMap(t, response.Error.Data)["code"] != string(durabledata.CodeIntegrity) {
			t.Fatalf("stored corruption accepted: %#v", response)
		}
	})
}
