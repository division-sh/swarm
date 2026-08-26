package store_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/bundlecatalog"
	"github.com/division-sh/swarm/internal/durabledata"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

const (
	testDataBundle = "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	testDataEvent  = "score.available"
)

type durableDataSelectedStore interface {
	runtimerunlifecycle.OperationOwner
	runtimerunlifecycle.CandidateStore
	UpsertBundleCatalogWithData(context.Context, bundlecatalog.Upsert, durabledata.Catalog) (bundlecatalog.UpsertResult, error)
	ExecuteDataSourceOperation(context.Context, durabledata.SourceCommand) (durabledata.SourceOperationResult, error)
	PruneDataResource(context.Context, durabledata.PruneCommand) (durabledata.PruneOperationResult, error)
	ShowDataResource(context.Context, string, durabledata.DeclarationRef) (durabledata.ResourceSnapshot, error)
	LoadDataSourceOperation(context.Context, string) (durabledata.SourceOperationRecord, error)
	LoadDataPruneOperation(context.Context, string) (durabledata.PruneOperationResult, error)
	LoadDataPruneOperationPins(context.Context, string) ([]durabledata.Pin, error)
}

func forEachDurableDataStore(t *testing.T, run func(*testing.T, durableDataSelectedStore, *sql.DB)) {
	t.Helper()
	t.Run("sqlite", func(t *testing.T) {
		selected := storetest.StartSQLiteRuntimeStore(t)
		run(t, selected, storetest.Database(selected))
	})
	t.Run("postgres", func(t *testing.T) {
		_, db, _ := testutil.StartPostgres(t)
		selected := storetest.AdmitPostgresRuntimeStore(t, db)
		run(t, selected, db)
	})
}

func requireEmptySourceDefectsPage(t testing.TB, result durabledata.SourceOperationResult) {
	t.Helper()
	if err := result.Defects.Validate(); err != nil || result.Defects.Items == nil || result.Defects.ItemCount != 0 ||
		result.Defects.EncodedItemsBytes != 2 || result.Defects.Continuation != durabledata.EndContinuation() {
		t.Fatalf("empty source defects page = %#v, validation error %v", result.Defects, err)
	}
	raw, err := json.Marshal(result)
	if err != nil || !strings.Contains(string(raw), `"defects":{"items":[],"item_count":0,"encoded_items_bytes":2,"continuation":{"state":"end"}}`) {
		t.Fatalf("empty source defects wire result = %s, %v", raw, err)
	}
}

func TestDurableDataSelectedStoreLifecycleReplayAndPrune(t *testing.T) {
	forEachDurableDataStore(t, func(t *testing.T, primary durableDataSelectedStore, _ *sql.DB) {
		ctx := context.Background()
		catalog, ref := durableDataTestCatalog(t)
		if err := registerDurableDataTestCatalog(ctx, primary, catalog); err != nil {
			t.Fatalf("register durable data test catalog: %v", err)
		}

		firstID := uuid.NewString()
		first, err := primary.ExecuteDataSourceOperation(ctx, durabledata.SourceCommand{
			Operation: "import", SourceInvocationID: firstID, Actor: "operator", BundleHash: testDataBundle,
			Declaration: ref, ExpectedHead: durabledata.AbsentHead(), InputFormat: "jsonl", Input: []byte("{\"slug\":\"beta\",\"score\":2}\n{\"slug\":\"alpha\",\"score\":1}\n"),
		})
		if err != nil || first.Outcome != "accepted" || !first.Head.Changed {
			t.Fatalf("first import = %#v, %v", first, err)
		}
		if first.Candidate.State != "version" || first.Candidate.VersionID == "" || first.Candidate.Alias != "v1" {
			t.Fatalf("first candidate = %#v", first.Candidate)
		}
		requireEmptySourceDefectsPage(t, first)

		replay, err := primary.ExecuteDataSourceOperation(ctx, durabledata.SourceCommand{
			Operation: "import", SourceInvocationID: firstID, Actor: "operator", BundleHash: testDataBundle,
			Declaration: ref, ExpectedHead: durabledata.AbsentHead(), InputFormat: "jsonl", Input: []byte("{\"slug\":\"beta\",\"score\":2}\n{\"slug\":\"alpha\",\"score\":1}\n"),
		})
		if err != nil || replay.Candidate.VersionID != first.Candidate.VersionID || replay.CompletedAt != first.CompletedAt {
			t.Fatalf("reconstructed exact replay = %#v, %v; want %#v", replay, err, first)
		}
		requireEmptySourceDefectsPage(t, replay)
		if _, err := primary.ExecuteDataSourceOperation(ctx, durabledata.SourceCommand{
			Operation: "check", SourceInvocationID: firstID, Actor: "operator", BundleHash: testDataBundle,
			Declaration: ref, ExpectedHead: first.Head.After, InputFormat: "jsonl", Input: []byte("{\"slug\":\"different\",\"score\":3}\n"),
		}); err == nil {
			t.Fatal("conflicting source_invocation_id reuse succeeded")
		}

		canonicalReorder, err := primary.ExecuteDataSourceOperation(ctx, durabledata.SourceCommand{
			Operation: "check", SourceInvocationID: uuid.NewString(), Actor: "operator", BundleHash: testDataBundle,
			Declaration: ref, ExpectedHead: first.Head.After, InputFormat: "jsonl", Input: []byte("{\"score\":1,\"slug\":\"alpha\"}\n{\"score\":2,\"slug\":\"beta\"}\n"),
		})
		if err != nil || canonicalReorder.Outcome != "accepted" || canonicalReorder.Candidate.State != "candidate" ||
			canonicalReorder.Candidate.Alias != "" || canonicalReorder.Candidate.VersionID != first.Candidate.VersionID {
			t.Fatalf("canonical reorder = %#v, %v", canonicalReorder, err)
		}

		rejected, err := primary.ExecuteDataSourceOperation(ctx, durabledata.SourceCommand{
			Operation: "import", SourceInvocationID: uuid.NewString(), Actor: "operator", BundleHash: testDataBundle,
			Declaration: ref, ExpectedHead: first.Head.After, InputFormat: "jsonl", Input: []byte("{}\n{\"slug\":\"wrong\",\"score\":\"wrong\"}\n{\"slug\":\"alpha\",\"score\":4}\n{\"slug\":\"alpha\",\"score\":5}\n"),
		})
		if err != nil || rejected.Outcome != "validation_rejected" || rejected.Head.Changed || len(rejected.Defects.Items) != 3 {
			t.Fatalf("whole-input rejection = %#v, %v", rejected, err)
		}

		second, err := primary.ExecuteDataSourceOperation(ctx, durabledata.SourceCommand{
			Operation: "import", SourceInvocationID: uuid.NewString(), Actor: "operator", BundleHash: testDataBundle,
			Declaration: ref, ExpectedHead: first.Head.After, InputFormat: "jsonl", Input: []byte("{\"slug\":\"alpha\",\"score\":9}\n"),
		})
		if err != nil || second.Outcome != "accepted" || second.Candidate.State != "version" || second.Candidate.Alias != "v2" {
			t.Fatalf("second import = %#v, %v", second, err)
		}

		pruneID := uuid.NewString()
		pruned, err := primary.PruneDataResource(ctx, durabledata.PruneCommand{
			PruneInvocationID: pruneID, Actor: "operator", Declaration: ref,
			VersionID: first.Candidate.VersionID, ExpectedHead: second.Head.After,
		})
		if err != nil || pruned.Outcome != "pruned" || pruned.PayloadAfter != "pruned" {
			t.Fatalf("prune = %#v, %v", pruned, err)
		}
		pruneReplay, err := primary.PruneDataResource(ctx, durabledata.PruneCommand{
			PruneInvocationID: pruneID, Actor: "operator", Declaration: ref,
			VersionID: first.Candidate.VersionID, ExpectedHead: second.Head.After,
		})
		if err != nil || pruneReplay.CompletedAt != pruned.CompletedAt || pruneReplay.Outcome != "pruned" {
			t.Fatalf("prune replay = %#v, %v; want %#v", pruneReplay, err, pruned)
		}

		snapshot, err := primary.ShowDataResource(ctx, testDataBundle, ref)
		if err != nil || len(snapshot.Versions) != 2 || snapshot.Versions[0].PrunedAt == nil || snapshot.Versions[1].PrunedAt != nil {
			t.Fatalf("snapshot = %#v, %v", snapshot, err)
		}
	})
}

func TestDurableDataSelectedStoreKeylessPreservesMultiplicityOrderAndSchemaMode(t *testing.T) {
	forEachDurableDataStore(t, func(t *testing.T, selected durableDataSelectedStore, _ *sql.DB) {
		ctx := context.Background()
		catalog, ref := durableDataKeylessTestCatalog(t)
		if err := registerDurableDataTestCatalog(ctx, selected, catalog); err != nil {
			t.Fatal(err)
		}
		input := []byte("{\"score\":1,\"label\":\"same\"}\n{\"label\":\"other\",\"score\":2}\n{\"label\":\"same\",\"score\":1}\n")
		first, err := selected.ExecuteDataSourceOperation(ctx, durabledata.SourceCommand{
			Operation: "import", SourceInvocationID: uuid.NewString(), Actor: "operator", BundleHash: testDataBundle,
			Declaration: ref, ExpectedHead: durabledata.AbsentHead(), InputFormat: "jsonl", Input: input,
		})
		if err != nil || first.Outcome != "accepted" || first.Candidate.Manifest == nil || first.Candidate.Manifest.RowCount != 3 {
			t.Fatalf("first keyless import = %#v, %v", first, err)
		}
		snapshot, err := selected.ShowDataResource(ctx, testDataBundle, ref)
		if err != nil || len(snapshot.Versions) != 1 || snapshot.Versions[0].BusinessKey != "" {
			t.Fatalf("keyless snapshot = %#v, %v", snapshot, err)
		}
		if got, want := string(snapshot.Versions[0].CanonicalJSONL), "{\"label\":\"same\",\"score\":1}\n{\"label\":\"other\",\"score\":2}\n{\"label\":\"same\",\"score\":1}\n"; got != want {
			t.Fatalf("keyless canonical rows = %q, want %q", got, want)
		}

		semanticReplay, err := selected.ExecuteDataSourceOperation(ctx, durabledata.SourceCommand{
			Operation: "check", SourceInvocationID: uuid.NewString(), Actor: "operator", BundleHash: testDataBundle,
			Declaration: ref, ExpectedHead: first.Head.After, InputFormat: "jsonl",
			Input: []byte("{\"label\":\"same\",\"score\":1}\n{\"score\":2,\"label\":\"other\"}\n{\"score\":1,\"label\":\"same\"}\n"),
		})
		if err != nil || semanticReplay.Candidate.VersionID != first.Candidate.VersionID {
			t.Fatalf("keyless semantic replay = %#v, %v", semanticReplay, err)
		}

		reordered, err := selected.ExecuteDataSourceOperation(ctx, durabledata.SourceCommand{
			Operation: "import", SourceInvocationID: uuid.NewString(), Actor: "operator", BundleHash: testDataBundle,
			Declaration: ref, ExpectedHead: first.Head.After, InputFormat: "jsonl",
			Input: []byte("{\"label\":\"same\",\"score\":1}\n{\"label\":\"same\",\"score\":1}\n{\"label\":\"other\",\"score\":2}\n"),
		})
		if err != nil || reordered.Outcome != "accepted" || reordered.Candidate.VersionID == first.Candidate.VersionID ||
			reordered.Delta.Summary == nil || !reordered.Delta.Summary.OrderChanged || reordered.Delta.Summary.Added != 0 || reordered.Delta.Summary.Removed != 0 {
			t.Fatalf("keyless reordered import = %#v, %v", reordered, err)
		}
	})
}

func TestDurableDataConcurrentCASHasOneWinner(t *testing.T) {
	forEachDurableDataStore(t, func(t *testing.T, selected durableDataSelectedStore, _ *sql.DB) {
		ctx := context.Background()
		catalog, ref := durableDataTestCatalog(t)
		if err := registerDurableDataTestCatalog(ctx, selected, catalog); err != nil {
			t.Fatal(err)
		}

		const contenders = 12
		results := make(chan string, contenders)
		errs := make(chan error, contenders)
		var wg sync.WaitGroup
		for index := 0; index < contenders; index++ {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				result, err := selected.ExecuteDataSourceOperation(ctx, durabledata.SourceCommand{
					Operation: "import", SourceInvocationID: uuid.NewString(), Actor: fmt.Sprintf("actor-%d", index), BundleHash: testDataBundle,
					Declaration: ref, ExpectedHead: durabledata.AbsentHead(), InputFormat: "jsonl", Input: []byte(fmt.Sprintf("{\"slug\":\"row-%02d\",\"score\":%d}\n", index, index)),
				})
				if err != nil {
					errs <- err
					return
				}
				results <- result.Outcome
			}(index)
		}
		wg.Wait()
		close(results)
		close(errs)
		for err := range errs {
			t.Fatalf("concurrent import: %v", err)
		}
		counts := map[string]int{}
		for outcome := range results {
			counts[outcome]++
		}
		if counts["accepted"] != 1 || counts["head_conflict"] != contenders-1 {
			t.Fatalf("concurrent outcomes = %#v", counts)
		}
	})
}

func TestDurableDataHeadConflictEvidenceIgnoresHistoricalRetentionState(t *testing.T) {
	forEachDurableDataStore(t, func(t *testing.T, selected durableDataSelectedStore, _ *sql.DB) {
		ctx := context.Background()
		catalog, ref := durableDataTestCatalog(t)
		if err := registerDurableDataTestCatalog(ctx, selected, catalog); err != nil {
			t.Fatal(err)
		}
		first, err := selected.ExecuteDataSourceOperation(ctx, durabledata.SourceCommand{
			Operation: "import", SourceInvocationID: uuid.NewString(), Actor: "operator", BundleHash: testDataBundle,
			Declaration: ref, ExpectedHead: durabledata.AbsentHead(), InputFormat: "jsonl", Input: []byte("{\"slug\":\"first\",\"score\":1}\n"),
		})
		if err != nil || first.Outcome != "accepted" {
			t.Fatalf("first import = %#v, %v", first, err)
		}
		second, err := selected.ExecuteDataSourceOperation(ctx, durabledata.SourceCommand{
			Operation: "import", SourceInvocationID: uuid.NewString(), Actor: "operator", BundleHash: testDataBundle,
			Declaration: ref, ExpectedHead: first.Head.After, InputFormat: "jsonl", Input: []byte("{\"slug\":\"second\",\"score\":2}\n"),
		})
		if err != nil || second.Outcome != "accepted" {
			t.Fatalf("second import = %#v, %v", second, err)
		}

		assertConflict := func(name string, expected durabledata.ExpectedHead) {
			t.Helper()
			id := uuid.NewString()
			result, err := selected.ExecuteDataSourceOperation(ctx, durabledata.SourceCommand{
				Operation: "import", SourceInvocationID: id, Actor: "operator", BundleHash: testDataBundle,
				Declaration: ref, ExpectedHead: expected, InputFormat: "jsonl", Input: []byte("{\"slug\":\"candidate\",\"score\":3}\n"),
			})
			if err != nil || result.Outcome != "head_conflict" || result.Candidate.State != "candidate" || result.Candidate.Alias != "" ||
				result.Delta.State != "not_computed" || result.Delta.Reason != "head_conflict" || !result.ObservedHead.Equal(second.Head.After) {
				t.Fatalf("%s conflict = %#v, %v", name, result, err)
			}
			requireEmptySourceDefectsPage(t, result)
			receipt, err := selected.LoadDataSourceOperation(ctx, id)
			if err != nil || receipt.Result.CompletedAt != result.CompletedAt || receipt.Result.Delta.State != "not_computed" {
				t.Fatalf("%s conflict receipt = %#v, %v", name, receipt, err)
			}
			requireEmptySourceDefectsPage(t, receipt.Result)
		}

		assertConflict("absent", durabledata.AbsentHead())
		assertConflict("materialized historical", first.Head.After)
		missing := durabledata.VersionID("resource-version-v1:sha256:" + strings.Repeat("f", 64))
		assertConflict("missing historical", durabledata.VersionHead(missing))

		pruned, err := selected.PruneDataResource(ctx, durabledata.PruneCommand{
			PruneInvocationID: uuid.NewString(), Actor: "operator", Declaration: ref,
			VersionID: first.Candidate.VersionID, ExpectedHead: second.Head.After,
		})
		if err != nil || pruned.Outcome != "pruned" {
			t.Fatalf("prune historical version = %#v, %v", pruned, err)
		}
		assertConflict("pruned historical", first.Head.After)

		snapshot, err := selected.ShowDataResource(ctx, testDataBundle, ref)
		if err != nil || len(snapshot.Versions) != 2 || !snapshot.Head.Before.Equal(second.Head.After) {
			t.Fatalf("head after conflict matrix = %#v, %v", snapshot, err)
		}
	})
}

func TestDurableDataFailsClosedOnPayloadCorruption(t *testing.T) {
	forEachDurableDataStore(t, func(t *testing.T, selected durableDataSelectedStore, db *sql.DB) {
		ctx := context.Background()
		catalog, ref := durableDataTestCatalog(t)
		if err := registerDurableDataTestCatalog(ctx, selected, catalog); err != nil {
			t.Fatal(err)
		}
		result, err := selected.ExecuteDataSourceOperation(ctx, durabledata.SourceCommand{
			Operation: "import", SourceInvocationID: uuid.NewString(), Actor: "operator", BundleHash: testDataBundle,
			Declaration: ref, ExpectedHead: durabledata.AbsentHead(), InputFormat: "jsonl", Input: []byte("{\"slug\":\"alpha\",\"score\":1}\n"),
		})
		if err != nil {
			t.Fatal(err)
		}
		query := `UPDATE resource_versions SET canonical_jsonl = ? WHERE version_id = ?`
		if _, ok := selected.(*store.PostgresStore); ok {
			query = `UPDATE resource_versions SET canonical_jsonl = $1 WHERE version_id = $2`
		}
		if _, err := db.ExecContext(ctx, query, []byte("{\"slug\":\"poison\",\"score\":99}\n"), result.Candidate.VersionID); err != nil {
			t.Fatal(err)
		}
		if _, err := selected.ShowDataResource(ctx, testDataBundle, ref); err == nil {
			t.Fatal("ShowDataResource succeeded with contradictory immutable payload")
		}
		invocationID := uuid.NewString()
		_, err = selected.ExecuteDataSourceOperation(ctx, durabledata.SourceCommand{
			Operation: "import", SourceInvocationID: invocationID, Actor: "operator", BundleHash: testDataBundle,
			Declaration: ref, ExpectedHead: result.Head.After, InputFormat: "jsonl", Input: []byte("{\"slug\":\"next\",\"score\":2}\n"),
		})
		var domainErr *durabledata.DomainError
		if !errors.As(err, &domainErr) || domainErr.Code != durabledata.CodeIntegrity {
			t.Fatalf("matching corrupt-base import error = %v, want %s", err, durabledata.CodeIntegrity)
		}
		if _, err := selected.LoadDataSourceOperation(ctx, invocationID); err == nil {
			t.Fatal("corrupt-base import fabricated a permanent operation receipt")
		}
	})
}

func TestDurableDataSourceReceiptRejectsCandidateStateCorruption(t *testing.T) {
	forEachDurableDataStore(t, func(t *testing.T, selected durableDataSelectedStore, db *sql.DB) {
		ctx := context.Background()
		catalog, ref := durableDataTestCatalog(t)
		if err := registerDurableDataTestCatalog(ctx, selected, catalog); err != nil {
			t.Fatal(err)
		}
		invocationID := uuid.NewString()
		command := durabledata.SourceCommand{
			Operation: "import", SourceInvocationID: invocationID, Actor: "operator", BundleHash: testDataBundle,
			Declaration: ref, ExpectedHead: durabledata.AbsentHead(), InputFormat: "jsonl", Input: []byte("{\"slug\":\"alpha\",\"score\":1}\n"),
		}
		if _, err := selected.ExecuteDataSourceOperation(ctx, command); err != nil {
			t.Fatal(err)
		}
		query := `SELECT result_json FROM resource_source_invocations WHERE source_invocation_id = ?`
		update := `UPDATE resource_source_invocations SET result_json = ? WHERE source_invocation_id = ?`
		if _, ok := selected.(*store.PostgresStore); ok {
			query = `SELECT result_json FROM resource_source_invocations WHERE source_invocation_id = $1::uuid`
			update = `UPDATE resource_source_invocations SET result_json = $1 WHERE source_invocation_id = $2::uuid`
		}
		var raw []byte
		if err := db.QueryRowContext(ctx, query, invocationID).Scan(&raw); err != nil {
			t.Fatal(err)
		}
		var result map[string]any
		if err := json.Unmarshal(raw, &result); err != nil {
			t.Fatal(err)
		}
		candidate, ok := result["candidate"].(map[string]any)
		if !ok {
			t.Fatalf("stored candidate = %#v", result["candidate"])
		}
		candidate["state"] = "candidate"
		delete(candidate, "alias")
		corrupt, err := json.Marshal(result)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(ctx, update, corrupt, invocationID); err != nil {
			t.Fatal(err)
		}
		for _, load := range []func() error{
			func() error { _, err := selected.LoadDataSourceOperation(ctx, invocationID); return err },
			func() error { _, err := selected.ExecuteDataSourceOperation(ctx, command); return err },
		} {
			var domainErr *durabledata.DomainError
			if err := load(); !errors.As(err, &domainErr) || domainErr.Code != durabledata.CodeIntegrity {
				t.Fatalf("corrupt source candidate error = %v, want %s", err, durabledata.CodeIntegrity)
			}
		}
	})
}

func TestDurableDataSourceReceiptRejectsDefectsPageCorruption(t *testing.T) {
	forEachDurableDataStore(t, func(t *testing.T, selected durableDataSelectedStore, db *sql.DB) {
		ctx := context.Background()
		catalog, ref := durableDataTestCatalog(t)
		if err := registerDurableDataTestCatalog(ctx, selected, catalog); err != nil {
			t.Fatal(err)
		}
		invocationID := uuid.NewString()
		command := durabledata.SourceCommand{
			Operation: "import", SourceInvocationID: invocationID, Actor: "operator", BundleHash: testDataBundle,
			Declaration: ref, ExpectedHead: durabledata.AbsentHead(), InputFormat: "jsonl", Input: []byte("{\"slug\":\"alpha\",\"score\":1}\n"),
		}
		if _, err := selected.ExecuteDataSourceOperation(ctx, command); err != nil {
			t.Fatal(err)
		}
		query := `SELECT result_json FROM resource_source_invocations WHERE source_invocation_id = ?`
		update := `UPDATE resource_source_invocations SET result_json = ? WHERE source_invocation_id = ?`
		if _, ok := selected.(*store.PostgresStore); ok {
			query = `SELECT result_json FROM resource_source_invocations WHERE source_invocation_id = $1::uuid`
			update = `UPDATE resource_source_invocations SET result_json = $1 WHERE source_invocation_id = $2::uuid`
		}
		var raw []byte
		if err := db.QueryRowContext(ctx, query, invocationID).Scan(&raw); err != nil {
			t.Fatal(err)
		}
		var result map[string]any
		if err := json.Unmarshal(raw, &result); err != nil {
			t.Fatal(err)
		}
		result["defects"] = map[string]any{
			"items": nil, "item_count": 0, "encoded_items_bytes": 0,
			"continuation": map[string]any{"state": ""},
		}
		corrupt, err := json.Marshal(result)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(ctx, update, corrupt, invocationID); err != nil {
			t.Fatal(err)
		}
		for _, load := range []func() error{
			func() error { _, err := selected.LoadDataSourceOperation(ctx, invocationID); return err },
			func() error { _, err := selected.ExecuteDataSourceOperation(ctx, command); return err },
		} {
			var domainErr *durabledata.DomainError
			if err := load(); !errors.As(err, &domainErr) || domainErr.Code != durabledata.CodeIntegrity {
				t.Fatalf("corrupt source defects error = %v, want %s", err, durabledata.CodeIntegrity)
			}
		}
	})
}

func TestDurableDataSourceReceiptRejectsAggregateCorruptionMatrix(t *testing.T) {
	tests := []struct {
		name   string
		column string
		mutate func(map[string]any)
	}{
		{name: "result invocation", column: "result_json", mutate: func(value map[string]any) { value["source_invocation_id"] = uuid.NewString() }},
		{name: "result operation", column: "result_json", mutate: func(value map[string]any) { value["operation"] = "import" }},
		{name: "result outcome", column: "result_json", mutate: func(value map[string]any) { value["outcome"] = "validation_rejected" }},
		{name: "result bundle", column: "result_json", mutate: func(value map[string]any) {
			value["bundle_hash"] = "bundle-v1:sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
		}},
		{name: "result schema", column: "result_json", mutate: func(value map[string]any) {
			value["schema_digest"] = "resource-schema-v1:sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
		}},
		{name: "result declaration", column: "result_json", mutate: func(value map[string]any) {
			value["declaration"] = map[string]any{"package_key": ".", "event_name": "hostile.changed"}
		}},
		{name: "result expected head", column: "result_json", mutate: func(value map[string]any) {
			candidate := value["candidate"].(map[string]any)
			value["expected_head"] = map[string]any{"state": "version", "version_id": candidate["version_id"]}
		}},
		{name: "result observed head", column: "result_json", mutate: func(value map[string]any) {
			candidate := value["candidate"].(map[string]any)
			value["observed_head"] = map[string]any{"state": "version", "version_id": candidate["version_id"]}
		}},
		{name: "result candidate union", column: "result_json", mutate: func(value map[string]any) {
			value["candidate"].(map[string]any)["state"] = "none"
		}},
		{name: "result head transition", column: "result_json", mutate: func(value map[string]any) { value["head"].(map[string]any)["changed"] = true }},
		{name: "result delta", column: "result_json", mutate: func(value map[string]any) {
			value["delta"] = map[string]any{"state": "not_computed", "reason": "head_conflict"}
		}},
		{name: "result delta row identity", column: "result_json", mutate: func(value map[string]any) {
			delete(value["delta"].(map[string]any), "row_identity")
		}},
		{name: "result defect page", column: "result_json", mutate: func(value map[string]any) {
			value["defects"] = map[string]any{
				"items":      []any{map[string]any{"code": "hostile", "message": "hostile"}},
				"item_count": 1, "encoded_items_bytes": 42, "continuation": map[string]any{"state": "end"},
			}
		}},
		{name: "result completion", column: "result_json", mutate: func(value map[string]any) { value["completed_at"] = "0001-01-01T00:00:00Z" }},
		{name: "evidence defects", column: "evidence_json", mutate: func(value map[string]any) {
			value["defects"] = []any{map[string]any{"code": "hostile", "message": "hostile"}}
		}},
		{name: "keyed delta evidence deleted", column: "evidence_json", mutate: func(value map[string]any) {
			delete(value, "delta_added")
			delete(value, "delta_removed")
			delete(value, "delta_changed")
		}},
		{name: "request hash agreement", column: "request_json", mutate: func(value map[string]any) { value["actor"] = "hostile" }},
	}
	forEachDurableDataStore(t, func(t *testing.T, selected durableDataSelectedStore, db *sql.DB) {
		ctx := context.Background()
		catalog, ref := durableDataTestCatalog(t)
		if err := registerDurableDataTestCatalog(ctx, selected, catalog); err != nil {
			t.Fatal(err)
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				invocationID := uuid.NewString()
				command := durabledata.SourceCommand{
					Operation: "check", SourceInvocationID: invocationID, Actor: "operator", BundleHash: testDataBundle,
					Declaration: ref, ExpectedHead: durabledata.AbsentHead(), InputFormat: "jsonl", Input: []byte("{\"slug\":\"alpha\",\"score\":1}\n"),
				}
				if result, err := selected.ExecuteDataSourceOperation(ctx, command); err != nil || result.Outcome != "accepted" {
					t.Fatalf("source fixture = %#v, %v", result, err)
				}
				corruptDurableDataJSONColumn(t, ctx, selected, db, "resource_source_invocations", test.column, "source_invocation_id", invocationID, test.mutate)
				for _, load := range []func() error{
					func() error { _, err := selected.LoadDataSourceOperation(ctx, invocationID); return err },
					func() error { _, err := selected.ExecuteDataSourceOperation(ctx, command); return err },
				} {
					var domainErr *durabledata.DomainError
					if err := load(); !errors.As(err, &domainErr) || domainErr.Code != durabledata.CodeIntegrity {
						t.Fatalf("corrupt %s error = %v, want %s", test.name, err, durabledata.CodeIntegrity)
					}
				}
			})
		}
	})
}

func TestDurableDataSourceReceiptRejectsCoordinatedSemanticCorruption(t *testing.T) {
	forEachDurableDataStore(t, func(t *testing.T, selected durableDataSelectedStore, db *sql.DB) {
		ctx := context.Background()
		catalog, ref := durableDataTestCatalog(t)
		if err := registerDurableDataTestCatalog(ctx, selected, catalog); err != nil {
			t.Fatal(err)
		}
		newCheck := func(t *testing.T, input []byte, expected durabledata.ExpectedHead) (string, durabledata.SourceCommand) {
			t.Helper()
			id := uuid.NewString()
			command := durabledata.SourceCommand{
				Operation: "check", SourceInvocationID: id, Actor: "operator", BundleHash: testDataBundle,
				Declaration: ref, ExpectedHead: expected, InputFormat: "jsonl", Input: input,
			}
			if _, err := selected.ExecuteDataSourceOperation(ctx, command); err != nil {
				t.Fatal(err)
			}
			return id, command
		}
		assertIntegrity := func(t *testing.T, id string, command durabledata.SourceCommand) {
			t.Helper()
			for _, load := range []func() error{
				func() error { _, err := selected.LoadDataSourceOperation(ctx, id); return err },
				func() error { _, err := selected.ExecuteDataSourceOperation(ctx, command); return err },
			} {
				var domainErr *durabledata.DomainError
				if err := load(); !errors.As(err, &domainErr) || domainErr.Code != durabledata.CodeIntegrity {
					t.Fatalf("coordinated source corruption error = %v, want %s", err, durabledata.CodeIntegrity)
				}
			}
		}

		t.Run("changed exact input with valid request hash", func(t *testing.T) {
			id, command := newCheck(t, []byte("{\"slug\":\"alpha\",\"score\":1}\n"), durabledata.AbsentHead())
			hostile := command
			hostile.Input = []byte("{\"slug\":\"beta\",\"score\":2}\n")
			hash, raw, err := hostile.RequestHash()
			if err != nil {
				t.Fatal(err)
			}
			query := `UPDATE resource_source_invocations SET request_hash = ?, request_json = ? WHERE source_invocation_id = ?`
			if _, ok := selected.(*store.PostgresStore); ok {
				query = `UPDATE resource_source_invocations SET request_hash = $1, request_json = $2 WHERE source_invocation_id = $3::uuid`
			}
			if _, err := db.ExecContext(ctx, query, hash, raw, id); err != nil {
				t.Fatal(err)
			}
			assertIntegrity(t, id, hostile)
		})

		t.Run("keyed to positional downgrade", func(t *testing.T) {
			id, command := newCheck(t, []byte("{\"slug\":\"alpha\",\"score\":1}\n"), durabledata.AbsentHead())
			corruptDurableDataJSONColumn(t, ctx, selected, db, "resource_source_invocations", "result_json", "source_invocation_id", id, func(value map[string]any) {
				value["delta"].(map[string]any)["row_identity"] = "position"
			})
			corruptDurableDataJSONColumn(t, ctx, selected, db, "resource_source_invocations", "evidence_json", "source_invocation_id", id, func(value map[string]any) {
				value["delta_added"] = nil
				value["delta_removed"] = nil
				value["delta_changed"] = nil
			})
			assertIntegrity(t, id, command)
		})

		t.Run("candidate and manifest", func(t *testing.T) {
			id, command := newCheck(t, []byte("{\"slug\":\"alpha\",\"score\":1}\n"), durabledata.AbsentHead())
			other, defects := durabledata.CompileJSONL(ref, map[string]any{
				"type": "object", "additionalProperties": false, "required": []string{"score", "slug"},
				"properties": map[string]any{"slug": map[string]any{"type": "string"}, "score": map[string]any{"type": "integer"}},
			}, "slug", []byte("{\"slug\":\"beta\",\"score\":2}\n"))
			if len(defects) != 0 {
				t.Fatal(defects)
			}
			corruptDurableDataJSONColumn(t, ctx, selected, db, "resource_source_invocations", "result_json", "source_invocation_id", id, func(value map[string]any) {
				candidate := value["candidate"].(map[string]any)
				candidate["version_id"] = other.VersionID
				candidate["manifest"] = map[string]any{
					"manifest_format": other.Manifest.ManifestFormat, "declaration": map[string]any{"package_key": ref.PackageKey, "event": ref.EventName},
					"schema_digest": other.Manifest.SchemaDigest, "row_codec": other.Manifest.RowCodec,
					"content_digest": other.Manifest.ContentDigest, "row_count": other.Manifest.RowCount,
				}
			})
			assertIntegrity(t, id, command)
		})

		t.Run("delta classes and counts", func(t *testing.T) {
			id, command := newCheck(t, []byte("{\"slug\":\"alpha\",\"score\":1}\n"), durabledata.AbsentHead())
			corruptDurableDataJSONColumn(t, ctx, selected, db, "resource_source_invocations", "result_json", "source_invocation_id", id, func(value map[string]any) {
				value["delta"].(map[string]any)["summary"] = map[string]any{"added": 0, "removed": 1, "changed": 0}
			})
			corruptDurableDataJSONColumn(t, ctx, selected, db, "resource_source_invocations", "evidence_json", "source_invocation_id", id, func(value map[string]any) {
				value["delta_added"] = nil
				value["delta_removed"] = []any{map[string]any{"key": "alpha"}}
				value["delta_changed"] = nil
			})
			assertIntegrity(t, id, command)
		})

		t.Run("validation defects", func(t *testing.T) {
			id, command := newCheck(t, []byte("{}\n"), durabledata.AbsentHead())
			defect := map[string]any{"row": 1, "path": "$.slug", "code": "schema_rejected", "message": "coordinated false defect"}
			corruptDurableDataJSONColumn(t, ctx, selected, db, "resource_source_invocations", "result_json", "source_invocation_id", id, func(value map[string]any) {
				items := []any{defect}
				raw, _ := json.Marshal(items)
				value["defects"] = map[string]any{"items": items, "item_count": 1, "encoded_items_bytes": len(raw), "continuation": map[string]any{"state": "end"}}
			})
			corruptDurableDataJSONColumn(t, ctx, selected, db, "resource_source_invocations", "evidence_json", "source_invocation_id", id, func(value map[string]any) {
				value["defects"] = []any{defect}
			})
			assertIntegrity(t, id, command)
		})

		t.Run("head conflict candidate", func(t *testing.T) {
			first, err := selected.ExecuteDataSourceOperation(ctx, durabledata.SourceCommand{
				Operation: "import", SourceInvocationID: uuid.NewString(), Actor: "operator", BundleHash: testDataBundle,
				Declaration: ref, ExpectedHead: durabledata.AbsentHead(), InputFormat: "jsonl", Input: []byte("{\"slug\":\"head\",\"score\":1}\n"),
			})
			if err != nil || first.Outcome != "accepted" {
				t.Fatalf("head-conflict base = %#v, %v", first, err)
			}
			id, command := newCheck(t, []byte("{\"slug\":\"stale\",\"score\":2}\n"), durabledata.AbsentHead())
			record, err := selected.LoadDataSourceOperation(ctx, id)
			if err != nil || record.Result.Outcome != "head_conflict" {
				t.Fatalf("head-conflict receipt = %#v, %v", record, err)
			}
			other, defects := durabledata.CompileJSONL(ref, map[string]any{
				"type": "object", "additionalProperties": false, "required": []string{"score", "slug"},
				"properties": map[string]any{"slug": map[string]any{"type": "string"}, "score": map[string]any{"type": "integer"}},
			}, "slug", []byte("{\"slug\":\"other\",\"score\":3}\n"))
			if len(defects) != 0 {
				t.Fatal(defects)
			}
			corruptDurableDataJSONColumn(t, ctx, selected, db, "resource_source_invocations", "result_json", "source_invocation_id", id, func(value map[string]any) {
				candidate := value["candidate"].(map[string]any)
				candidate["version_id"] = other.VersionID
				candidate["manifest"] = map[string]any{
					"manifest_format": other.Manifest.ManifestFormat, "declaration": map[string]any{"package_key": ref.PackageKey, "event": ref.EventName},
					"schema_digest": other.Manifest.SchemaDigest, "row_codec": other.Manifest.RowCodec,
					"content_digest": other.Manifest.ContentDigest, "row_count": other.Manifest.RowCount,
				}
			})
			assertIntegrity(t, id, command)
		})
	})
}

func TestDurableDataSourceReceiptsRetainHistoricalBaseAfterPrune(t *testing.T) {
	forEachDurableDataStore(t, func(t *testing.T, selected durableDataSelectedStore, _ *sql.DB) {
		ctx := context.Background()
		catalog, ref := durableDataTestCatalog(t)
		if err := registerDurableDataTestCatalog(ctx, selected, catalog); err != nil {
			t.Fatal(err)
		}
		first, err := selected.ExecuteDataSourceOperation(ctx, durabledata.SourceCommand{
			Operation: "import", SourceInvocationID: uuid.NewString(), Actor: "operator", BundleHash: testDataBundle,
			Declaration: ref, ExpectedHead: durabledata.AbsentHead(), InputFormat: "jsonl", Input: []byte("{\"slug\":\"alpha\",\"score\":1}\n"),
		})
		if err != nil {
			t.Fatal(err)
		}
		checkCommand := durabledata.SourceCommand{
			Operation: "check", SourceInvocationID: uuid.NewString(), Actor: "operator", BundleHash: testDataBundle,
			Declaration: ref, ExpectedHead: first.Head.After, InputFormat: "jsonl", Input: []byte("{\"slug\":\"alpha\",\"score\":2}\n"),
		}
		checked, err := selected.ExecuteDataSourceOperation(ctx, checkCommand)
		if err != nil || checked.Outcome != "accepted" || checked.Delta.Summary == nil || checked.Delta.Summary.Changed != 1 {
			t.Fatalf("historical-base check = %#v, %v", checked, err)
		}
		conflictCommand := checkCommand
		conflictCommand.SourceInvocationID = uuid.NewString()
		conflictCommand.ExpectedHead = durabledata.AbsentHead()
		conflict, err := selected.ExecuteDataSourceOperation(ctx, conflictCommand)
		if err != nil || conflict.Outcome != "head_conflict" {
			t.Fatalf("historical-base conflict = %#v, %v", conflict, err)
		}
		second, err := selected.ExecuteDataSourceOperation(ctx, durabledata.SourceCommand{
			Operation: "import", SourceInvocationID: uuid.NewString(), Actor: "operator", BundleHash: testDataBundle,
			Declaration: ref, ExpectedHead: first.Head.After, InputFormat: "jsonl", Input: []byte("{\"slug\":\"beta\",\"score\":2}\n"),
		})
		if err != nil {
			t.Fatal(err)
		}
		pruned, err := selected.PruneDataResource(ctx, durabledata.PruneCommand{
			PruneInvocationID: uuid.NewString(), Actor: "operator", Declaration: ref,
			VersionID: first.Candidate.VersionID, ExpectedHead: second.Head.After,
		})
		if err != nil || pruned.Outcome != "pruned" {
			t.Fatalf("prune historical base = %#v, %v", pruned, err)
		}
		for _, test := range []struct {
			command durabledata.SourceCommand
			want    durabledata.SourceOperationResult
		}{{checkCommand, checked}, {conflictCommand, conflict}} {
			record, err := selected.LoadDataSourceOperation(ctx, test.command.SourceInvocationID)
			if err != nil || !reflect.DeepEqual(record.Result, test.want) {
				t.Fatalf("historical receipt readback = %#v, %v; want %#v", record.Result, err, test.want)
			}
			replay, err := selected.ExecuteDataSourceOperation(ctx, test.command)
			if err != nil || !reflect.DeepEqual(replay, test.want) {
				t.Fatalf("historical receipt replay = %#v, %v; want %#v", replay, err, test.want)
			}
		}
	})
}

func TestDurableDataProvenanceOwnsTypedLineageAndProjection(t *testing.T) {
	forEachDurableDataStore(t, func(t *testing.T, selected durableDataSelectedStore, db *sql.DB) {
		ctx := context.Background()
		catalog, ref := durableDataTestCatalog(t)
		if err := registerDurableDataTestCatalog(ctx, selected, catalog); err != nil {
			t.Fatal(err)
		}
		command := durabledata.SourceCommand{
			Operation: "import", SourceInvocationID: uuid.NewString(), Actor: "operator", BundleHash: testDataBundle,
			Declaration: ref, ExpectedHead: durabledata.AbsentHead(), InputFormat: "jsonl", Input: []byte("{\"slug\":\"alpha\",\"score\":1}\n"),
		}
		result, err := selected.ExecuteDataSourceOperation(ctx, command)
		if err != nil {
			t.Fatal(err)
		}
		now := result.CompletedAt
		insertProvenance := func(sequence uint64, kind, id string) {
			t.Helper()
			refValue, err := durabledata.NewProvenanceRef(kind, id)
			if err != nil {
				t.Fatal(err)
			}
			provenance := durabledata.Provenance{Sequence: sequence, VersionID: result.Candidate.VersionID, ProducerRef: refValue, Actor: "operator", CommittedAt: now}
			if err := provenance.Validate(); err != nil {
				t.Fatal(err)
			}
			raw, _ := json.Marshal(provenance)
			query := `INSERT INTO resource_version_provenance (version_id, provenance_sequence, producer_kind, producer_id, actor, provenance_json, committed_at) VALUES (?, ?, ?, ?, ?, ?, ?)`
			if _, ok := selected.(*store.PostgresStore); ok {
				query = `INSERT INTO resource_version_provenance (version_id, provenance_sequence, producer_kind, producer_id, actor, provenance_json, committed_at) VALUES ($1, $2, $3, $4::uuid, $5, $6, $7)`
			}
			if _, err := db.ExecContext(ctx, query, result.Candidate.VersionID, sequence, kind, id, "operator", raw, now); err != nil {
				t.Fatal(err)
			}
		}
		insertProvenance(2, "normal_run", uuid.NewString())
		insertProvenance(3, "fork_candidate_promotion", uuid.NewString())
		snapshot, err := selected.ShowDataResource(ctx, testDataBundle, ref)
		if err != nil || len(snapshot.Versions) != 1 || len(snapshot.Versions[0].Provenance) != 3 {
			t.Fatalf("all provenance arms = %#v, %v", snapshot.Versions, err)
		}
		for index, provenance := range snapshot.Versions[0].Provenance {
			if provenance.Sequence != uint64(index+1) || provenance.Validate() != nil {
				t.Fatalf("provenance %d = %#v", index, provenance)
			}
		}

		assertCorrupt := func(t *testing.T) {
			t.Helper()
			var domainErr *durabledata.DomainError
			if _, err := selected.ShowDataResource(ctx, testDataBundle, ref); !errors.As(err, &domainErr) || domainErr.Code != durabledata.CodeIntegrity {
				t.Fatalf("provenance contradiction error = %v, want %s", err, durabledata.CodeIntegrity)
			}
		}
		jsonMutations := []struct {
			name   string
			mutate func(map[string]any)
		}{
			{name: "version", mutate: func(value map[string]any) {
				value["version_id"] = "resource-version-v1:sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
			}},
			{name: "producer ref", mutate: func(value map[string]any) {
				value["producer_ref"] = map[string]any{"kind": "normal_run", "run_id": uuid.NewString()}
			}},
			{name: "actor", mutate: func(value map[string]any) { value["actor"] = "hostile" }},
			{name: "committed at", mutate: func(value map[string]any) { value["committed_at"] = now.Add(time.Minute).Format(time.RFC3339Nano) }},
		}
		for _, test := range jsonMutations {
			t.Run("json/"+test.name, func(t *testing.T) {
				query := `SELECT provenance_json FROM resource_version_provenance WHERE version_id = ? AND provenance_sequence = 1`
				update := `UPDATE resource_version_provenance SET provenance_json = ? WHERE version_id = ? AND provenance_sequence = 1`
				if _, ok := selected.(*store.PostgresStore); ok {
					query = `SELECT provenance_json FROM resource_version_provenance WHERE version_id = $1 AND provenance_sequence = 1`
					update = `UPDATE resource_version_provenance SET provenance_json = $1 WHERE version_id = $2 AND provenance_sequence = 1`
				}
				var original []byte
				if err := db.QueryRowContext(ctx, query, result.Candidate.VersionID).Scan(&original); err != nil {
					t.Fatal(err)
				}
				var value map[string]any
				_ = json.Unmarshal(original, &value)
				test.mutate(value)
				corrupt, _ := json.Marshal(value)
				if _, err := db.ExecContext(ctx, update, corrupt, result.Candidate.VersionID); err != nil {
					t.Fatal(err)
				}
				assertCorrupt(t)
				if _, err := db.ExecContext(ctx, update, original, result.Candidate.VersionID); err != nil {
					t.Fatal(err)
				}
			})
		}
		typedMutations := []struct {
			name    string
			column  string
			hostile any
			valid   any
		}{
			{name: "producer kind", column: "producer_kind", hostile: "normal_run", valid: "import"},
			{name: "producer id", column: "producer_id", hostile: uuid.NewString(), valid: command.SourceInvocationID},
			{name: "actor", column: "actor", hostile: "hostile", valid: "operator"},
			{name: "committed at", column: "committed_at", hostile: now.Add(time.Minute), valid: now},
		}
		for _, test := range typedMutations {
			t.Run("typed/"+test.name, func(t *testing.T) {
				update := fmt.Sprintf("UPDATE resource_version_provenance SET %s = ? WHERE version_id = ? AND provenance_sequence = 1", test.column)
				if _, ok := selected.(*store.PostgresStore); ok {
					update = fmt.Sprintf("UPDATE resource_version_provenance SET %s = $1 WHERE version_id = $2 AND provenance_sequence = 1", test.column)
				}
				if _, err := db.ExecContext(ctx, update, test.hostile, result.Candidate.VersionID); err != nil {
					t.Fatal(err)
				}
				assertCorrupt(t)
				if _, err := db.ExecContext(ctx, update, test.valid, result.Candidate.VersionID); err != nil {
					t.Fatal(err)
				}
			})
		}
	})
}

func TestDurableDataPruneReceiptRejectsDestructiveDecisionCorruption(t *testing.T) {
	tests := []struct {
		name   string
		column string
		mutate func(map[string]any)
	}{
		{name: "result invocation", column: "result_json", mutate: func(value map[string]any) { value["prune_invocation_id"] = uuid.NewString() }},
		{name: "result outcome", column: "result_json", mutate: func(value map[string]any) { value["outcome"] = "already_pruned" }},
		{name: "result declaration", column: "result_json", mutate: func(value map[string]any) {
			value["declaration"] = map[string]any{"package_key": ".", "event_name": "hostile.changed"}
		}},
		{name: "result version", column: "result_json", mutate: func(value map[string]any) {
			value["version_id"] = "resource-version-v1:sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
		}},
		{name: "result expected head", column: "result_json", mutate: func(value map[string]any) {
			value["expected_head"] = map[string]any{"state": "absent"}
		}},
		{name: "result observed head", column: "result_json", mutate: func(value map[string]any) {
			value["observed_head"] = map[string]any{"state": "version", "version_id": value["version_id"]}
		}},
		{name: "result current version", column: "result_json", mutate: func(value map[string]any) { value["current_version_id"] = value["version_id"] }},
		{name: "result payload before", column: "result_json", mutate: func(value map[string]any) { value["payload_before"] = "pruned" }},
		{name: "result payload after", column: "result_json", mutate: func(value map[string]any) { value["payload_after"] = "materialized" }},
		{name: "result pin count", column: "result_json", mutate: func(value map[string]any) { value["pin_count"] = 1 }},
		{name: "result defects", column: "result_json", mutate: func(value map[string]any) {
			value["defects"] = map[string]any{
				"items":      []any{map[string]any{"code": "version_not_found", "message": "hostile"}},
				"item_count": 1, "encoded_items_bytes": 61, "continuation": map[string]any{"state": "end"},
			}
		}},
		{name: "result completion", column: "result_json", mutate: func(value map[string]any) { value["completed_at"] = "0001-01-01T00:00:00Z" }},
		{name: "request hash agreement", column: "request_json", mutate: func(value map[string]any) { value["actor"] = "hostile" }},
	}
	forEachDurableDataStore(t, func(t *testing.T, selected durableDataSelectedStore, db *sql.DB) {
		ctx := context.Background()
		catalog, ref := durableDataTestCatalog(t)
		if err := registerDurableDataTestCatalog(ctx, selected, catalog); err != nil {
			t.Fatal(err)
		}
		head := durabledata.AbsentHead()
		for index, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				target, err := selected.ExecuteDataSourceOperation(ctx, durabledata.SourceCommand{
					Operation: "import", SourceInvocationID: uuid.NewString(), Actor: "operator", BundleHash: testDataBundle,
					Declaration: ref, ExpectedHead: head, InputFormat: "jsonl",
					Input: []byte(fmt.Sprintf("{\"slug\":\"target-%d\",\"score\":%d}\n", index, index)),
				})
				if err != nil {
					t.Fatal(err)
				}
				current, err := selected.ExecuteDataSourceOperation(ctx, durabledata.SourceCommand{
					Operation: "import", SourceInvocationID: uuid.NewString(), Actor: "operator", BundleHash: testDataBundle,
					Declaration: ref, ExpectedHead: target.Head.After, InputFormat: "jsonl",
					Input: []byte(fmt.Sprintf("{\"slug\":\"current-%d\",\"score\":%d}\n", index, index)),
				})
				if err != nil {
					t.Fatal(err)
				}
				head = current.Head.After
				pruneID := uuid.NewString()
				command := durabledata.PruneCommand{
					PruneInvocationID: pruneID, Actor: "operator", Declaration: ref,
					VersionID: target.Candidate.VersionID, ExpectedHead: current.Head.After,
				}
				if result, err := selected.PruneDataResource(ctx, command); err != nil || result.Outcome != "pruned" {
					t.Fatalf("prune fixture = %#v, %v", result, err)
				}
				corruptDurableDataJSONColumn(t, ctx, selected, db, "resource_prune_invocations", test.column, "prune_invocation_id", pruneID, test.mutate)
				for _, load := range []func() error{
					func() error { _, err := selected.LoadDataPruneOperation(ctx, pruneID); return err },
					func() error { _, err := selected.PruneDataResource(ctx, command); return err },
				} {
					var domainErr *durabledata.DomainError
					if err := load(); !errors.As(err, &domainErr) || domainErr.Code != durabledata.CodeIntegrity {
						t.Fatalf("corrupt %s error = %v, want %s", test.name, err, durabledata.CodeIntegrity)
					}
				}
			})
		}
	})
}

func corruptDurableDataJSONColumn(t testing.TB, ctx context.Context, selected durableDataSelectedStore, db *sql.DB, table, column, identityColumn, identity string, mutate func(map[string]any)) {
	t.Helper()
	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s = ?", column, table, identityColumn)
	update := fmt.Sprintf("UPDATE %s SET %s = ? WHERE %s = ?", table, column, identityColumn)
	if _, ok := selected.(*store.PostgresStore); ok {
		query = fmt.Sprintf("SELECT %s FROM %s WHERE %s = $1::uuid", column, table, identityColumn)
		update = fmt.Sprintf("UPDATE %s SET %s = $1 WHERE %s = $2::uuid", table, column, identityColumn)
	}
	var raw []byte
	if err := db.QueryRowContext(ctx, query, identity).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var value map[string]any
	if err := json.Unmarshal(raw, &value); err != nil {
		t.Fatal(err)
	}
	mutate(value)
	corrupt, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, update, corrupt, identity); err != nil {
		t.Fatal(err)
	}
}

func TestDurableDataPruneValidatesCurrentAggregateBeforeHeadConflict(t *testing.T) {
	forEachDurableDataStore(t, func(t *testing.T, selected durableDataSelectedStore, db *sql.DB) {
		ctx := context.Background()
		catalog, ref := durableDataTestCatalog(t)
		if err := registerDurableDataTestCatalog(ctx, selected, catalog); err != nil {
			t.Fatal(err)
		}
		first, err := selected.ExecuteDataSourceOperation(ctx, durabledata.SourceCommand{
			Operation: "import", SourceInvocationID: uuid.NewString(), Actor: "operator", BundleHash: testDataBundle,
			Declaration: ref, ExpectedHead: durabledata.AbsentHead(), InputFormat: "jsonl", Input: []byte("{\"slug\":\"first\",\"score\":1}\n"),
		})
		if err != nil {
			t.Fatal(err)
		}
		second, err := selected.ExecuteDataSourceOperation(ctx, durabledata.SourceCommand{
			Operation: "import", SourceInvocationID: uuid.NewString(), Actor: "operator", BundleHash: testDataBundle,
			Declaration: ref, ExpectedHead: first.Head.After, InputFormat: "jsonl", Input: []byte("{\"slug\":\"second\",\"score\":2}\n"),
		})
		if err != nil {
			t.Fatal(err)
		}
		query := `UPDATE resource_versions SET canonical_jsonl = ? WHERE version_id = ?`
		if _, ok := selected.(*store.PostgresStore); ok {
			query = `UPDATE resource_versions SET canonical_jsonl = $1 WHERE version_id = $2`
		}
		if _, err := db.ExecContext(ctx, query, []byte("{\"slug\":\"corrupt\",\"score\":99}\n"), second.Candidate.VersionID); err != nil {
			t.Fatal(err)
		}

		pruneID := uuid.NewString()
		_, err = selected.PruneDataResource(ctx, durabledata.PruneCommand{
			PruneInvocationID: pruneID, Actor: "operator", Declaration: ref,
			VersionID: first.Candidate.VersionID, ExpectedHead: durabledata.AbsentHead(),
		})
		var domainErr *durabledata.DomainError
		if !errors.As(err, &domainErr) || domainErr.Code != durabledata.CodeIntegrity {
			t.Fatalf("prune error = %v, want %s before head_conflict", err, durabledata.CodeIntegrity)
		}

		var receiptCount int
		query = `SELECT COUNT(*) FROM resource_prune_invocations WHERE prune_invocation_id = ?`
		if _, ok := selected.(*store.PostgresStore); ok {
			query = `SELECT COUNT(*) FROM resource_prune_invocations WHERE prune_invocation_id = $1`
		}
		if err := db.QueryRowContext(ctx, query, pruneID).Scan(&receiptCount); err != nil || receiptCount != 0 {
			t.Fatalf("prune receipt count = %d, %v; want zero", receiptCount, err)
		}
		var payload []byte
		query = `SELECT canonical_jsonl FROM resource_versions WHERE version_id = ?`
		if _, ok := selected.(*store.PostgresStore); ok {
			query = `SELECT canonical_jsonl FROM resource_versions WHERE version_id = $1`
		}
		if err := db.QueryRowContext(ctx, query, first.Candidate.VersionID).Scan(&payload); err != nil || payload == nil {
			t.Fatalf("historical payload after failed prune = %q, %v; want materialized", payload, err)
		}
	})
}

func TestDurableDataPruneRetainsCompletePinRefusalEvidence(t *testing.T) {
	forEachDurableDataStore(t, func(t *testing.T, selected durableDataSelectedStore, db *sql.DB) {
		ctx := context.Background()
		catalog, ref := durableDataTestCatalog(t)
		if err := registerDurableDataTestCatalog(ctx, selected, catalog); err != nil {
			t.Fatal(err)
		}
		first, err := selected.ExecuteDataSourceOperation(ctx, durabledata.SourceCommand{
			Operation: "import", SourceInvocationID: uuid.NewString(), Actor: "operator", BundleHash: testDataBundle,
			Declaration: ref, ExpectedHead: durabledata.AbsentHead(), InputFormat: "jsonl", Input: []byte("{\"slug\":\"first\",\"score\":1}\n"),
		})
		if err != nil || first.Outcome != "accepted" {
			t.Fatalf("first import = %#v, %v", first, err)
		}
		second, err := selected.ExecuteDataSourceOperation(ctx, durabledata.SourceCommand{
			Operation: "import", SourceInvocationID: uuid.NewString(), Actor: "operator", BundleHash: testDataBundle,
			Declaration: ref, ExpectedHead: first.Head.After, InputFormat: "jsonl", Input: []byte("{\"slug\":\"second\",\"score\":2}\n"),
		})
		if err != nil || second.Outcome != "accepted" {
			t.Fatalf("second import = %#v, %v", second, err)
		}

		postgres := false
		if _, ok := selected.(*store.PostgresStore); ok {
			postgres = true
		}
		const pinCount = durabledata.MaxPublicPageItems + 5
		for index := 0; index < pinCount; index++ {
			runID := fmt.Sprintf("00000000-0000-4000-8000-%012d", index+1)
			storetest.RequireRun(t, ctx, selected, storetest.RunFixture{
				RunID:        runID,
				State:        runtimerunlifecycle.StateRunning,
				Origin:       storetest.ScenarioSetupOrigin(),
				BundleHash:   testDataBundle,
				BundleSource: runtimerunlifecycle.BundleSourceEphemeral,
				StartedAt:    time.Unix(int64(index+1), 0).UTC(),
			})
		}

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		pinInsert := `INSERT INTO resource_version_pins (run_id, package_key, event_name, schema_digest, version_id, selection, pinned_at) VALUES (?, ?, ?, ?, ?, 'explicit', ?)`
		if postgres {
			pinInsert = `INSERT INTO resource_version_pins (run_id, package_key, event_name, schema_digest, version_id, selection, pinned_at) VALUES ($1::uuid, $2, $3, $4, $5, 'explicit', $6)`
		}
		for index := 0; index < pinCount; index++ {
			runID := fmt.Sprintf("00000000-0000-4000-8000-%012d", index+1)
			if _, err := tx.ExecContext(ctx, pinInsert, runID, ref.PackageKey, ref.EventName, first.SchemaDigest, first.Candidate.VersionID, time.Unix(int64(index+1), 0).UTC()); err != nil {
				_ = tx.Rollback()
				t.Fatalf("insert pin evidence source %d: %v", index, err)
			}
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}

		pruneID := uuid.NewString()
		command := durabledata.PruneCommand{
			PruneInvocationID: pruneID, Actor: "operator", Declaration: ref,
			VersionID: first.Candidate.VersionID, ExpectedHead: second.Head.After,
		}
		result, err := selected.PruneDataResource(ctx, command)
		if err != nil || result.Outcome != "refused_pinned" || result.PinCount != pinCount || result.Pins == nil || result.Pins.Continuation.State != "more" || len(result.Pins.Items) >= pinCount {
			t.Fatalf("prune refusal = %#v, %v", result, err)
		}
		replay, err := selected.PruneDataResource(ctx, command)
		if err != nil || replay.CompletedAt != result.CompletedAt || replay.PinCount != pinCount {
			t.Fatalf("prune refusal replay = %#v, %v", replay, err)
		}
		pins, err := selected.LoadDataPruneOperationPins(ctx, pruneID)
		if err != nil || len(pins) != pinCount {
			t.Fatalf("complete prune pin evidence count = %d, %v", len(pins), err)
		}
		if pins[0].RunID != "00000000-0000-4000-8000-000000000001" || pins[pinCount-1].RunID != fmt.Sprintf("00000000-0000-4000-8000-%012d", pinCount) {
			t.Fatalf("complete prune pin evidence first/last = %#v/%#v", pins[0], pins[pinCount-1])
		}

		publicStore, ok := selected.(apiv1.DurableDataStore)
		if !ok {
			t.Fatal("selected store does not expose the durable data public read owner")
		}
		show := apiv1.OperatorDataHandlers(apiv1.DataHandlerOptions{Store: publicStore})["data.show"]
		cursor := ""
		seen := map[string]struct{}{}
		for {
			page := map[string]any{"limit": 137, "byte_limit": durabledata.MaxPublicPageBytes}
			if cursor != "" {
				page["cursor"] = cursor
			}
			result, err := show(ctx, apiv1.Request{Method: "data.show", Params: map[string]any{
				"view": "operation", "operation_ref": map[string]any{"kind": "prune", "prune_invocation_id": pruneID},
				"detail": "pins", "page": page,
			}})
			if err != nil {
				t.Fatalf("page prune pin evidence: %v", err)
			}
			publicPage, ok := result.(durabledata.PageResult[durabledata.Pin])
			if !ok || len(publicPage.Items) == 0 {
				t.Fatalf("prune pin public page = %#v", result)
			}
			for _, pin := range publicPage.Items {
				if _, duplicate := seen[pin.RunID]; duplicate {
					t.Fatalf("duplicate paged prune pin %s", pin.RunID)
				}
				seen[pin.RunID] = struct{}{}
			}
			if publicPage.Continuation.State == "end" {
				break
			}
			if publicPage.Continuation.State != "more" || publicPage.Continuation.Cursor == "" {
				t.Fatalf("prune pin continuation = %#v", publicPage.Continuation)
			}
			cursor = publicPage.Continuation.Cursor
		}
		if len(seen) != pinCount {
			t.Fatalf("public prune pin evidence count = %d, want %d", len(seen), pinCount)
		}

		deleteQuery := `DELETE FROM resource_prune_pin_evidence WHERE prune_invocation_id = ? AND ordinal = ?`
		if postgres {
			deleteQuery = `DELETE FROM resource_prune_pin_evidence WHERE prune_invocation_id = $1::uuid AND ordinal = $2`
		}
		if _, err := db.ExecContext(ctx, deleteQuery, pruneID, pinCount); err != nil {
			t.Fatal(err)
		}
		for _, load := range []func() error{
			func() error { _, err := selected.LoadDataPruneOperation(ctx, pruneID); return err },
			func() error { _, err := selected.PruneDataResource(ctx, command); return err },
		} {
			var domainErr *durabledata.DomainError
			if err := load(); !errors.As(err, &domainErr) || domainErr.Code != durabledata.CodeIntegrity {
				t.Fatalf("corrupt prune evidence error = %v, want %s", err, durabledata.CodeIntegrity)
			}
		}
	})
}

func registerDurableDataTestCatalog(ctx context.Context, selected durableDataSelectedStore, catalog durabledata.Catalog) error {
	_, err := selected.UpsertBundleCatalogWithData(ctx, bundlecatalog.Upsert{
		BundleHash:  catalog.BundleHash,
		ContentYAML: "api_version: swarm.bundle.catalog.test.v1\n",
		ParsedJSON:  map[string]any{"projection_version": "swarm.bundle.catalog.v2", "agents": []any{}},
		Metadata:    map[string]any{"source": "durable-data-test"},
	}, catalog)
	return err
}

func durableDataTestCatalog(t *testing.T) (durabledata.Catalog, durabledata.DeclarationRef) {
	t.Helper()
	ref, err := durabledata.ParseDeclarationRef(".", testDataEvent)
	if err != nil {
		t.Fatal(err)
	}
	schema := map[string]any{
		"type": "object", "additionalProperties": false,
		"required": []string{"score", "slug"},
		"properties": map[string]any{
			"slug":  map[string]any{"type": "string"},
			"score": map[string]any{"type": "integer"},
		},
	}
	compiled, defects := durabledata.CompileJSONL(ref, schema, "slug", []byte("{\"slug\":\"probe\",\"score\":1}\n"))
	if len(defects) != 0 {
		t.Fatalf("compile data catalog schema: %#v", defects)
	}
	return durabledata.Catalog{
		BundleHash: testDataBundle,
		Declarations: []durabledata.Declaration{{
			Name: "score.available", Ref: ref, BusinessKey: "slug",
			SchemaDigest: compiled.Manifest.SchemaDigest, CanonicalSchema: compiled.CanonicalSchema,
		}},
	}, ref
}

func durableDataKeylessTestCatalog(t *testing.T) (durabledata.Catalog, durabledata.DeclarationRef) {
	t.Helper()
	ref, err := durabledata.ParseDeclarationRef(".", "score.observed")
	if err != nil {
		t.Fatal(err)
	}
	schema := map[string]any{
		"type": "object", "additionalProperties": false,
		"required": []string{"label", "score"},
		"properties": map[string]any{
			"label": map[string]any{"type": "string"},
			"score": map[string]any{"type": "integer"},
		},
	}
	compiled, defects := durabledata.CompileJSONL(ref, schema, "", []byte("{\"label\":\"probe\",\"score\":1}\n"))
	if len(defects) != 0 {
		t.Fatalf("compile keyless catalog schema: %#v", defects)
	}
	return durabledata.Catalog{
		BundleHash: testDataBundle,
		Declarations: []durabledata.Declaration{{
			Name: "score.observed", Ref: ref,
			SchemaDigest: compiled.Manifest.SchemaDigest, CanonicalSchema: compiled.CanonicalSchema,
		}},
	}, ref
}
