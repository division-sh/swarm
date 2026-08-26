package apiv1

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/bundlecatalog"
	"github.com/division-sh/swarm/internal/durabledata"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

const dataRunSchemaBundleHash = "bundle-v1:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
const dataRunMissingBundleHash = "bundle-v1:sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"

type dataRunLifecycleStore interface {
	runtimebus.EventStore
	RunReadStore
	ObservabilityReadStore
	APIIdempotencyStore
	RunBundleContextStore
	EntityReadStore
	runtimerunlifecycle.OperationOwner
	runtimerunlifecycle.CandidateStore
	UpsertBundleCatalogWithData(context.Context, bundlecatalog.Upsert, durabledata.Catalog) (bundlecatalog.UpsertResult, error)
	ExecuteDataSourceOperation(context.Context, durabledata.SourceCommand) (durabledata.SourceOperationResult, error)
	PruneDataResource(context.Context, durabledata.PruneCommand) (durabledata.PruneOperationResult, error)
	ShowDataResource(context.Context, string, durabledata.DeclarationRef) (durabledata.ResourceSnapshot, error)
	ListDataDeclarationSummaries(context.Context, string) ([]durabledata.DeclarationSummary, error)
	LoadDataSourceOperation(context.Context, string) (durabledata.SourceOperationRecord, error)
	LoadDataPruneOperation(context.Context, string) (durabledata.PruneOperationResult, error)
	LoadDataPruneOperationPins(context.Context, string) ([]durabledata.Pin, error)
	LoadDataPins(context.Context, durabledata.VersionID) ([]durabledata.Pin, error)
	LoadDataHeadHistory(context.Context, durabledata.DeclarationRef) ([]durabledata.HeadHistory, error)
	LoadDataRunCreationOperation(context.Context, string) (durabledata.RunCreationOperationRecord, error)
}

type dataRunLifecycleFixture struct {
	primary       dataRunLifecycleStore
	reconstructed dataRunLifecycleStore
	db            *sql.DB
}

func forEachDataRunLifecycleStore(t *testing.T, run func(*testing.T, dataRunLifecycleFixture)) {
	t.Helper()
	t.Run("sqlite", func(t *testing.T) {
		primary, reconstructed := storetest.StartSQLiteRuntimeStorePair(t)
		run(t, dataRunLifecycleFixture{primary: primary, reconstructed: reconstructed, db: storetest.Database(primary)})
	})
	t.Run("postgres", func(t *testing.T) {
		_, db, _ := testutil.StartPostgres(t)
		primary := storetest.AdmitPostgresRuntimeStore(t, db)
		reconstructed := storetest.AdmitPostgresRuntimeStore(t, db)
		run(t, dataRunLifecycleFixture{primary: primary, reconstructed: reconstructed, db: db})
	})
}

func TestDurableDataRunLifecycleAcrossSelectedStores(t *testing.T) {
	forEachDataRunLifecycleStore(t, func(t *testing.T, fixture dataRunLifecycleFixture) {
		ctx := context.Background()
		catalog, scanRef, scoreRef := dataRunLifecycleCatalog(t, runStartTestBundleHash, false)
		if err := registerDataRunLifecycleCatalog(ctx, fixture.primary, catalog); err != nil {
			t.Fatalf("register data run catalog: %v", err)
		}
		source := semanticview.Wrap(runStartTestBundle("scan.requested"))
		bus, err := newScopedAPITestEventBus(t, fixture.primary, runStartTestEventBusOptions(source))
		if err != nil {
			t.Fatalf("NewEventBusWithOptions: %v", err)
		}
		// The EventBus test fixture admits its empty semantic projection first; the
		// exact catalog admission above must remain the declaration authority.
		if err := registerDataRunLifecycleCatalog(ctx, fixture.primary, catalog); err != nil {
			t.Fatalf("restore exact data run catalog: %v", err)
		}
		handler := eventPublishTestHandlerWithStores(t, fixture.primary, fixture.primary, fixture.primary, bus, source)

		t.Run("duplicate fused child invocation IDs fail before success or rejection persistence", func(t *testing.T) {
			for _, test := range []struct {
				name       string
				secondData []byte
			}{
				{name: "otherwise successful children", secondData: []byte("{\"label\":\"pool-one\"}\n")},
				{name: "otherwise rejected child", secondData: []byte("not-json\n")},
			} {
				t.Run(test.name, func(t *testing.T) {
					runID := uuid.NewString()
					sourceID := uuid.NewString()
					data := map[string]any{
						"imports": []any{
							dataRunFusedImport(sourceID, scanRef, durabledata.AbsentHead(), []byte("{\"topic\":\"pool-one\"}\n")),
							dataRunFusedImport(sourceID, scoreRef, durabledata.AbsentHead(), test.secondData),
						},
						"pins": []any{},
					}
					response := rpcCall(t, handler, dataRunEventPublishBody(runID, uuid.NewString(), data))
					if response.Error == nil {
						t.Fatalf("duplicate child request succeeded: %#v", response)
					}
					if got := dataRunCount(t, fixture, "resource_run_creation_operations", "run_id", runID); got != 0 {
						t.Fatalf("run-creation receipts = %d, want 0", got)
					}
					if got := dataRunCount(t, fixture, "resource_source_invocations", "source_invocation_id", sourceID); got != 0 {
						t.Fatalf("source receipts = %d, want 0", got)
					}
					if got := dataRunCount(t, fixture, "runs", "run_id", runID); got != 0 {
						t.Fatalf("runs = %d, want 0", got)
					}
				})
			}
		})

		var ordinaryRunID string
		t.Run("ordinary declaration does not imply a pin", func(t *testing.T) {
			response := rpcCall(t, handler, dataRunEventPublishBody("", "ordinary-no-data", nil))
			if response.Error != nil {
				t.Fatalf("ordinary event.publish: %#v", response.Error)
			}
			result := asMap(t, response.Result)
			runID := stringValue(t, result["run_id"], "ordinary run_id")
			ordinaryRunID = runID
			if binding := asMap(t, result["data_binding"]); binding["state"] != "none" {
				t.Fatalf("ordinary data binding = %#v", binding)
			}
			receipt, err := fixture.reconstructed.LoadDataRunCreationOperation(ctx, runID)
			if err != nil || receipt.Summary.PinCount != 0 || receipt.Summary.ImportCount != 0 || receipt.Binding.State != "none" {
				t.Fatalf("ordinary reconstructed receipt = %#v, %v", receipt, err)
			}
		})

		t.Run("existing run rejects data before any mutation", func(t *testing.T) {
			for _, method := range []string{"run.start", "event.publish"} {
				t.Run(method, func(t *testing.T) {
					sourceID := uuid.NewString()
					data := map[string]any{
						"imports": []any{dataRunFusedImport(sourceID, scanRef, durabledata.AbsentHead(), []byte("{\"topic\":\"must-not-commit\"}\n"))},
						"pins":    []any{},
					}
					beforeEvents := dataRunCount(t, fixture, "events", "run_id", ordinaryRunID)
					beforeRuns := dataRunCount(t, fixture, "runs", "run_id", ordinaryRunID)
					beforeRunReceipts := dataRunCount(t, fixture, "resource_run_creation_operations", "run_id", ordinaryRunID)
					idempotencyKey := uuid.NewString()
					body := dataRunStartBody(ordinaryRunID, idempotencyKey, data)
					if method == "event.publish" {
						body = dataRunEventPublishBody(ordinaryRunID, idempotencyKey, data)
					}
					response := rpcCall(t, handler, body)
					if response.Error == nil || asMap(t, response.Error.Data)["code"] != string(durabledata.CodeRunDataImmutable) {
						t.Fatalf("existing-run data response = %#v, want %s", response, durabledata.CodeRunDataImmutable)
					}
					if got := dataRunCount(t, fixture, "events", "run_id", ordinaryRunID); got != beforeEvents {
						t.Fatalf("existing-run rejection event count = %d, want unchanged %d", got, beforeEvents)
					}
					if got := dataRunCount(t, fixture, "runs", "run_id", ordinaryRunID); got != beforeRuns {
						t.Fatalf("existing-run rejection run count = %d, want unchanged %d", got, beforeRuns)
					}
					if got := dataRunCount(t, fixture, "resource_run_creation_operations", "run_id", ordinaryRunID); got != beforeRunReceipts {
						t.Fatalf("existing-run rejection receipt count = %d, want unchanged %d", got, beforeRunReceipts)
					}
					if got := dataRunCount(t, fixture, "resource_source_invocations", "source_invocation_id", sourceID); got != 0 {
						t.Fatalf("existing-run rejection source receipts = %d, want 0", got)
					}
				})
			}
			snapshot, err := fixture.reconstructed.ShowDataResource(ctx, runStartTestBundleHash, scanRef)
			if err != nil || len(snapshot.Versions) != 0 || snapshot.Head.Before.State != "absent" {
				t.Fatalf("existing-run rejection resource snapshot = %#v, %v", snapshot, err)
			}
		})

		var firstVersion durabledata.VersionID
		var fusedRunID string
		t.Run("fused import commits and replays one atomic binding", func(t *testing.T) {
			runID := uuid.NewString()
			fusedRunID = runID
			sourceID := uuid.NewString()
			data := map[string]any{
				"imports": []any{dataRunFusedImport(sourceID, scanRef, durabledata.AbsentHead(), []byte("{\"topic\":\"pool-one\"}\n"))},
				"pins":    []any{},
			}
			body := dataRunEventPublishBody(runID, "fused-success", data)
			response := rpcCall(t, handler, body)
			if response.Error != nil {
				t.Fatalf("fused event.publish: %#v", response.Error)
			}
			result := asMap(t, response.Result)
			binding := asMap(t, result["data_binding"])
			if binding["state"] != "bound" || binding["pin_count"] != float64(1) || binding["import_count"] != float64(1) {
				t.Fatalf("fused data binding = %#v", binding)
			}
			sourceReceipt, err := fixture.reconstructed.LoadDataSourceOperation(ctx, sourceID)
			if err != nil || sourceReceipt.Result.Outcome != "accepted" {
				t.Fatalf("fused source receipt = %#v, %v", sourceReceipt, err)
			}
			firstVersion = sourceReceipt.Result.Candidate.VersionID
			parent, err := fixture.reconstructed.LoadDataRunCreationOperation(ctx, runID)
			if err != nil || parent.Summary.Outcome != "created" || parent.Summary.PinCount != 1 || len(parent.Evidence.RunBinding) != 2 {
				t.Fatalf("fused parent receipt = %#v, %v", parent, err)
			}
			pins, err := fixture.reconstructed.LoadDataPins(ctx, firstVersion)
			if err != nil || len(pins) != 1 || pins[0].RunID != runID || pins[0].Selection != "fused_import" {
				t.Fatalf("fused pins = %#v, %v", pins, err)
			}
			replay := rpcCall(t, handler, body)
			if replay.Error != nil || stringValue(t, asMap(t, replay.Result)["event_id"], "replay event_id") != stringValue(t, result["event_id"], "event_id") {
				t.Fatalf("fused replay = %#v", replay)
			}
		})

		t.Run("failed multi-import retains evidence and no child mutation", func(t *testing.T) {
			runID := uuid.NewString()
			readySourceID := uuid.NewString()
			rejectedSourceID := uuid.NewString()
			data := map[string]any{
				"imports": []any{
					dataRunFusedImport(readySourceID, scoreRef, durabledata.AbsentHead(), []byte("{\"label\":\"ready\"}\n")),
					dataRunFusedImport(rejectedSourceID, scanRef, durabledata.VersionHead(firstVersion), []byte("{}\n")),
				},
				"pins": []any{},
			}
			body := dataRunEventPublishBody(runID, "fused-rejected", data)
			response := rpcCall(t, handler, body)
			if response.Error == nil || asMap(t, response.Error.Data)["code"] != string(durabledata.CodeRunDataRejected) {
				t.Fatalf("failed fused response = %#v", response)
			}
			replay := rpcCall(t, handler, body)
			if replay.Error == nil || asMap(t, replay.Error.Data)["code"] != string(durabledata.CodeRunDataRejected) {
				t.Fatalf("failed fused replay = %#v", replay)
			}
			if got := dataRunCount(t, fixture, "runs", "run_id", runID); got != 0 {
				t.Fatalf("failed fused run count = %d", got)
			}
			if got := dataRunCount(t, fixture, "events", "run_id", runID); got != 0 {
				t.Fatalf("failed fused event count = %d", got)
			}
			parent, err := fixture.reconstructed.LoadDataRunCreationOperation(ctx, runID)
			if err != nil || parent.Summary.Outcome != "data_rejected" ||
				parent.Summary.Rejection.Code != durabledata.RunCreationRejectionFusedValidation ||
				len(parent.Evidence.ChildEvaluations) != 2 || parent.Summary.PinCount != 0 {
				t.Fatalf("failed fused parent = %#v, %v", parent, err)
			}
			if _, err := fixture.reconstructed.LoadDataSourceOperation(ctx, readySourceID); err == nil {
				t.Fatal("failed-parent ready child became a standalone source receipt")
			}
			snapshot, err := fixture.reconstructed.ShowDataResource(ctx, runStartTestBundleHash, scoreRef)
			if err != nil || len(snapshot.Versions) != 0 || snapshot.Head.Before.State != "absent" {
				t.Fatalf("failed child resource snapshot = %#v, %v", snapshot, err)
			}
			corruptRunCreationChildJSONColumn(t, ctx, fixture, "context_json", runID, rejectedSourceID, func(value map[string]any) {
				value["declaration"].(map[string]any)["business_key"] = "hostile"
			})
			var domainErr *durabledata.DomainError
			if _, err := fixture.reconstructed.LoadDataRunCreationOperation(ctx, runID); !errors.As(err, &domainErr) || domainErr.Code != durabledata.CodeIntegrity {
				t.Fatalf("corrupt failed-child context load error = %v, want %s", err, durabledata.CodeIntegrity)
			}
			if replay := rpcCall(t, handler, body); replay.Error == nil {
				t.Fatalf("corrupt failed-child context replay succeeded: %#v", replay)
			}
		})

		second, err := fixture.primary.ExecuteDataSourceOperation(ctx, durabledata.SourceCommand{
			Operation: "import", SourceInvocationID: uuid.NewString(), Actor: "operator", BundleHash: runStartTestBundleHash,
			Declaration: scanRef, ExpectedHead: durabledata.VersionHead(firstVersion), InputFormat: "jsonl", Input: []byte("{\"topic\":\"pool-two\"}\n"),
		})
		if err != nil || second.Outcome != "accepted" {
			t.Fatalf("second standalone import = %#v, %v", second, err)
		}

		t.Run("failed head conflict replays its permanent outcome", func(t *testing.T) {
			runID := uuid.NewString()
			data := map[string]any{
				"imports": []any{dataRunFusedImport(uuid.NewString(), scanRef, durabledata.VersionHead(firstVersion), []byte("{\"topic\":\"stale\"}\n"))},
				"pins":    []any{},
			}
			body := dataRunEventPublishBody(runID, "fused-head-conflict", data)
			for attempt := 0; attempt < 2; attempt++ {
				response := rpcCall(t, handler, body)
				if response.Error == nil || asMap(t, response.Error.Data)["code"] != string(durabledata.CodeRunHeadConflict) {
					t.Fatalf("head-conflict attempt %d = %#v", attempt+1, response)
				}
			}
			parent, err := fixture.reconstructed.LoadDataRunCreationOperation(ctx, runID)
			if err != nil || parent.Summary.Outcome != "head_conflict" ||
				parent.Summary.Rejection.Code != durabledata.RunCreationRejectionFusedHead || parent.Binding.State != "none" {
				t.Fatalf("head-conflict receipt = %#v, %v", parent, err)
			}
			if got := dataRunCount(t, fixture, "runs", "run_id", runID); got != 0 {
				t.Fatalf("head-conflict run count = %d", got)
			}
		})

		t.Run("missing explicit pin retains typed rejection on replay", func(t *testing.T) {
			runID := uuid.NewString()
			missingVersion := durabledata.VersionID("resource-version-v1:sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")
			data := map[string]any{
				"imports": []any{},
				"pins": []any{map[string]any{
					"declaration": dataRunDeclaration(scanRef), "version_id": missingVersion,
				}},
			}
			body := dataRunEventPublishBody(runID, "explicit-missing", data)
			for attempt := 0; attempt < 2; attempt++ {
				response := rpcCall(t, handler, body)
				if response.Error == nil || asMap(t, response.Error.Data)["code"] != string(durabledata.CodeRunDataRejected) {
					t.Fatalf("missing explicit pin attempt %d = %#v", attempt+1, response)
				}
				details := asMap(t, asMap(t, response.Error.Data)["details"])
				rejection := asMap(t, asMap(t, details["operation"])["rejection"])
				if rejection["state"] != "rejected" || rejection["code"] != durabledata.RunCreationRejectionVersionMissing ||
					rejection["version_id"] != string(missingVersion) {
					t.Fatalf("missing explicit pin rejection = %#v", rejection)
				}
			}
			parent, err := fixture.reconstructed.LoadDataRunCreationOperation(ctx, runID)
			if err != nil || parent.Summary.Rejection.Code != durabledata.RunCreationRejectionVersionMissing ||
				parent.Summary.Rejection.Declaration == nil || *parent.Summary.Rejection.Declaration != scanRef ||
				parent.Summary.Rejection.VersionID != missingVersion {
				t.Fatalf("missing explicit pin receipt = %#v, %v", parent, err)
			}
			selectSummary := `SELECT summary_json FROM resource_run_creation_operations WHERE run_id = ?`
			updateSummary := `UPDATE resource_run_creation_operations SET summary_json = ? WHERE run_id = ?`
			if _, ok := fixture.primary.(*store.PostgresStore); ok {
				selectSummary = `SELECT summary_json FROM resource_run_creation_operations WHERE run_id = $1::uuid`
				updateSummary = `UPDATE resource_run_creation_operations SET summary_json = $1 WHERE run_id = $2::uuid`
			}
			var raw []byte
			if err := fixture.db.QueryRowContext(ctx, selectSummary, runID).Scan(&raw); err != nil {
				t.Fatal(err)
			}
			var summary map[string]any
			if err := json.Unmarshal(raw, &summary); err != nil {
				t.Fatal(err)
			}
			delete(summary, "rejection")
			corrupt, err := json.Marshal(summary)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := fixture.db.ExecContext(ctx, updateSummary, corrupt, runID); err != nil {
				t.Fatal(err)
			}
			var domainErr *durabledata.DomainError
			if _, err := fixture.reconstructed.LoadDataRunCreationOperation(ctx, runID); !errors.As(err, &domainErr) || domainErr.Code != durabledata.CodeIntegrity {
				t.Fatalf("missing typed rejection corruption error = %v, want %s", err, durabledata.CodeIntegrity)
			}
		})

		var scoreHead durabledata.ExpectedHead
		t.Run("pruned explicit pin retains typed rejection on replay", func(t *testing.T) {
			firstScore, err := fixture.primary.ExecuteDataSourceOperation(ctx, durabledata.SourceCommand{
				Operation: "import", SourceInvocationID: uuid.NewString(), Actor: "operator", BundleHash: runStartTestBundleHash,
				Declaration: scoreRef, ExpectedHead: durabledata.AbsentHead(), InputFormat: "jsonl", Input: []byte("{\"label\":\"first\"}\n"),
			})
			if err != nil || firstScore.Outcome != "accepted" {
				t.Fatalf("first score import = %#v, %v", firstScore, err)
			}
			secondScore, err := fixture.primary.ExecuteDataSourceOperation(ctx, durabledata.SourceCommand{
				Operation: "import", SourceInvocationID: uuid.NewString(), Actor: "operator", BundleHash: runStartTestBundleHash,
				Declaration: scoreRef, ExpectedHead: firstScore.Head.After, InputFormat: "jsonl", Input: []byte("{\"label\":\"second\"}\n"),
			})
			if err != nil || secondScore.Outcome != "accepted" {
				t.Fatalf("second score import = %#v, %v", secondScore, err)
			}
			scoreHead = secondScore.Head.After
			pruned, err := fixture.primary.PruneDataResource(ctx, durabledata.PruneCommand{
				PruneInvocationID: uuid.NewString(), Actor: "operator", Declaration: scoreRef,
				VersionID: firstScore.Candidate.VersionID, ExpectedHead: scoreHead,
			})
			if err != nil || pruned.Outcome != "pruned" {
				t.Fatalf("prune score version = %#v, %v", pruned, err)
			}

			runID := uuid.NewString()
			data := map[string]any{
				"imports": []any{},
				"pins": []any{map[string]any{
					"declaration": dataRunDeclaration(scoreRef), "version_id": firstScore.Candidate.VersionID,
				}},
			}
			body := dataRunEventPublishBody(runID, "explicit-pruned", data)
			for attempt := 0; attempt < 2; attempt++ {
				response := rpcCall(t, handler, body)
				if response.Error == nil || asMap(t, response.Error.Data)["code"] != string(durabledata.CodeRunDataRejected) {
					t.Fatalf("pruned explicit pin attempt %d = %#v", attempt+1, response)
				}
			}
			parent, err := fixture.reconstructed.LoadDataRunCreationOperation(ctx, runID)
			if err != nil || parent.Summary.Rejection.Code != durabledata.RunCreationRejectionVersionPruned ||
				parent.Summary.Rejection.Declaration == nil || *parent.Summary.Rejection.Declaration != scoreRef ||
				parent.Summary.Rejection.VersionID != firstScore.Candidate.VersionID {
				t.Fatalf("pruned explicit pin receipt = %#v, %v", parent, err)
			}
		})

		t.Run("fork pin owner preserves exact selected subset and overrides", func(t *testing.T) {
			ctx := testAuthorActivityContext(context.Background())
			newTarget := func() string {
				runID := uuid.NewString()
				storetest.RequireRun(t, ctx, fixture.primary, storetest.RunFixture{
					RunID: runID, State: runtimerunlifecycle.StateRunning, Origin: storetest.ScenarioSetupOrigin(),
					BundleHash: runStartTestBundleHash, BundleSource: runtimerunlifecycle.BundleSourceEphemeral,
				})
				return runID
			}

			zeroTarget := newTarget()
			zeroPins, err := storetest.MaterializeDataForkPins(ctx, fixture.primary, ordinaryRunID, zeroTarget, runStartTestBundleHash, nil, false)
			if err != nil || len(zeroPins) != 0 {
				t.Fatalf("zero-pin fork projection = %#v, %v", zeroPins, err)
			}

			inheritedTarget := newTarget()
			inherited, err := storetest.MaterializeDataForkPins(ctx, fixture.primary, fusedRunID, inheritedTarget, runStartTestBundleHash, nil, false)
			if err != nil || len(inherited) != 1 || inherited[0].Declaration != scanRef || inherited[0].VersionID != firstVersion || inherited[0].Selection != "fork_inherited" {
				t.Fatalf("inherited fork pins = %#v, %v", inherited, err)
			}
			replayed, err := storetest.MaterializeDataForkPins(ctx, fixture.reconstructed, fusedRunID, inheritedTarget, runStartTestBundleHash, nil, true)
			if err != nil || len(replayed) != 1 || replayed[0].VersionID != firstVersion {
				t.Fatalf("reconstructed fork pin replay = %#v, %v", replayed, err)
			}

			override := []durabledata.ExplicitPin{{Declaration: scanRef, VersionID: second.Candidate.VersionID}}
			overrideTarget := newTarget()
			overridden, err := storetest.MaterializeDataForkPins(ctx, fixture.primary, fusedRunID, overrideTarget, runStartTestBundleHash, override, false)
			if err != nil || len(overridden) != 1 || overridden[0].VersionID != second.Candidate.VersionID || overridden[0].Selection != "fork_override" {
				t.Fatalf("override fork pins = %#v, %v", overridden, err)
			}
			if _, err := storetest.MaterializeDataForkPins(ctx, fixture.primary, fusedRunID, overrideTarget, runStartTestBundleHash, nil, true); err == nil {
				t.Fatal("fork pin replay accepted a changed override set")
			}

			duplicateTarget := newTarget()
			if _, err := storetest.MaterializeDataForkPins(ctx, fixture.primary, fusedRunID, duplicateTarget, runStartTestBundleHash, append(override, override...), false); err == nil {
				t.Fatal("fork pin owner accepted duplicate overrides")
			}

			missingCatalog := durabledata.Catalog{BundleHash: dataRunMissingBundleHash, Declarations: []durabledata.Declaration{catalog.Declarations[1]}}
			if err := registerDataRunLifecycleCatalog(ctx, fixture.primary, missingCatalog); err != nil {
				t.Fatalf("register target catalog without source pin declaration: %v", err)
			}
			missingTarget := uuid.NewString()
			storetest.RequireRun(t, ctx, fixture.primary, storetest.RunFixture{
				RunID: missingTarget, State: runtimerunlifecycle.StateRunning, Origin: storetest.ScenarioSetupOrigin(),
				BundleHash: dataRunMissingBundleHash, BundleSource: runtimerunlifecycle.BundleSourceEphemeral,
			})
			_, err = storetest.MaterializeDataForkPins(ctx, fixture.primary, fusedRunID, missingTarget, dataRunMissingBundleHash, nil, false)
			var domainErr *durabledata.DomainError
			if !errors.As(err, &domainErr) || domainErr.Code != durabledata.CodePinConflict {
				t.Fatalf("target dropped pinned declaration error = %v", err)
			}
		})

		t.Run("explicit historical pin survives restart and terminal retention", func(t *testing.T) {
			runID := uuid.NewString()
			data := map[string]any{
				"imports": []any{},
				"pins": []any{map[string]any{
					"declaration": dataRunDeclaration(scanRef), "version_id": firstVersion,
				}},
			}
			response := rpcCall(t, handler, dataRunEventPublishBody(runID, "explicit-history", data))
			if response.Error != nil {
				t.Fatalf("explicit historical event.publish: %#v", response.Error)
			}
			parent, err := fixture.reconstructed.LoadDataRunCreationOperation(ctx, runID)
			if err != nil || parent.Summary.PinCount != 1 || parent.Evidence.RunBinding[0].Pin == nil || parent.Evidence.RunBinding[0].Pin.Selection != "explicit" {
				t.Fatalf("explicit historical receipt = %#v, %v", parent, err)
			}
			refused, err := fixture.reconstructed.PruneDataResource(ctx, durabledata.PruneCommand{
				PruneInvocationID: uuid.NewString(), Actor: "operator", Declaration: scanRef,
				VersionID: firstVersion, ExpectedHead: second.Head.After,
			})
			if err != nil || refused.Outcome != "refused_pinned" || refused.PinCount < 2 {
				t.Fatalf("active pin prune = %#v, %v", refused, err)
			}
			if _, _, err := storetest.TerminalizeRun(testAuthorActivityContext(ctx), fixture.primary, runtimerunlifecycle.TerminalRequest{
				RunID: runID, State: runtimerunlifecycle.StateCancelled, EndedAt: time.Now().UTC(),
			}); err != nil {
				t.Fatalf("terminalize pinned run: %v", err)
			}
			pins, err := fixture.reconstructed.LoadDataPins(ctx, firstVersion)
			if err != nil || !dataRunHasPin(pins, runID, "cancelled") {
				t.Fatalf("terminal reconstructed pins = %#v, %v", pins, err)
			}
			refused, err = fixture.reconstructed.PruneDataResource(ctx, durabledata.PruneCommand{
				PruneInvocationID: uuid.NewString(), Actor: "operator", Declaration: scanRef,
				VersionID: firstVersion, ExpectedHead: second.Head.After,
			})
			if err != nil || refused.Outcome != "refused_pinned" || refused.Pins == nil || !dataRunHasPin(refused.Pins.Items, runID, "cancelled") {
				t.Fatalf("terminal forkable pin prune = %#v, %v", refused, err)
			}
		})

		t.Run("same declaration schema amendment is not comparable", func(t *testing.T) {
			amended, _, _ := dataRunLifecycleCatalog(t, dataRunSchemaBundleHash, true)
			if err := registerDataRunLifecycleCatalog(ctx, fixture.primary, amended); err != nil {
				t.Fatalf("register amended catalog: %v", err)
			}
			amendedSource := semanticview.Wrap(runStartTestBundle("scan.requested"))
			amendedBus, err := newScopedAPITestEventBus(t, fixture.primary, runtimebus.EventBusOptions{
				ContractBundle: amendedSource, BundleSourceFact: mustAPITestBundleSourceFact(dataRunSchemaBundleHash),
			})
			if err != nil {
				t.Fatalf("new amended data EventBus: %v", err)
			}
			if err := registerDataRunLifecycleCatalog(ctx, fixture.primary, amended); err != nil {
				t.Fatalf("restore amended catalog: %v", err)
			}
			amendedHandler := eventPublishTestHandlerWithStores(t, fixture.primary, fixture.primary, fixture.primary, amendedBus, amendedSource)
			mismatchRunID := uuid.NewString()
			mismatchData := map[string]any{
				"imports": []any{},
				"pins": []any{map[string]any{
					"declaration": dataRunDeclaration(scanRef), "version_id": firstVersion,
				}},
			}
			mismatchBody := dataRunEventPublishBodyForBundle(mismatchRunID, "explicit-schema-mismatch", dataRunSchemaBundleHash, mismatchData)
			for attempt := 0; attempt < 2; attempt++ {
				response := rpcCall(t, amendedHandler, mismatchBody)
				if response.Error == nil || asMap(t, response.Error.Data)["code"] != string(durabledata.CodeRunDataRejected) {
					t.Fatalf("schema-mismatch explicit pin attempt %d = %#v", attempt+1, response)
				}
			}
			mismatchReceipt, err := fixture.reconstructed.LoadDataRunCreationOperation(ctx, mismatchRunID)
			if err != nil || mismatchReceipt.Summary.Rejection.Code != durabledata.RunCreationRejectionSchemaMismatch ||
				mismatchReceipt.Summary.Rejection.Declaration == nil || *mismatchReceipt.Summary.Rejection.Declaration != scanRef ||
				mismatchReceipt.Summary.Rejection.VersionID != firstVersion ||
				mismatchReceipt.Summary.Rejection.ExpectedSchemaDigest != amended.Declarations[0].SchemaDigest ||
				mismatchReceipt.Summary.Rejection.SelectedSchemaDigest == amended.Declarations[0].SchemaDigest {
				t.Fatalf("schema-mismatch explicit pin receipt = %#v, %v", mismatchReceipt, err)
			}
			result, err := fixture.primary.ExecuteDataSourceOperation(ctx, durabledata.SourceCommand{
				Operation: "import", SourceInvocationID: uuid.NewString(), Actor: "operator", BundleHash: dataRunSchemaBundleHash,
				Declaration: scanRef, ExpectedHead: second.Head.After, InputFormat: "jsonl", Input: []byte("{\"stage\":\"new\",\"topic\":\"pool-three\"}\n"),
			})
			if err != nil || result.Outcome != "accepted" || result.Delta.State != "not_comparable" || result.Delta.Reason != "schema_changed" {
				t.Fatalf("schema amendment result = %#v, %v", result, err)
			}
			snapshot, err := fixture.reconstructed.ShowDataResource(ctx, dataRunSchemaBundleHash, scanRef)
			if err != nil || len(snapshot.Versions) != 3 || snapshot.Versions[0].VersionID != firstVersion || snapshot.Versions[0].Manifest.SchemaDigest == result.SchemaDigest {
				t.Fatalf("schema amendment history = %#v, %v", snapshot, err)
			}

			ctx := testAuthorActivityContext(context.Background())
			incompatibleTarget := uuid.NewString()
			storetest.RequireRun(t, ctx, fixture.primary, storetest.RunFixture{
				RunID: incompatibleTarget, State: runtimerunlifecycle.StateRunning, Origin: storetest.ScenarioSetupOrigin(),
				BundleHash: dataRunSchemaBundleHash, BundleSource: runtimerunlifecycle.BundleSourceEphemeral,
			})
			_, err = storetest.MaterializeDataForkPins(ctx, fixture.primary, fusedRunID, incompatibleTarget, dataRunSchemaBundleHash, nil, false)
			var domainErr *durabledata.DomainError
			if !errors.As(err, &domainErr) || domainErr.Code != durabledata.CodeSchemaMismatch {
				t.Fatalf("schema-incompatible inherited fork error = %v", err)
			}
			if pins, loadErr := fixture.reconstructed.LoadDataPins(ctx, firstVersion); loadErr != nil || dataRunHasPin(pins, incompatibleTarget, "running") || dataRunHasPin(pins, incompatibleTarget, "paused") {
				t.Fatalf("schema-incompatible fork left pins = %#v, %v", pins, loadErr)
			}
			overrideTarget := uuid.NewString()
			storetest.RequireRun(t, ctx, fixture.primary, storetest.RunFixture{
				RunID: overrideTarget, State: runtimerunlifecycle.StateRunning, Origin: storetest.ScenarioSetupOrigin(),
				BundleHash: dataRunSchemaBundleHash, BundleSource: runtimerunlifecycle.BundleSourceEphemeral,
			})
			pins, err := storetest.MaterializeDataForkPins(ctx, fixture.primary, fusedRunID, overrideTarget, dataRunSchemaBundleHash, []durabledata.ExplicitPin{{
				Declaration: scanRef, VersionID: result.Candidate.VersionID,
			}}, false)
			if err != nil || len(pins) != 1 || pins[0].Selection != "fork_override" || pins[0].VersionID != result.Candidate.VersionID {
				t.Fatalf("schema-amended fork override = %#v, %v", pins, err)
			}
		})

		t.Run("empty fused JSONL commits a zero-row version", func(t *testing.T) {
			runID := uuid.NewString()
			sourceID := uuid.NewString()
			data := map[string]any{
				"imports": []any{dataRunFusedImport(sourceID, scoreRef, scoreHead, nil)},
				"pins":    []any{},
			}
			response := rpcCall(t, handler, dataRunEventPublishBody(runID, "empty-fused-jsonl", data))
			if response.Error != nil {
				t.Fatalf("empty fused event.publish: %#v", response.Error)
			}
			receipt, err := fixture.reconstructed.LoadDataSourceOperation(ctx, sourceID)
			if err != nil || receipt.Result.Outcome != "accepted" || receipt.Result.Candidate.State != "version" ||
				receipt.Result.Candidate.Alias != "v3" || receipt.Result.Candidate.Manifest == nil ||
				receipt.Result.Candidate.Manifest.RowCount != 0 {
				t.Fatalf("empty fused source receipt = %#v, %v", receipt, err)
			}
			snapshot, err := fixture.reconstructed.ShowDataResource(ctx, runStartTestBundleHash, scoreRef)
			if err != nil {
				t.Fatalf("empty fused resource snapshot: %v", err)
			}
			found := false
			for _, version := range snapshot.Versions {
				if version.VersionID == receipt.Result.Candidate.VersionID {
					found = true
					if version.Manifest.RowCount != 0 || len(version.CanonicalJSONL) != 0 {
						t.Fatalf("empty fused committed version = %#v", version)
					}
				}
			}
			if !found {
				t.Fatalf("empty fused version %s absent from snapshot", receipt.Result.Candidate.VersionID)
			}
		})
	})
}

func TestRunCreationReceiptRejectsAggregateCorruptionAcrossSelectedStores(t *testing.T) {
	tests := []struct {
		name   string
		column string
		mutate func(map[string]any)
	}{
		{name: "summary kind", column: "summary_json", mutate: func(value map[string]any) { value["kind"] = "event_publish" }},
		{name: "summary counts", column: "summary_json", mutate: func(value map[string]any) { value["pin_count"] = float64(0) }},
		{name: "summary outcome union", column: "summary_json", mutate: func(value map[string]any) { value["outcome"] = "data_rejected" }},
		{name: "summary event identity", column: "summary_json", mutate: func(value map[string]any) { value["event_id"] = uuid.NewString() }},
		{name: "summary completion", column: "summary_json", mutate: func(value map[string]any) { value["completed_at"] = "0001-01-01T00:00:00Z" }},
		{name: "binding tagged union", column: "binding_json", mutate: func(value map[string]any) { value["run_id"] = uuid.NewString() }},
		{name: "binding evidence", column: "evidence_json", mutate: func(value map[string]any) {
			value["run_binding"] = []any{map[string]any{"kind": "pin"}}
		}},
		{name: "request hash agreement", column: "request_json", mutate: func(value map[string]any) { value["event_id"] = uuid.NewString() }},
	}
	forEachDataRunLifecycleStore(t, func(t *testing.T, fixture dataRunLifecycleFixture) {
		ctx := context.Background()
		catalog, scanRef, _ := dataRunLifecycleCatalog(t, runStartTestBundleHash, false)
		if err := registerDataRunLifecycleCatalog(ctx, fixture.primary, catalog); err != nil {
			t.Fatal(err)
		}
		source := semanticview.Wrap(runStartTestBundle("scan.requested"))
		bus, err := newScopedAPITestEventBus(t, fixture.primary, runStartTestEventBusOptions(source))
		if err != nil {
			t.Fatal(err)
		}
		if err := registerDataRunLifecycleCatalog(ctx, fixture.primary, catalog); err != nil {
			t.Fatal(err)
		}
		handler := eventPublishTestHandlerWithStores(t, fixture.primary, fixture.primary, fixture.primary, bus, source)
		t.Run("pinless summary status corruption fails load and replay", func(t *testing.T) {
			runID := uuid.NewString()
			emptyData := map[string]any{"imports": []any{}, "pins": []any{}}
			body := dataRunEventPublishBody(runID, uuid.NewString(), emptyData)
			if response := rpcCall(t, handler, body); response.Error != nil {
				t.Fatalf("pinless run fixture: %#v", response.Error)
			}
			waitCtx, cancelWait := context.WithTimeout(ctx, 10*time.Second)
			if err := bus.WaitForQuiescence(waitCtx); err != nil {
				cancelWait()
				t.Fatalf("wait for pinless run fixture settlement: %v", err)
			}
			cancelWait()
			corruptRunCreationJSONColumn(t, ctx, fixture, "summary_json", runID, func(value map[string]any) {
				value["status"] = "bogus"
			})
			var domainErr *durabledata.DomainError
			if _, err := fixture.reconstructed.LoadDataRunCreationOperation(ctx, runID); !errors.As(err, &domainErr) || domainErr.Code != durabledata.CodeIntegrity {
				t.Fatalf("corrupt pinless status load error = %v, want %s", err, durabledata.CodeIntegrity)
			}
			if replay := rpcCall(t, handler, dataRunEventPublishBody(runID, uuid.NewString(), emptyData)); replay.Error == nil {
				t.Fatalf("corrupt pinless status replay succeeded: %#v", replay)
			}
		})
		expectedHead := durabledata.AbsentHead()
		for index, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				runID := uuid.NewString()
				data := map[string]any{
					"imports": []any{dataRunFusedImport(uuid.NewString(), scanRef, expectedHead,
						[]byte(fmt.Sprintf("{\"topic\":\"corruption-%d\"}\n", index)))},
					"pins": []any{},
				}
				body := dataRunEventPublishBody(runID, uuid.NewString(), data)
				if response := rpcCall(t, handler, body); response.Error != nil {
					t.Fatalf("run fixture: %#v", response.Error)
				}
				waitCtx, cancelWait := context.WithTimeout(ctx, 10*time.Second)
				if err := bus.WaitForQuiescence(waitCtx); err != nil {
					cancelWait()
					t.Fatalf("wait for run fixture settlement: %v", err)
				}
				cancelWait()
				snapshot, err := fixture.primary.ShowDataResource(ctx, runStartTestBundleHash, scanRef)
				if err != nil {
					t.Fatal(err)
				}
				expectedHead = snapshot.Head.After
				corruptRunCreationJSONColumn(t, ctx, fixture, test.column, runID, test.mutate)
				var domainErr *durabledata.DomainError
				if _, err := fixture.reconstructed.LoadDataRunCreationOperation(ctx, runID); !errors.As(err, &domainErr) || domainErr.Code != durabledata.CodeIntegrity {
					t.Fatalf("corrupt %s load error = %v, want %s", test.name, err, durabledata.CodeIntegrity)
				}
				replay := rpcCall(t, handler, dataRunEventPublishBody(runID, uuid.NewString(), data))
				if replay.Error == nil {
					t.Fatalf("corrupt %s replay succeeded: %#v", test.name, replay)
				}
			})
		}
	})
}

func TestDataShowPinCursorSurvivesConcurrentRunCreationAcrossSelectedStores(t *testing.T) {
	forEachDataRunLifecycleStore(t, func(t *testing.T, fixture dataRunLifecycleFixture) {
		ctx := context.Background()
		catalog, scanRef, _ := dataRunLifecycleCatalog(t, runStartTestBundleHash, false)
		if err := registerDataRunLifecycleCatalog(ctx, fixture.primary, catalog); err != nil {
			t.Fatal(err)
		}
		source := semanticview.Wrap(runStartTestBundle("scan.requested"))
		bus, err := newScopedAPITestEventBus(t, fixture.primary, runStartTestEventBusOptions(source))
		if err != nil {
			t.Fatal(err)
		}
		if err := registerDataRunLifecycleCatalog(ctx, fixture.primary, catalog); err != nil {
			t.Fatal(err)
		}
		publish := eventPublishTestHandlerWithStores(t, fixture.primary, fixture.primary, fixture.primary, bus, source)
		imported, err := fixture.primary.ExecuteDataSourceOperation(ctx, durabledata.SourceCommand{
			Operation: "import", SourceInvocationID: uuid.NewString(), Actor: "operator", BundleHash: runStartTestBundleHash,
			Declaration: scanRef, ExpectedHead: durabledata.AbsentHead(), InputFormat: "jsonl", Input: []byte("{\"topic\":\"pin-page\"}\n"),
		})
		if err != nil || imported.Outcome != "accepted" {
			t.Fatalf("pin-page import = %#v, %v", imported, err)
		}
		versionID := imported.Candidate.VersionID
		createPin := func(runID string) {
			t.Helper()
			data := map[string]any{
				"imports": []any{},
				"pins": []any{map[string]any{
					"declaration": dataRunDeclaration(scanRef), "version_id": versionID,
				}},
			}
			if response := rpcCall(t, publish, dataRunEventPublishBody(runID, uuid.NewString(), data)); response.Error != nil {
				t.Fatalf("create pin for run %s: %#v", runID, response.Error)
			}
		}
		runID := func(suffix int) string { return fmt.Sprintf("00000000-0000-4000-8000-%012d", suffix) }
		createPin(runID(10))
		createPin(runID(20))
		createPin(runID(30))

		show := OperatorDataHandlers(DataHandlerOptions{Store: fixture.reconstructed})["data.show"]
		params := map[string]any{
			"view": "pins", "declaration": dataRunDeclaration(scanRef),
			"selector": map[string]any{"kind": "version", "version_id": string(versionID)},
			"page":     map[string]any{"limit": 1, "byte_limit": durabledata.MaxPublicPageBytes},
		}
		first, err := show(ctx, Request{Method: "data.show", Params: params})
		if err != nil {
			t.Fatal(err)
		}
		firstPage := first.(durabledata.PageResult[durabledata.Pin])
		if len(firstPage.Items) != 1 || firstPage.Items[0].RunID != runID(10) || firstPage.Continuation.State != "more" {
			t.Fatalf("first persisted pin page = %#v", firstPage)
		}

		createPin(runID(5))
		createPin(runID(15))
		params["page"] = map[string]any{
			"limit": 10, "byte_limit": durabledata.MaxPublicPageBytes, "cursor": firstPage.Continuation.Cursor,
		}
		second, err := show(ctx, Request{Method: "data.show", Params: params})
		if err != nil {
			t.Fatal(err)
		}
		secondPage := second.(durabledata.PageResult[durabledata.Pin])
		want := []string{runID(15), runID(20), runID(30)}
		if len(secondPage.Items) != len(want) || secondPage.Continuation.State != "end" {
			t.Fatalf("second persisted pin page = %#v", secondPage)
		}
		for index := range want {
			if secondPage.Items[index].RunID != want[index] {
				t.Fatalf("second persisted pin page[%d] = %s, want %s", index, secondPage.Items[index].RunID, want[index])
			}
		}
	})
}

func TestDataShowProvenanceCursorSurvivesConcurrentProducerAcrossSelectedStores(t *testing.T) {
	forEachDataRunLifecycleStore(t, func(t *testing.T, fixture dataRunLifecycleFixture) {
		ctx := context.Background()
		catalog, scanRef, _ := dataRunLifecycleCatalog(t, runStartTestBundleHash, false)
		if err := registerDataRunLifecycleCatalog(ctx, fixture.primary, catalog); err != nil {
			t.Fatal(err)
		}
		input := []byte("{\"topic\":\"provenance-page\"}\n")
		head := durabledata.AbsentHead()
		importSame := func(id string) durabledata.SourceOperationResult {
			t.Helper()
			result, err := fixture.primary.ExecuteDataSourceOperation(ctx, durabledata.SourceCommand{
				Operation: "import", SourceInvocationID: id, Actor: "operator", BundleHash: runStartTestBundleHash,
				Declaration: scanRef, ExpectedHead: head, InputFormat: "jsonl", Input: input,
			})
			if err != nil || result.Outcome != "accepted" {
				t.Fatalf("equal-content import = %#v, %v", result, err)
			}
			head = result.Head.After
			return result
		}
		firstID, secondID, thirdID := uuid.NewString(), uuid.NewString(), uuid.NewString()
		firstImport := importSame(firstID)
		importSame(secondID)
		show := OperatorDataHandlers(DataHandlerOptions{Store: fixture.reconstructed})["data.show"]
		params := map[string]any{
			"view": "provenance", "declaration": dataRunDeclaration(scanRef),
			"selector": map[string]any{"kind": "version", "version_id": string(firstImport.Candidate.VersionID)},
			"page":     map[string]any{"limit": 1, "byte_limit": durabledata.MaxPublicPageBytes},
		}
		first, err := show(ctx, Request{Method: "data.show", Params: params})
		if err != nil {
			t.Fatal(err)
		}
		firstPage := first.(durabledata.PageResult[durabledata.Provenance])
		if len(firstPage.Items) != 1 || firstPage.Items[0].ProducerRef.SourceInvocationID != firstID || firstPage.Continuation.State != "more" {
			t.Fatalf("first persisted provenance page = %#v", firstPage)
		}
		importSame(thirdID)
		params["page"] = map[string]any{
			"limit": 10, "byte_limit": durabledata.MaxPublicPageBytes, "cursor": firstPage.Continuation.Cursor,
		}
		second, err := show(ctx, Request{Method: "data.show", Params: params})
		if err != nil {
			t.Fatal(err)
		}
		secondPage := second.(durabledata.PageResult[durabledata.Provenance])
		if len(secondPage.Items) != 2 || secondPage.Items[0].ProducerRef.SourceInvocationID != secondID ||
			secondPage.Items[1].ProducerRef.SourceInvocationID != thirdID || secondPage.Continuation.State != "end" {
			t.Fatalf("continued persisted provenance page = %#v", secondPage)
		}
	})
}

func TestFusedRunBindingRejectsCorruptCanonicalSourceEvaluationAcrossSelectedStores(t *testing.T) {
	forEachDataRunLifecycleStore(t, func(t *testing.T, fixture dataRunLifecycleFixture) {
		ctx := context.Background()
		catalog, scanRef, _ := dataRunLifecycleCatalog(t, runStartTestBundleHash, false)
		if err := registerDataRunLifecycleCatalog(ctx, fixture.primary, catalog); err != nil {
			t.Fatal(err)
		}
		source := semanticview.Wrap(runStartTestBundle("scan.requested"))
		bus, err := newScopedAPITestEventBus(t, fixture.primary, runStartTestEventBusOptions(source))
		if err != nil {
			t.Fatal(err)
		}
		publish := eventPublishTestHandlerWithStores(t, fixture.primary, fixture.primary, fixture.primary, bus, source)
		runID, sourceID := uuid.NewString(), uuid.NewString()
		data := map[string]any{
			"imports": []any{dataRunFusedImport(sourceID, scanRef, durabledata.AbsentHead(), []byte("{\"topic\":\"bound\"}\n"))},
			"pins":    []any{},
		}
		body := dataRunEventPublishBody(runID, uuid.NewString(), data)
		if response := rpcCall(t, publish, body); response.Error != nil {
			t.Fatalf("fused run fixture: %#v", response.Error)
		}
		quiescenceCtx, cancelQuiescence := context.WithTimeout(ctx, 30*time.Second)
		defer cancelQuiescence()
		if err := bus.WaitForQuiescence(quiescenceCtx); err != nil {
			t.Fatalf("wait for fused publication settlement: %v", err)
		}
		requestQuery := `SELECT request_json FROM resource_source_invocations WHERE source_invocation_id = ?`
		if _, ok := fixture.primary.(*store.PostgresStore); ok {
			requestQuery = `SELECT request_json FROM resource_source_invocations WHERE source_invocation_id = $1::uuid`
		}
		var requestJSON []byte
		if err := fixture.db.QueryRowContext(ctx, requestQuery, sourceID).Scan(&requestJSON); err != nil {
			t.Fatal(err)
		}
		var exactCommand durabledata.SourceCommand
		if err := json.Unmarshal(requestJSON, &exactCommand); err != nil {
			t.Fatal(err)
		}
		corruptSourceOperationJSONColumn(t, ctx, fixture, "evaluation_json", sourceID, func(value map[string]any) {
			value["declaration"].(map[string]any)["business_key"] = "hostile"
		})
		var domainErr *durabledata.DomainError
		if _, err := fixture.reconstructed.LoadDataRunCreationOperation(ctx, runID); !errors.As(err, &domainErr) || domainErr.Code != durabledata.CodeIntegrity {
			t.Fatalf("corrupt fused source load error = %v, want %s", err, durabledata.CodeIntegrity)
		}
		_, err = fixture.reconstructed.ExecuteDataSourceOperation(ctx, exactCommand)
		if !errors.As(err, &domainErr) || domainErr.Code != durabledata.CodeIntegrity {
			t.Fatalf("corrupt fused source exact replay error = %v, want %s", err, durabledata.CodeIntegrity)
		}
	})
}

func corruptRunCreationJSONColumn(t testing.TB, ctx context.Context, fixture dataRunLifecycleFixture, column, runID string, mutate func(map[string]any)) {
	t.Helper()
	query := fmt.Sprintf("SELECT %s FROM resource_run_creation_operations WHERE run_id = ?", column)
	update := fmt.Sprintf("UPDATE resource_run_creation_operations SET %s = ? WHERE run_id = ?", column)
	if _, ok := fixture.primary.(*store.PostgresStore); ok {
		query = fmt.Sprintf("SELECT %s FROM resource_run_creation_operations WHERE run_id = $1::uuid", column)
		update = fmt.Sprintf("UPDATE resource_run_creation_operations SET %s = $1 WHERE run_id = $2::uuid", column)
	}
	var raw []byte
	retryRunCreationHostileSQL(t, fixture, func() error {
		return fixture.db.QueryRowContext(ctx, query, runID).Scan(&raw)
	})
	var value map[string]any
	if err := json.Unmarshal(raw, &value); err != nil {
		t.Fatal(err)
	}
	mutate(value)
	corrupt, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	retryRunCreationHostileSQL(t, fixture, func() error {
		_, err := fixture.db.ExecContext(ctx, update, corrupt, runID)
		return err
	})
}

func corruptRunCreationChildJSONColumn(t testing.TB, ctx context.Context, fixture dataRunLifecycleFixture, column, runID, sourceID string, mutate func(map[string]any)) {
	t.Helper()
	query := fmt.Sprintf("SELECT %s FROM resource_run_creation_child_evaluations WHERE parent_run_id = ? AND source_invocation_id = ?", column)
	update := fmt.Sprintf("UPDATE resource_run_creation_child_evaluations SET %s = ? WHERE parent_run_id = ? AND source_invocation_id = ?", column)
	if _, ok := fixture.primary.(*store.PostgresStore); ok {
		query = fmt.Sprintf("SELECT %s FROM resource_run_creation_child_evaluations WHERE parent_run_id = $1::uuid AND source_invocation_id = $2::uuid", column)
		update = fmt.Sprintf("UPDATE resource_run_creation_child_evaluations SET %s = $1 WHERE parent_run_id = $2::uuid AND source_invocation_id = $3::uuid", column)
	}
	var raw []byte
	retryRunCreationHostileSQL(t, fixture, func() error {
		return fixture.db.QueryRowContext(ctx, query, runID, sourceID).Scan(&raw)
	})
	var value map[string]any
	if err := json.Unmarshal(raw, &value); err != nil {
		t.Fatal(err)
	}
	mutate(value)
	corrupt, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	retryRunCreationHostileSQL(t, fixture, func() error {
		_, err := fixture.db.ExecContext(ctx, update, corrupt, runID, sourceID)
		return err
	})
}

func corruptSourceOperationJSONColumn(t testing.TB, ctx context.Context, fixture dataRunLifecycleFixture, column, sourceID string, mutate func(map[string]any)) {
	t.Helper()
	query := fmt.Sprintf("SELECT %s FROM resource_source_invocations WHERE source_invocation_id = ?", column)
	update := fmt.Sprintf("UPDATE resource_source_invocations SET %s = ? WHERE source_invocation_id = ?", column)
	if _, ok := fixture.primary.(*store.PostgresStore); ok {
		query = fmt.Sprintf("SELECT %s FROM resource_source_invocations WHERE source_invocation_id = $1::uuid", column)
		update = fmt.Sprintf("UPDATE resource_source_invocations SET %s = $1 WHERE source_invocation_id = $2::uuid", column)
	}
	var raw []byte
	retryRunCreationHostileSQL(t, fixture, func() error {
		return fixture.db.QueryRowContext(ctx, query, sourceID).Scan(&raw)
	})
	var value map[string]any
	if err := json.Unmarshal(raw, &value); err != nil {
		t.Fatal(err)
	}
	mutate(value)
	corrupt, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	retryRunCreationHostileSQL(t, fixture, func() error {
		_, err := fixture.db.ExecContext(ctx, update, corrupt, sourceID)
		return err
	})
}

func retryRunCreationHostileSQL(t testing.TB, fixture dataRunLifecycleFixture, operation func() error) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		err := operation()
		if err == nil {
			return
		}
		_, postgres := fixture.primary.(*store.PostgresStore)
		text := strings.ToLower(err.Error())
		busy := strings.Contains(text, "sqlite_busy") || strings.Contains(text, "database is locked") || strings.Contains(text, "database table is locked")
		if postgres || !busy || time.Now().After(deadline) {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func dataRunLifecycleCatalog(t *testing.T, bundleHash string, amended bool) (durabledata.Catalog, durabledata.DeclarationRef, durabledata.DeclarationRef) {
	t.Helper()
	scanRef, err := durabledata.ParseDeclarationRef(".", "scan.requested")
	if err != nil {
		t.Fatal(err)
	}
	scanProperties := map[string]any{"topic": map[string]any{"type": "string"}}
	scanRequired := []string{"topic"}
	scanInput := []byte("{\"topic\":\"probe\"}\n")
	if amended {
		scanProperties["stage"] = map[string]any{"type": "string"}
		scanRequired = append(scanRequired, "stage")
		scanInput = []byte("{\"stage\":\"new\",\"topic\":\"probe\"}\n")
	}
	scanSchema := map[string]any{"type": "object", "additionalProperties": false, "required": scanRequired, "properties": scanProperties}
	scan, defects := durabledata.CompileJSONL(scanRef, scanSchema, "", scanInput)
	if len(defects) != 0 {
		t.Fatalf("compile scan schema: %#v", defects)
	}
	scoreRef, err := durabledata.ParseDeclarationRef(".", "score.observed")
	if err != nil {
		t.Fatal(err)
	}
	scoreSchema := map[string]any{
		"type": "object", "additionalProperties": false, "required": []string{"label"},
		"properties": map[string]any{"label": map[string]any{"type": "string"}},
	}
	score, defects := durabledata.CompileJSONL(scoreRef, scoreSchema, "label", []byte("{\"label\":\"probe\"}\n"))
	if len(defects) != 0 {
		t.Fatalf("compile score schema: %#v", defects)
	}
	return durabledata.Catalog{BundleHash: bundleHash, Declarations: []durabledata.Declaration{
		{Name: scanRef.EventName, Ref: scanRef, SchemaDigest: scan.Manifest.SchemaDigest, CanonicalSchema: scan.CanonicalSchema},
		{Name: scoreRef.EventName, Ref: scoreRef, BusinessKey: "label", SchemaDigest: score.Manifest.SchemaDigest, CanonicalSchema: score.CanonicalSchema},
	}}, scanRef, scoreRef
}

func registerDataRunLifecycleCatalog(ctx context.Context, selected dataRunLifecycleStore, catalog durabledata.Catalog) error {
	_, err := selected.UpsertBundleCatalogWithData(ctx, bundlecatalog.Upsert{
		BundleHash: catalog.BundleHash, ContentYAML: "api_version: swarm.bundle.catalog.test.v1\n",
		ParsedJSON: map[string]any{"projection_version": "swarm.bundle.catalog.v2", "agents": []any{}},
		Metadata:   map[string]any{"source": "api-test"},
	}, catalog)
	return err
}

func dataRunEventPublishBody(runID, idempotencyKey string, data map[string]any) string {
	return dataRunEventPublishBodyForBundle(runID, idempotencyKey, runStartTestBundleHash, data)
}

func dataRunEventPublishBodyForBundle(runID, idempotencyKey, bundleHash string, data map[string]any) string {
	params := map[string]any{
		"bundle_hash": bundleHash, "event_name": "scan.requested",
		"payload": map[string]any{"topic": "trigger"}, "idempotency_key": idempotencyKey,
	}
	if runID != "" {
		params["run_id"] = runID
	}
	if data != nil {
		params["data"] = data
	}
	raw, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": idempotencyKey, "method": "event.publish", "params": params})
	if err != nil {
		panic(err)
	}
	return string(raw)
}

func dataRunStartBody(runID, idempotencyKey string, data map[string]any) string {
	params := map[string]any{
		"bundle_hash": runStartTestBundleHash, "event_name": "scan.requested", "payload": map[string]any{"topic": "trigger"},
		"run_id": runID, "idempotency_key": idempotencyKey, "data": data,
	}
	raw, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": idempotencyKey, "method": "run.start", "params": params})
	if err != nil {
		panic(err)
	}
	return string(raw)
}

func dataRunFusedImport(id string, ref durabledata.DeclarationRef, expected durabledata.ExpectedHead, input []byte) map[string]any {
	head := map[string]any{"state": expected.State}
	if expected.State == "version" {
		head["version_id"] = expected.VersionID
	}
	return map[string]any{
		"source_invocation_id": id, "declaration": dataRunDeclaration(ref), "expected_head": head,
		"input": map[string]any{"format": "jsonl", "content_base64": base64.StdEncoding.EncodeToString(input)},
	}
}

func dataRunDeclaration(ref durabledata.DeclarationRef) map[string]any {
	return map[string]any{"package_key": ref.PackageKey, "event": ref.EventName}
}

func dataRunHasPin(pins []durabledata.Pin, runID, state string) bool {
	for _, pin := range pins {
		if pin.RunID == runID && pin.RunState == state {
			return true
		}
	}
	return false
}

func dataRunCount(t *testing.T, fixture dataRunLifecycleFixture, table, field, id string) int {
	t.Helper()
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s = ?", table, field)
	if _, ok := fixture.primary.(*store.PostgresStore); ok {
		query = fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s = $1::uuid", table, field)
	}
	var count int
	if err := fixture.db.QueryRowContext(context.Background(), query, id).Scan(&count); err != nil {
		t.Fatalf("count %s by %s: %v", table, field, err)
	}
	return count
}
