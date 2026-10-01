package tools_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type resourceReadSelectedStore interface {
	runlifecycle.OperationOwner
	runlifecycle.CandidateStore
	durabledata.ResourceAccessStore
	EnsureSourceArtifactWithData(context.Context, *sourceartifact.AdmittedSourceArtifact, durabledata.Catalog) (sourceartifact.EnsureResult, error)
	ExecuteDataSourceOperation(context.Context, durabledata.SourceCommand) (durabledata.SourceOperationResult, error)
}

func TestResourceReadSelectedStore(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, keyed := range []bool{false, true} {
			name := "keyless"
			if keyed {
				name = "keyed"
			}
			t.Run(backend+"/"+name, func(t *testing.T) {
				root := resourceReadSourceRoot(t)
				if keyed {
					if err := os.WriteFile(filepath.Join(root, "support/events.yaml"), []byte("records.loaded:\n  key: id\n  id: text\n  payload: text\n"), 0o600); err != nil {
						t.Fatal(err)
					}
				}
				repo := pipeline.WorkflowRepoRoot()
				bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, root, contracts.DefaultPlatformSpecFile(repo))
				if err != nil {
					t.Fatal(err)
				}
				source := semanticview.Wrap(bundle)
				catalog, err := contracts.BuildDurableDataCatalog(bundle)
				if err != nil {
					t.Fatal(err)
				}
				actor := models.AgentConfig{ID: "public-factory-cto", Role: "factory_cto", FlowID: "support"}
				declaration, ok := semanticview.ResolveAgentDeclaration(source, actor)
				if !ok {
					t.Fatal("resolve resource actor")
				}
				plan, err := semanticview.ScopedAgentNamePlan(source, declaration)
				if err != nil {
					t.Fatal(err)
				}
				actor.Identity = agentidentitytest.Declared(t, plan.AgentID, plan.OwnerURI, "support", "selected-store", "support/selected-store")
				ref := flowdataResourceRef(t, source)
				const runID = "11111111-1111-4111-8111-111111111111"
				ctx := models.WithActor(runtimecorrelation.WithRunID(unmanagedToolTestContext(), runID), actor)
				var selected resourceReadSelectedStore
				var db *sql.DB
				var reopen func() resourceReadSelectedStore
				if backend == "postgres" {
					_, db, _ = testutil.StartPostgres(t)
					reopen = func() resourceReadSelectedStore { return storetest.AdmitPostgresRuntimeStore(t, db) }
					selected = reopen()
				} else {
					original := storetest.StartSQLiteRuntimeStore(t)
					selected, db = original, storetest.Database(original)
					reopen = func() resourceReadSelectedStore { return storetest.AdmitSQLiteRuntimeStore(t, db) }
				}
				if _, err := selected.EnsureSourceArtifactWithData(ctx, bundle.SourceArtifact, catalog); err != nil {
					t.Fatal(err)
				}
				storetest.RequireRun(t, ctx, selected, storetest.RunFixture{RunID: runID, State: runlifecycle.StateRunning, Artifact: bundle.SourceArtifact, Origin: storetest.ScenarioSetupOrigin(), StartedAt: time.Now().UTC()})
				importRows := func(input string, head durabledata.ExpectedHead) durabledata.SourceOperationResult {
					result, err := selected.ExecuteDataSourceOperation(ctx, durabledata.SourceCommand{Operation: "import", SourceInvocationID: uuid.NewString(), Actor: "operator", BundleHash: catalog.BundleHash, Declaration: ref, ExpectedHead: head, InputFormat: "jsonl", Input: []byte(input)})
					if err != nil || result.Outcome != "accepted" {
						t.Fatalf("import %+v: %v", result, err)
					}
					return result
				}
				first := importRows("{\"id\":\"b\",\"payload\":\"beta\"}\n{\"id\":\"a\",\"payload\":\"alpha\"}\n", durabledata.AbsentHead())
				pin := `INSERT INTO resource_version_pins (run_id, flow_path, event_name, schema_digest, version_id, selection, pinned_at) VALUES ($1,$2,$3,$4,$5,'explicit',$6)`
				if _, err := db.ExecContext(ctx, pin, runID, ref.FlowPath, ref.EventName, first.SchemaDigest, first.Candidate.VersionID, time.Now().UTC()); err != nil {
					t.Fatal(err)
				}
				executor := runtimetools.NewExecutorWithOptions(nil, runtimetools.ExecutorOptions{WorkflowSource: source, DataAccessStore: selected})
				input := map[string]any{"kind": "resource_row", "declaration": ref, "position": 1}
				if keyed {
					input = map[string]any{"kind": "resource_row", "declaration": ref, "key": "a"}
				}
				before, err := executor.Execute(ctx, "read_flow_data", input)
				if err != nil {
					t.Fatal(err)
				}
				result := before.(map[string]any)
				if result["version_id"] != first.Candidate.VersionID {
					t.Fatalf("row selected another version: %+v", result)
				}
				row := result["row"].(map[string]any)
				want := "b"
				if keyed {
					want = "a"
				}
				if row["ordinal"] != uint64(1) || row["value"].(map[string]any)["id"] != want {
					t.Fatalf("row order %+v", row)
				}
				pageInput := map[string]any{"kind": "resource_rows", "declaration": ref, "page": map[string]any{"limit": 1}}
				firstPage, err := executor.Execute(ctx, "read_flow_data", pageInput)
				if err != nil {
					t.Fatal(err)
				}
				page := firstPage.(map[string]any)["rows"].(durabledata.PageResult[map[string]any])
				if page.ItemCount != 1 || page.Continuation.State != "more" {
					t.Fatalf("first page %+v", page)
				}
				importRows("{\"id\":\"c\",\"payload\":\"current head is not this run\"}\n", first.Head.After)
				selected = reopen()
				executor = runtimetools.NewExecutorWithOptions(nil, runtimetools.ExecutorOptions{WorkflowSource: source, DataAccessStore: selected})
				after, err := executor.Execute(ctx, "read_flow_data", input)
				if err != nil || !reflect.DeepEqual(before, after) {
					t.Fatalf("reopened pinned row=%+v, %v; before=%+v", after, err, before)
				}
				pageInput["page"] = map[string]any{"limit": 1, "cursor": page.Continuation.Cursor}
				last, err := executor.Execute(ctx, "read_flow_data", pageInput)
				if err != nil {
					t.Fatal(err)
				}
				lastPage := last.(map[string]any)["rows"].(durabledata.PageResult[map[string]any])
				encoded, _ := json.Marshal(lastPage.Items)
				if lastPage.ItemCount != 1 || lastPage.Items[0]["ordinal"] != uint64(2) || lastPage.Continuation != durabledata.EndContinuation() || lastPage.EncodedItemsBytes != len(encoded) {
					t.Fatalf("last page %+v", lastPage)
				}
				for _, tc := range []struct {
					name, query string
					value       any
					code        durabledata.ErrorCode
				}{
					{"corrupt_payload", `UPDATE resource_versions SET canonical_jsonl=$1 WHERE version_id=$2`, []byte("x"), durabledata.CodeIntegrity},
					{"pruned_payload", `UPDATE resource_versions SET pruned_at=$1,canonical_jsonl=NULL WHERE version_id=$2`, time.Now().UTC(), durabledata.CodePayloadPruned},
					{"pin_schema", `UPDATE resource_version_pins SET schema_digest=$1 WHERE version_id=$2`, "resource-schema-v1:sha256:" + strings.Repeat("f", 64), durabledata.CodeIntegrity},
				} {
					t.Run(tc.name, func(t *testing.T) {
						var original []byte
						if err := db.QueryRowContext(ctx, `SELECT canonical_jsonl FROM resource_versions WHERE version_id=$1`, first.Candidate.VersionID).Scan(&original); err != nil {
							t.Fatal(err)
						}
						if _, err := db.ExecContext(ctx, tc.query, tc.value, first.Candidate.VersionID); err != nil {
							t.Fatal(err)
						}
						_, err := executor.Execute(ctx, "read_flow_data", input)
						var domain *durabledata.DomainError
						if !errors.As(err, &domain) || domain.Code != tc.code {
							t.Fatalf("%s returned %v", tc.name, err)
						}
						if _, err := db.ExecContext(ctx, `UPDATE resource_versions SET canonical_jsonl=$1,pruned_at=NULL WHERE version_id=$2`, original, first.Candidate.VersionID); err != nil {
							t.Fatal(err)
						}
						if _, err := db.ExecContext(ctx, `UPDATE resource_version_pins SET schema_digest=$1 WHERE version_id=$2`, first.SchemaDigest, first.Candidate.VersionID); err != nil {
							t.Fatal(err)
						}
					})
				}
				if _, err := db.ExecContext(ctx, `DELETE FROM resource_version_pins WHERE run_id=$1`, runID); err != nil {
					t.Fatal(err)
				}
				_, err = executor.Execute(ctx, "read_flow_data", input)
				var domain *durabledata.DomainError
				if !errors.As(err, &domain) || domain.Code != durabledata.CodeAccessDenied {
					t.Fatalf("missing pin returned %v", err)
				}
			})
		}
	}
}

func flowdataResourceRef(t *testing.T, source semanticview.Source) durabledata.DeclarationRef {
	t.Helper()
	refs := source.DurableDataForAgent("support", "factory-cto")
	if len(refs) != 1 {
		t.Fatalf("resource declarations %+v", refs)
	}
	return refs[0]
}

func resourceReadSourceRoot(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	for path, contents := range map[string]string{
		"schema.yaml":           "name: resource-read-store\n",
		"support/schema.yaml":   "name: support\n",
		"support/entities.yaml": "support_state:\n  support_id: text\n",
		"support/agents.yaml":   "factory-cto:\n  id: public-factory-cto\n  role: factory_cto\n  intent: {inline: Read exact run data.}\n  data_access:\n    - data: support/records.loaded\n",
		"support/events.yaml":   "records.loaded:\n  id: text\n  payload: text\n",
	} {
		file := filepath.Join(root, path)
		if err := os.MkdirAll(filepath.Dir(file), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(file, []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return root
}
