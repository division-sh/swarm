package serveapp

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/packartifact"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/mutationlog"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	storeselected "github.com/division-sh/swarm/internal/store/selected"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/tests/support/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/tests/support/testutil"
)

type registryProofCompany struct {
	Slug     string `json:"slug"`
	Name     string `json:"name"`
	EngRoles int    `json:"eng_roles"`
}

func TestVerifyRunJobflowRegistryIntegrationBothStores(t *testing.T) {
	canonicalrouting.Prove(t, canonicalrouting.ArtifactID("internal/serveapp/testdata/verify-run-jobflow-registry"))
	root := filepath.Join(repoRootForTest(), "internal/serveapp/testdata/verify-run-jobflow-registry")
	companies := registryProofInput(t, root)
	outputEvents := loadWorkflowValidationBundleAt(t, root).FlowOutputEvents("registry")
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			unsetStoreSelectorEnv(t)
			configPath := writeStoreBackendRuntimeConfig(t, "sqlite", filepath.Join(t.TempDir(), "registry.sqlite"))
			if backend == "postgres" {
				setPostgresEnvFromDSN(t, testutil.StartEmptyPostgresDSN(t))
				configPath = os.Getenv("SWARM_CONFIG")
			}
			var selected any
			captureSelectedRuntimePersistence(t, func(p serveRuntimePersistence) { selected = p.deps.EventStore })
			serveOptions := cliapp.ServeOptions{
				SourceRoot: root, ConfigPath: configPath, SwarmDir: t.TempDir(), SwarmDirSet: true,
				APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0", SelfCheck: true, Verbose: true,
				TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(),
			}
			process := startServeRuntimeTestProcess(t, serveOptions)
			process.waitForReadyLine()
			endpoint := "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString()) + "/v1/rpc"
			runID := requireServedLiveLandingRunID(t, endpoint, servedEventPublishFixtureBundleHash(t, root), ".")
			copies := storetest.CollectTransactions(t, selected, storetest.TransactionProbeOptions{})
			out, errOut, code := registryProofCLI(t, "run", "start", "--connect", strings.TrimSuffix(endpoint, "/v1/rpc"),
				"--run-id", runID, "--data", "company.registered="+filepath.Join(root, "data/companies.jsonl"), "--no-follow")
			if code != 0 || errOut != "" || !strings.Contains(out, "run_id="+runID) {
				t.Fatalf("public registry run.start: code=%d stdout=%s stderr=%s\nserve=%s", code, out, errOut, process.outputString())
			}
			registryProofWaitCompleted(t, endpoint, runID)
			// Close the write interval before business/history inspection, verify,
			// or intentional corruption. Already captured attempts still join.
			copies.Stop()
			registryProofCopyReceipt(t, copies)
			logs, err := selected.(interface {
				ListOperatorRuntimeLogs(context.Context, operatorread.OperatorRuntimeLogListOptions) (operatorread.OperatorRuntimeLogListResult, error)
			}).ListOperatorRuntimeLogs(context.Background(), operatorread.OperatorRuntimeLogListOptions{RunID: runID, Limit: 100})
			if err != nil {
				t.Fatalf("observe completed registry diagnostics outside the cost window: %v", err)
			}
			t.Logf("Q1_Q2_REGISTRY_EXERCISE runtime_logs_first_100=%d", len(logs.Logs))
			entities, entityIDs := registryProofBusinessResults(t, endpoint, runID, outputEvents, companies)
			history := storetest.ObserveEntityMutationHistory(t, context.Background(), selected, runID)
			for _, row := range history {
				if row.EntityID == runID {
					t.Logf("root mutation id=%s domain=%s path=%q value=%s writer=%s step=%s cause=%s", row.MutationID, row.Domain, row.Path, row.NewValue, row.WriterID, row.HandlerStep, row.CausedByEvent)
				}
			}
			registryProofCommittedHistory(t, history, entities)
			t.Logf("replacement registry run=%s source=%s entities=4 history_rows=%d", runID, servedEventPublishFixtureBundleHash(t, root), len(history))
			verify := func(t *testing.T, phase, entityID string) {
				t.Helper()
				registryProofVerify(t, serveOptions, runID, phase, entityID)
				if after := storetest.ObserveEntityMutationHistory(t, context.Background(), selected, runID); !reflect.DeepEqual(history, after) {
					t.Fatal("check-stage inspection or the injection changed mutation history")
				}
			}
			t.Run("clean", func(t *testing.T) { verify(t, "clean", "") })
			if t.Failed() {
				return
			}
			injectedEntity := entityIDs["aptos"]
			if injectedEntity == "" {
				t.Fatal("registry source entity was not constructed")
			}
			ctx := servedRuntimeProofAuthorActivityContext(t, servedTestProcessRuntime(t, process), servedEventPublishFixtureBundleHash(t, root))
			if err := storetest.CorruptRegistryVerdict(ctx, selected, runID, injectedEntity, "injected-drift"); err != nil {
				t.Fatal(err)
			}
			t.Run("drift", func(t *testing.T) { verify(t, "drift", injectedEntity) })
		})
	}
}

func registryProofCopyReceipt(t *testing.T, collector *storetest.TransactionCollector) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		snapshot := collector.Snapshot()
		if snapshot.Active == 0 {
			operations, err := json.Marshal(snapshot.ByOperation)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("Q6_REGISTRY_OPERATION_RECEIPT counts=%s", operations)
			raw, err := json.Marshal(snapshot.Total)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("Q6_REGISTRY_COPY_RECEIPT counts=%s", raw)
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("registry write interval retains unsettled transactions: %+v", snapshot)
		}
		time.Sleep(time.Millisecond)
	}
}

func registryProofCommittedHistory(t *testing.T, history []storetest.EntityMutationEvidence, entities []operatorread.OperatorEntitySummary) {
	t.Helper()
	for _, entity := range entities {
		stages := map[string]bool{}
		causes := map[string]bool{}
		for _, row := range history {
			if row.EntityID != entity.EntityID || row.Domain != string(mutationlog.DomainLifecycleState) {
				continue
			}
			var stage string
			if err := json.Unmarshal(row.NewValue, &stage); err != nil {
				t.Fatal(err)
			}
			stages[stage] = true
			if row.CausedByEvent != "" {
				causes[row.CausedByEvent] = true
			}
		}
		if !reflect.DeepEqual(stages, map[string]bool{"registered": true, "investigating": true, "assessed": true, "done": true}) || len(causes) < 3 {
			t.Fatalf("registry did not commit its real successive handlers: entity=%s stages=%v causes=%v", entity.EntityID, stages, causes)
		}
	}
}

func registryProofVerify(t *testing.T, options cliapp.ServeOptions, runID, phase, entityID string) {
	t.Helper()
	ctx := context.Background()
	cfg, err := cliapp.LoadRuntimeConfigWithOptions(cliapp.RuntimeConfigLoadOptions{RepoRoot: repoRootForTest(), ExplicitPath: options.ConfigPath})
	if err != nil {
		t.Fatal(err)
	}
	paths, err := cliapp.ResolveCLISourcePaths(repoRootForTest(), cliapp.CLISourcePathOptions{SourceRoot: options.SourceRoot, ConfigPath: options.ConfigPath})
	if err != nil {
		t.Fatal(err)
	}
	invocation, err := cliapp.NewInvocationRoot(repoRootForTest())
	if err != nil {
		t.Fatal(err)
	}
	directory, err := cliapp.ResolveServeContextRegistrationSwarmDir(invocation, options)
	if err != nil {
		t.Fatal(err)
	}
	local, err := cliapp.ResolveLocalRuntimeState(cliapp.LocalRuntimeStateOptions{RepoRoot: repoRootForTest(), ResolvedPaths: paths, SwarmDir: directory, Config: cfg.Config})
	if err != nil {
		t.Fatal(err)
	}
	base, err := cliapp.LoadConfiguredPlatformPackBase(repoRootForTest(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	bases, err := packartifact.NewPlatformPackBaseGenerationOwner(base)
	if err != nil {
		t.Fatal(err)
	}
	options.DryRun = true
	options.APIListenAddr, options.MCPListenAddr = "127.0.0.1:0", "127.0.0.1:0"
	stage, err := cliapp.InspectServeCheckStage(ctx, cliapp.ServeCheckStageRequest{Repo: repoRootForTest(), Paths: paths, Config: cfg, Options: options, Selection: local.StoreSelection, PackBases: bases})
	if err != nil {
		t.Fatal(err)
	}
	observed := false
	for _, observation := range stage.Validation.BootReport.Observations {
		if observation.CheckID == "run_mutation_drift" {
			observed = true
			if (observation.Status == "passed") != (phase == "clean") {
				t.Fatalf("shared stage drift decision: phase=%s observation=%+v", phase, observation)
			}
		}
	}
	if !observed {
		t.Fatal("shared stage omitted active Live drift inspection")
	}
	request := storeselected.AuthorityRequest{Selection: local.StoreSelection}
	if request.Selection.Backend == storebackend.BackendPostgres {
		request.PostgresDSN, err = postgresDSNFromConfig(ctx, cfg.Config.Database)
		if err != nil {
			t.Fatal(err)
		}
	}
	inspection, err := storeselected.OpenAdmissionInspection(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	plans, err := cliapp.StateStoreSchemaPlans(stage.Bundle)
	if err != nil {
		t.Fatal(errors.Join(err, inspection.Close()))
	}
	schema, err := cliapp.SchemaBootstrapRequest(stage.Bundle.Platform, plans.Platform)
	if err != nil {
		t.Fatal(errors.Join(err, inspection.Close()))
	}
	var result mutationlog.DriftReport
	_, err = inspection.Inspect(ctx, schema, func(snapshot *storeselected.AdmissionSnapshot) error {
		var err error
		result, err = snapshot.InspectRunMutationDrift(ctx, runID)
		return err
	})
	if err = errors.Join(err, inspection.Close()); err != nil || result.RunID != runID || result.EntitiesChecked != 4 {
		t.Fatalf("native inspection lost actual run evidence: %+v %v", result, err)
	}
	if phase == "clean" {
		if len(result.Rows) != 0 {
			t.Fatalf("real clean registry disagrees: %+v", result)
		}
		return
	}
	path := "verdict"
	want := []mutationlog.DriftRow{{Kind: "value", EntityID: entityID, Domain: mutationlog.DomainAuthoredField, Path: &path,
		FoldedPresent: true, StoredPresent: true, FoldedValue: "watchlist", StoredValue: "injected-drift", FoldedType: "text", StoredType: "text"}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("drift is not the one injected coordinate: rows=%+v want=%+v", result.Rows, want)
	}
}

func registryProofCLI(t *testing.T, args ...string) (string, string, int) {
	t.Helper()
	var out, errOut bytes.Buffer
	code := executeCLIFrom(context.Background(), repoRootForTest(), args, &out, &errOut, nil)
	return out.String(), errOut.String(), code
}

func registryProofInput(t *testing.T, root string) []registryProofCompany {
	t.Helper()
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		label, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		t.Logf("replacement asset %s sha256=%x", filepath.ToSlash(label), sha256.Sum256(raw))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(filepath.Join(repoRootForTest(), "internal/durabledata/testdata/jobflow-gems.jsonl.gz"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	reader, err := gzip.NewReader(file)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if len(raw) != 666737 || fmt.Sprintf("%x", sha256.Sum256(raw)) != "7f91b3f892fd32e8605c9155bd4f7b4d90f4ff8dda1b556b1a68f0bb2d865a67" {
		t.Fatal("maintained jobflow corpus pin changed")
	}
	var want []registryProofCompany
	scanner := bufio.NewScanner(bytes.NewReader(raw))
	for len(want) < 3 && scanner.Scan() {
		var company registryProofCompany
		if err := json.Unmarshal(scanner.Bytes(), &company); err != nil {
			t.Fatal(err)
		}
		want = append(want, company)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	input, err := os.Open(filepath.Join(root, "data/companies.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	var got []registryProofCompany
	decoder := json.NewDecoder(input)
	for {
		var company registryProofCompany
		if err := decoder.Decode(&company); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		got = append(got, company)
	}
	if len(got) != 3 || !reflect.DeepEqual(got, want) {
		t.Fatalf("replacement inputs do not match the pinned jobflow projection: got=%+v want=%+v", got, want)
	}
	return got
}

func registryProofWaitCompleted(t *testing.T, endpoint, runID string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		var result cliapp.DiagnosticRunDiagnosisResult
		requireServedJSONRPCResult(t, endpoint, "run.diagnose", map[string]any{"run_id": runID}, &result)
		if result.Run.RunID == runID && result.Run.Status == "live" && result.Run.EndedAt == "" && result.Run.Failure == nil && result.TestQuiescence.Ready != nil && *result.TestQuiescence.Ready {
			return
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("registry did not settle its actual run: %+v", result)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func registryProofBusinessResults(t *testing.T, endpoint, runID string, outputEvents []string, companies []registryProofCompany) ([]operatorread.OperatorEntitySummary, map[string]string) {
	t.Helper()
	var listed operatorread.OperatorEntityListResult
	requireServedJSONRPCResult(t, endpoint, "entity.list", map[string]any{"run_id": runID, "flow": "registry", "limit": 10}, &listed)
	if len(listed.Entities) != 3 || listed.NextCursor != "" {
		t.Fatalf("registry entity cardinality=%+v", listed)
	}
	want := map[string]map[string]any{}
	for _, company := range companies {
		verdict := "watchlist"
		if company.EngRoles >= 7 {
			verdict = "qualified"
		}
		want[company.Slug] = map[string]any{"slug": company.Slug, "name": company.Name, "eng_roles": float64(company.EngRoles), "verdict": verdict}
	}
	entityIDs := map[string]string{}
	seenOutputs := map[string]bool{}
	for _, entity := range listed.Entities {
		var detail operatorread.OperatorEntityFull
		requireServedJSONRPCResult(t, endpoint, "entity.get", map[string]any{"run_id": runID, "entity_id": entity.EntityID}, &detail)
		slug, _ := detail.Fields["slug"].(string)
		if entityIDs[slug] != "" || entity.CurrentState == nil || *entity.CurrentState != "done" || !reflect.DeepEqual(detail.Fields, want[slug]) {
			t.Fatalf("registry business result differs: entity=%+v fields=%+v expected=%+v", entity, detail.Fields, want[slug])
		}
		entityIDs[slug] = entity.EntityID
		statusEvent := runtimeidentity.ExternalizeEventForFlow(entity.FlowInstance, outputEvents, "company.status")
		var events operatorread.OperatorEventListResult
		requireServedJSONRPCResult(t, endpoint, "event.list", map[string]any{"filter": map[string]any{"run_id": runID, "event_name": statusEvent}, "limit": 10}, &events)
		if len(events.Events) != 1 || events.NextCursor != "" {
			t.Fatalf("registry output cardinality=%+v canonical_event=%q", events, statusEvent)
		}
		event := events.Events[0]
		payload := event.Payload
		if seenOutputs[slug] || !reflect.DeepEqual(payload, want[slug]) || len(event.Deliveries) != 1 || event.Deliveries[0].Status != "delivered" {
			t.Fatalf("registry output/settlement differs: event=%+v payload=%+v", event, payload)
		}
		if target := event.Deliveries[0].Target; target.FlowID != "registry" || target.FlowInstance != entity.FlowInstance || target.EntityID != entity.EntityID {
			t.Fatalf("registry output settled on another receiver: target=%+v entity=%+v", target, entity)
		}
		seenOutputs[slug] = true
	}
	return listed.Entities, entityIDs
}
