package serveapp

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/json"
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
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	"github.com/division-sh/swarm/internal/runtime/mutationlog"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
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
			process := startServeRuntimeTestProcess(t, cliapp.ServeOptions{
				SourceRoot: root, ConfigPath: configPath, PlatformSpecPath: defaultPlatformSpecPath,
				SwarmDir: t.TempDir(), SwarmDirSet: true,
				APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0", SelfCheck: true, Verbose: true,
				TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(),
			})
			process.waitForReadyLine()
			endpoint := "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString()) + "/v1/rpc"
			runID := uuid.NewString()
			out, errOut, code := registryProofCLI(t, "run", "start", "--connect", strings.TrimSuffix(endpoint, "/v1/rpc"),
				"--run-id", runID, "--data", "company.registered="+filepath.Join(root, "data/companies.jsonl"), "--no-follow")
			if code != 0 || errOut != "" || !strings.Contains(out, "run_id="+runID) {
				t.Fatalf("public registry run.start: code=%d stdout=%s stderr=%s\nserve=%s", code, out, errOut, process.outputString())
			}
			registryProofWaitCompleted(t, endpoint, runID)
			entities, entityIDs := registryProofBusinessResults(t, endpoint, runID, outputEvents, companies)
			history := storetest.ObserveEntityMutationHistory(t, context.Background(), selected, runID)
			for _, row := range history {
				if row.EntityID == runID {
					t.Logf("root mutation id=%s domain=%s path=%q value=%s writer=%s step=%s cause=%s", row.MutationID, row.Domain, row.Path, row.NewValue, row.WriterID, row.HandlerStep, row.CausedByEvent)
				}
			}
			registryProofCommittedHistory(t, history, entities)
			t.Logf("replacement registry run=%s source=%s entities=4 history_rows=%d", runID, servedEventPublishFixtureBundleHash(t, root), len(history))
			verify := func(t *testing.T, phase, mode, entityID string) {
				t.Helper()
				registryProofVerify(t, root, configPath, runID, phase, mode, entityID)
				if after := storetest.ObserveEntityMutationHistory(t, context.Background(), selected, runID); !reflect.DeepEqual(history, after) {
					t.Fatal("public verification or the injection changed mutation history")
				}
			}
			for _, mode := range []string{"text", "json"} {
				t.Run("clean/"+mode, func(t *testing.T) { verify(t, "clean", mode, "") })
			}
			if t.Failed() {
				return
			}
			injectedEntity := entityIDs["aptos"]
			if injectedEntity == "" {
				t.Fatal("registry source entity was not constructed")
			}
			if err := storetest.CorruptRegistryVerdict(context.Background(), selected, runID, injectedEntity, "injected-drift"); err != nil {
				t.Fatal(err)
			}
			for _, mode := range []string{"text", "json"} {
				t.Run("drift/"+mode, func(t *testing.T) { verify(t, "drift", mode, injectedEntity) })
			}
		})
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

func registryProofVerify(t *testing.T, root, configPath, runID, phase, mode, entityID string) {
	t.Helper()
	args := []string{"verify", root, "--config", configPath, "--run", runID}
	if mode == "json" {
		args = append(args, "--json")
	}
	out, errOut, code := registryProofCLI(t, args...)
	wantCode := 0
	if phase == "drift" {
		wantCode = cliapp.CLIExitValidation
	}
	if code != wantCode || errOut != "" {
		t.Fatalf("public verify %s/%s: code=%d want=%d stdout=%s stderr=%s", phase, mode, code, wantCode, out, errOut)
	}
	if mode == "text" {
		registryProofVerifyText(t, phase, entityID, out)
	} else {
		registryProofVerifyJSON(t, phase, runID, entityID, out)
	}
	t.Logf("%s/%s public transcript: %s", phase, mode, strings.TrimSpace(out))
}

func registryProofVerifyText(t *testing.T, phase, entityID, out string) {
	t.Helper()
	if phase == "clean" {
		if out != "4 entities checked, no drift\n" {
			t.Fatalf("clean text is not the actual registry result: %s", out)
		}
		return
	}
	for _, want := range []string{"4 entities checked, 1 mismatches", entityID, "authored_field", `"verdict"`, "watchlist", "injected-drift"} {
		if !strings.Contains(out, want) {
			t.Fatalf("drift text misses %q: %s", want, out)
		}
	}
	if strings.Count(out, entityID) != 1 {
		t.Fatalf("drift text has extra entity rows: %s", out)
	}
}

func registryProofVerifyJSON(t *testing.T, phase, runID, entityID, out string) {
	t.Helper()
	var result struct {
		mutationlog.DriftReport
		Status string            `json:"status"`
		OK     bool              `json:"ok"`
		Errors []json.RawMessage `json:"errors"`
	}
	if err := json.Unmarshal([]byte(out), &result); err != nil {
		t.Fatal(err)
	}
	if result.RunID != runID || result.EntitiesChecked != 4 || result.Errors == nil || len(result.Errors) != 0 || result.Rows == nil {
		t.Fatalf("verification lost actual run evidence: %s", out)
	}
	if phase == "clean" {
		if !result.OK || result.Status != "passed" || len(result.Rows) != 0 {
			t.Fatalf("real clean registry disagrees: %s", out)
		}
		return
	}
	path := "verdict"
	want := []mutationlog.DriftRow{{Kind: "value", EntityID: entityID, Domain: mutationlog.DomainAuthoredField, Path: &path,
		FoldedPresent: true, StoredPresent: true, FoldedValue: "watchlist", StoredValue: "injected-drift", FoldedType: "text", StoredType: "text"}}
	if result.OK || result.Status != "drift" || !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("drift is not the one injected coordinate: rows=%+v want=%+v output=%s", result.Rows, want, out)
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
		var result struct {
			Run struct {
				RunID  string `json:"run_id"`
				Status string `json:"status"`
			} `json:"run"`
		}
		requireServedJSONRPCResult(t, endpoint, "run.get", map[string]any{"run_id": runID}, &result)
		if result.Run.RunID == runID && result.Run.Status == "completed" {
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
	requireServedJSONRPCResult(t, endpoint, "entity.list", map[string]any{"run_id": runID, "type": "company", "limit": 10}, &listed)
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
		if entityIDs[slug] != "" || entity.CurrentState != "done" || !reflect.DeepEqual(detail.Fields, want[slug]) {
			t.Fatalf("registry business result differs: entity=%+v fields=%+v expected=%+v", entity, detail.Fields, want[slug])
		}
		entityIDs[slug] = entity.EntityID
		statusEvent := eventidentity.ExternalizeForFlow(entity.FlowInstance, outputEvents, "company.status")
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
