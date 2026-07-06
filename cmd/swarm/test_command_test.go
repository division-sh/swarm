package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/google/uuid"
	"gopkg.in/yaml.v3"

	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
)

func TestSwarmTestRunsScenarioThroughPublicRPC(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	setCLIAPITestToken(t, "test-token")
	contractsPath := writeScenarioRunnerFixture(t)
	bundleHash := servedEventPublishFixtureBundleHash(t, contractsPath)

	var calls []jsonRPCRequest
	var publishCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/rpc" {
			t.Errorf("path = %q, want /v1/rpc", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-token" {
			t.Errorf("Authorization = %q, want bearer token", got)
		}
		var req jsonRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode request: %v", err)
		}
		calls = append(calls, req)
		switch req.Method {
		case eventPublishMethod:
			publishCalls++
			switch publishCalls {
			case 1:
				if req.Params["event_name"] != "thing.created" || req.Params["bundle_hash"] != bundleHash || req.Params["idempotency_key"] != scenarioSHA40("empire-cost-router") {
					t.Fatalf("event.publish initial params = %#v", req.Params)
				}
				payload, ok := req.Params["payload"].(map[string]any)
				if !ok || payload["who"] != "operator" || !numberEquals(payload["amount"], 7) {
					t.Fatalf("event.publish initial payload = %#v", req.Params["payload"])
				}
				writeJSONRPCResult(t, w, req.ID, eventPublishTestResult(true))
			case 2:
				if req.Params["event_name"] != "thing.reviewed" || req.Params["bundle_hash"] != bundleHash || req.Params["run_id"] != "run-1" || req.Params["source_event_id"] != "event-1" {
					t.Fatalf("event.publish follow-up params = %#v", req.Params)
				}
				payload, ok := req.Params["payload"].(map[string]any)
				if !ok || payload["note"] != "approved" {
					t.Fatalf("event.publish follow-up payload = %#v", req.Params["payload"])
				}
				result := eventPublishTestResult(false)
				result["event_id"] = "event-2"
				result["source_event_id"] = "event-1"
				writeJSONRPCResult(t, w, req.ID, result)
			default:
				t.Fatalf("unexpected extra event.publish params = %#v", req.Params)
			}
		case "run.diagnose":
			writeJSONRPCResult(t, w, req.ID, scenarioRunDiagnoseTestResult("run-1", true))
		case "run.trace":
			row := validRunCommandTraceRow("event-1")
			row["event_name"] = "thing.created"
			followUp := validRunCommandTraceRow("event-2")
			followUp["event_name"] = "thing.reviewed"
			writeJSONRPCResult(t, w, req.ID, map[string]any{"trace": []map[string]any{row, followUp}})
		case eventObservationMethodList:
			writeJSONRPCResult(t, w, req.ID, map[string]any{"events": []any{}})
		case entityListMethod:
			entity := validEntitySummary("entity-1")
			entity["entity_type"] = "widget"
			entity["current_state"] = "done"
			writeJSONRPCResult(t, w, req.ID, map[string]any{"entities": []map[string]any{entity}})
		case entityGetMethod:
			if req.Params["entity_id"] != "entity-1" || req.Params["run_id"] != "run-1" {
				t.Fatalf("entity.get params = %#v", req.Params)
			}
			result := validEntityFullResult("entity-1")
			entity := result["entity"].(map[string]any)
			entity["entity_type"] = "widget"
			entity["current_state"] = "done"
			writeJSONRPCResult(t, w, req.ID, result)
		default:
			t.Fatalf("unexpected method = %s", req.Method)
		}
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), repoRoot(), []string{
		"test",
		"--contracts", contractsPath,
		"--platform-spec", defaultPlatformSpecPath,
		"--timeout", "2s",
		"--poll-interval", "10ms",
	}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	assertScenarioTestMethods(t, calls, []string{
		eventPublishMethod,
		"run.diagnose",
		eventPublishMethod,
		"run.diagnose",
		"run.diagnose",
		"run.trace",
		eventObservationMethodList,
		entityListMethod,
		entityGetMethod,
	})
	for _, want := range []string{"scenario ok:", "swarm test ok: scenarios=1"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout.String())
		}
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestSwarmTestSetupEntitiesSeedsAliasTargetAndExpectationThroughPublicRPC(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	setCLIAPITestToken(t, "test-token")
	contractsPath := writeScenarioSetupFixture(t)
	bundleHash := servedEventPublishFixtureBundleHash(t, contractsPath)

	var calls []jsonRPCRequest
	var setupRunID string
	var setupEntityID string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req jsonRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode request: %v", err)
		}
		calls = append(calls, req)
		switch req.Method {
		case scenarioTestSetupEntitiesMethod:
			if req.Params["bundle_hash"] != bundleHash {
				t.Fatalf("test.setup_entities bundle_hash = %#v, want %s", req.Params["bundle_hash"], bundleHash)
			}
			setupRunID, _ = req.Params["run_id"].(string)
			if _, err := uuid.Parse(setupRunID); err != nil {
				t.Fatalf("test.setup_entities run_id = %#v, want UUID", req.Params["run_id"])
			}
			entities, ok := req.Params["entities"].([]any)
			if !ok || len(entities) != 1 {
				t.Fatalf("test.setup_entities entities = %#v, want one", req.Params["entities"])
			}
			entity, ok := entities[0].(map[string]any)
			if !ok {
				t.Fatalf("test.setup_entities entity = %#v, want mapping", entities[0])
			}
			setupEntityID, _ = entity["entity_id"].(string)
			if _, err := uuid.Parse(setupEntityID); err != nil {
				t.Fatalf("test.setup_entities entity_id = %#v, want UUID", entity["entity_id"])
			}
			if entity["alias"] != "product" || entity["flow_instance"] != "operating" || entity["entity_type"] != "product" || entity["current_state"] != "waiting" {
				t.Fatalf("test.setup_entities entity = %#v", entity)
			}
			if err := assertScenarioJSONEqual("test.setup_entities fields", entity["fields"], map[string]any{"product_id": "p-1", "note": "seeded"}); err != nil {
				t.Fatal(err)
			}
			if err := assertScenarioJSONEqual("test.setup_entities gates", entity["gates"], map[string]any{"review_ready": true}); err != nil {
				t.Fatal(err)
			}
			writeJSONRPCResult(t, w, req.ID, map[string]any{
				"run_id": setupRunID,
				"entities": []map[string]any{{
					"alias":         "product",
					"entity_id":     setupEntityID,
					"flow_instance": "operating",
					"entity_type":   "product",
					"current_state": "waiting",
				}},
			})
		case eventPublishMethod:
			if req.Params["event_name"] != "operating/opco.product_review_requested" || req.Params["bundle_hash"] != bundleHash || req.Params["run_id"] != setupRunID {
				t.Fatalf("event.publish params = %#v", req.Params)
			}
			target, ok := req.Params["target"].(map[string]any)
			if !ok || target["flow_instance"] != "operating" || target["entity_id"] != setupEntityID {
				t.Fatalf("event.publish target = %#v", req.Params["target"])
			}
			payload, ok := req.Params["payload"].(map[string]any)
			if !ok || payload["note"] != "approved" {
				t.Fatalf("event.publish payload = %#v", req.Params["payload"])
			}
			result := eventPublishTestResult(false)
			result["run_id"] = setupRunID
			result["event_id"] = "event-setup-follow-up"
			result["source_event_id"] = ""
			writeJSONRPCResult(t, w, req.ID, result)
		case "run.diagnose":
			writeJSONRPCResult(t, w, req.ID, scenarioRunDiagnoseTestResult(setupRunID, true))
		case "run.trace":
			writeJSONRPCResult(t, w, req.ID, map[string]any{"trace": []map[string]any{scenarioTraceRowForEvent("event-setup-follow-up", "operating/opco.product_review_requested")}})
		case entityGetMethod:
			if req.Params["entity_id"] != setupEntityID || req.Params["run_id"] != setupRunID {
				t.Fatalf("entity.get params = %#v", req.Params)
			}
			result := validEntityFullResult(setupEntityID)
			entity := result["entity"].(map[string]any)
			entity["run_id"] = setupRunID
			entity["entity_type"] = "product"
			entity["current_state"] = "ready"
			result["fields"] = map[string]any{"product_id": "p-1", "note": "approved"}
			result["gates"] = map[string]any{"review_ready": false}
			writeJSONRPCResult(t, w, req.ID, result)
		default:
			t.Fatalf("unexpected method = %s", req.Method)
		}
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), repoRoot(), []string{
		"test",
		"--contracts", contractsPath,
		"--platform-spec", defaultPlatformSpecPath,
		"--timeout", "2s",
		"--poll-interval", "10ms",
	}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	assertScenarioTestMethods(t, calls, []string{
		scenarioTestSetupEntitiesMethod,
		eventPublishMethod,
		"run.diagnose",
		"run.diagnose",
		"run.trace",
		entityGetMethod,
	})
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestSwarmTestSetupEntitiesSeedsRootRunEntityThroughPublicRPC(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	setCLIAPITestToken(t, "test-token")
	contractsPath := writeScenarioRootSetupFixture(t)
	bundleHash := servedEventPublishFixtureBundleHash(t, contractsPath)

	var calls []jsonRPCRequest
	var setupRunID string
	var setupEntityID string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req jsonRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode request: %v", err)
		}
		calls = append(calls, req)
		switch req.Method {
		case scenarioTestSetupEntitiesMethod:
			if req.Params["bundle_hash"] != bundleHash {
				t.Fatalf("test.setup_entities bundle_hash = %#v, want %s", req.Params["bundle_hash"], bundleHash)
			}
			setupRunID, _ = req.Params["run_id"].(string)
			if _, err := uuid.Parse(setupRunID); err != nil {
				t.Fatalf("test.setup_entities run_id = %#v, want UUID", req.Params["run_id"])
			}
			entities, ok := req.Params["entities"].([]any)
			if !ok || len(entities) != 1 {
				t.Fatalf("test.setup_entities entities = %#v, want one", req.Params["entities"])
			}
			entity, ok := entities[0].(map[string]any)
			if !ok {
				t.Fatalf("test.setup_entities entity = %#v, want mapping", entities[0])
			}
			setupEntityID, _ = entity["entity_id"].(string)
			if want := runtimeflowidentity.EntityID(setupRunID); setupEntityID != want {
				t.Fatalf("test.setup_entities root entity_id = %q, want canonical run root %q", setupEntityID, want)
			}
			if entity["alias"] != "widget" || entity["flow_instance"] != "" || entity["entity_type"] != "widget" || entity["current_state"] != "waiting" {
				t.Fatalf("test.setup_entities root entity = %#v", entity)
			}
			if err := assertScenarioJSONEqual("test.setup_entities root fields", entity["fields"], map[string]any{"score": float64(5)}); err != nil {
				t.Fatal(err)
			}
			writeJSONRPCResult(t, w, req.ID, map[string]any{
				"run_id": setupRunID,
				"entities": []map[string]any{{
					"alias":         "widget",
					"entity_id":     setupEntityID,
					"flow_instance": "",
					"entity_type":   "widget",
					"current_state": "waiting",
				}},
			})
		case eventPublishMethod:
			if req.Params["event_name"] != "widget.scored" || req.Params["bundle_hash"] != bundleHash || req.Params["run_id"] != setupRunID {
				t.Fatalf("event.publish root setup params = %#v", req.Params)
			}
			if _, ok := req.Params["target"]; ok {
				t.Fatalf("event.publish root setup target = %#v, want omitted", req.Params["target"])
			}
			payload, ok := req.Params["payload"].(map[string]any)
			if !ok || !numberEquals(payload["delta"], 7) {
				t.Fatalf("event.publish root setup payload = %#v", req.Params["payload"])
			}
			result := eventPublishTestResult(false)
			result["run_id"] = setupRunID
			result["event_id"] = "event-root-setup"
			result["source_event_id"] = ""
			writeJSONRPCResult(t, w, req.ID, result)
		case "run.diagnose":
			writeJSONRPCResult(t, w, req.ID, scenarioRunDiagnoseTestResult(setupRunID, true))
		case "run.trace":
			writeJSONRPCResult(t, w, req.ID, map[string]any{"trace": []map[string]any{scenarioTraceRowForEvent("event-root-setup", "widget.scored")}})
		case entityGetMethod:
			if req.Params["entity_id"] != setupEntityID || req.Params["run_id"] != setupRunID {
				t.Fatalf("entity.get root setup params = %#v", req.Params)
			}
			result := validEntityFullResult(setupEntityID)
			entity := result["entity"].(map[string]any)
			entity["run_id"] = setupRunID
			entity["entity_type"] = "widget"
			entity["current_state"] = "done"
			result["fields"] = map[string]any{"score": float64(12)}
			writeJSONRPCResult(t, w, req.ID, result)
		default:
			t.Fatalf("unexpected method = %s", req.Method)
		}
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), repoRoot(), []string{
		"test",
		"--contracts", contractsPath,
		"--platform-spec", defaultPlatformSpecPath,
		"--timeout", "2s",
		"--poll-interval", "10ms",
	}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	assertScenarioTestMethods(t, calls, []string{
		scenarioTestSetupEntitiesMethod,
		eventPublishMethod,
		"run.diagnose",
		"run.diagnose",
		"run.trace",
		entityGetMethod,
	})
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestSwarmTestRunsCatalogSmokeCompanionVisibleBehavior(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	setCLIAPITestToken(t, "test-token")
	contractsPath := filepath.Join(repoRoot(), "tests", "tier1-primitives", "test-emits-single")
	bundleHash := servedEventPublishFixtureBundleHash(t, contractsPath)

	var calls []jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req jsonRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode request: %v", err)
		}
		calls = append(calls, req)
		switch req.Method {
		case eventPublishMethod:
			if req.Params["event_name"] != "item.received" || req.Params["bundle_hash"] != bundleHash {
				t.Fatalf("event.publish params = %#v", req.Params)
			}
			payload, ok := req.Params["payload"].(map[string]any)
			if !ok || len(payload) != 0 {
				t.Fatalf("event.publish payload = %#v, want empty mapping", req.Params["payload"])
			}
			writeJSONRPCResult(t, w, req.ID, eventPublishTestResult(true))
		case "run.diagnose":
			writeJSONRPCResult(t, w, req.ID, scenarioRunDiagnoseTestResult("run-1", true))
		case "run.trace":
			received := validRunCommandTraceRow("event-1")
			received["event_name"] = "item.received"
			processed := validRunCommandTraceRow("event-2")
			processed["event_name"] = "item.processed"
			writeJSONRPCResult(t, w, req.ID, map[string]any{"trace": []map[string]any{received, processed}})
		case entityListMethod:
			if req.Params["run_id"] != "run-1" || req.Params["type"] != "default" {
				t.Fatalf("entity.list params = %#v", req.Params)
			}
			entity := validEntitySummary("entity-1")
			entity["entity_type"] = "default"
			entity["current_state"] = "done"
			writeJSONRPCResult(t, w, req.ID, map[string]any{"entities": []map[string]any{entity}})
		case entityGetMethod:
			if req.Params["entity_id"] != "entity-1" || req.Params["run_id"] != "run-1" {
				t.Fatalf("entity.get params = %#v", req.Params)
			}
			result := validEntityFullResult("entity-1")
			entity := result["entity"].(map[string]any)
			entity["entity_type"] = "default"
			entity["current_state"] = "done"
			writeJSONRPCResult(t, w, req.ID, result)
		default:
			t.Fatalf("unexpected method = %s", req.Method)
		}
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), repoRoot(), []string{
		"test",
		"--contracts", contractsPath,
		"--platform-spec", defaultPlatformSpecPath,
		"--timeout", "2s",
		"--poll-interval", "10ms",
	}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	assertScenarioTestMethods(t, calls, []string{
		eventPublishMethod,
		"run.diagnose",
		"run.diagnose",
		"run.trace",
		entityListMethod,
		entityGetMethod,
	})
	for _, want := range []string{"scenario ok:", "swarm test ok: scenarios=1"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout.String())
		}
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestSwarmTestRunsTier1PositiveEmissionCatalogCompanions(t *testing.T) {
	for _, packageName := range []string{
		"test-advances-to",
		"test-advances-to-terminal",
		"test-compute-standalone",
		"test-data-accumulation-direct",
		"test-data-accumulation-literal",
		"test-data-accumulation-mapped",
		"test-emits-multiple",
		"test-emits-payload-transform",
		"test-from-filter",
		"test-guard-escalate",
		"test-guard-multi",
		"test-guard-pass",
		"test-guard-policy-ref",
		"test-on-complete-first-match",
		"test-on-complete-second-match",
		"test-record-evidence",
		"test-rules-advances-to",
		"test-rules-else",
		"test-rules-match",
		"test-sets-gate",
	} {
		t.Run(packageName, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			setCLIAPITestToken(t, "test-token")
			contractsPath := filepath.Join(repoRoot(), "tests", "tier1-primitives", packageName)
			scenarioPath := filepath.Join(contractsPath, "tests", "visible-smoke.yaml")
			raw, err := os.ReadFile(scenarioPath)
			if err != nil {
				t.Fatalf("read scenario: %v", err)
			}
			doc, err := parseScenarioDocument(raw)
			if err != nil {
				t.Fatalf("parse scenario: %v", err)
			}
			if len(doc.Steps) != 1 || doc.Steps[0].Action != "publish" {
				t.Fatalf("scenario steps = %#v, want one publish step", doc.Steps)
			}
			if len(doc.Expect.Events.Include) == 0 {
				t.Fatal("scenario must include a positive emitted-event expectation")
			}
			if len(doc.Expect.Entities) != 1 || !doc.Expect.Entities[0].StateSet {
				t.Fatalf("scenario entity expectations = %#v, want one current_state detail assertion", doc.Expect.Entities)
			}
			assertSwarmTestScenarioThroughPublicRPC(t, contractsPath, doc)
		})
	}
}

func TestSwarmTestRunsTier3ListProcessingCatalogCompanions(t *testing.T) {
	for _, tc := range []struct {
		packageName       string
		wantPositiveEvent bool
	}{
		{packageName: "test-fan-out-basic", wantPositiveEvent: true},
		{packageName: "test-fan-out-count", wantPositiveEvent: true},
		{packageName: "test-fan-out-emit-mapping", wantPositiveEvent: true},
		{packageName: "test-fan-out-empty"},
		{packageName: "test-filter-basic", wantPositiveEvent: true},
		{packageName: "test-filter-empty", wantPositiveEvent: true},
		{packageName: "test-group-by-standalone", wantPositiveEvent: true},
		{packageName: "test-reduce-count", wantPositiveEvent: true},
		{packageName: "test-reduce-max", wantPositiveEvent: true},
		{packageName: "test-reduce-min", wantPositiveEvent: true},
		{packageName: "test-reduce-operation-count", wantPositiveEvent: true},
	} {
		t.Run(tc.packageName, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			setCLIAPITestToken(t, "test-token")
			contractsPath := filepath.Join(repoRoot(), "tests", "tier3-list-processing", tc.packageName)
			scenarioPath := filepath.Join(contractsPath, "tests", "visible-smoke.yaml")
			raw, err := os.ReadFile(scenarioPath)
			if err != nil {
				t.Fatalf("read scenario: %v", err)
			}
			doc, err := parseScenarioDocument(raw)
			if err != nil {
				t.Fatalf("parse scenario: %v", err)
			}
			if len(doc.Steps) != 1 || doc.Steps[0].Action != "publish" {
				t.Fatalf("scenario steps = %#v, want one publish step", doc.Steps)
			}
			if tc.wantPositiveEvent && len(doc.Expect.Events.Include) == 0 {
				t.Fatal("scenario must include a positive emitted-event expectation")
			}
			if !tc.wantPositiveEvent && len(doc.Expect.Events.Include) != 0 {
				t.Fatalf("scenario includes events %#v, want no event-output assertion", doc.Expect.Events.Include)
			}
			if len(doc.Expect.Entities) != 1 || !doc.Expect.Entities[0].StateSet {
				t.Fatalf("scenario entity expectations = %#v, want one current_state detail assertion", doc.Expect.Entities)
			}
			assertSwarmTestScenarioThroughPublicRPC(t, contractsPath, doc)
		})
	}
}

func TestSwarmTestRunsRemainingCurrentPublicOwnerCatalogCompanions(t *testing.T) {
	for _, tc := range []struct {
		tier        string
		packageName string
	}{
		{tier: "tier1-primitives", packageName: "test-guard-discard"},
		{tier: "tier1-primitives", packageName: "test-guard-kill"},
		{tier: "tier1-primitives", packageName: "test-guard-multi-fail"},
		{tier: "tier1-primitives", packageName: "test-guard-reject"},
		{tier: "tier1-primitives", packageName: "test-rules-data-accumulation"},
		{tier: "tier1-primitives", packageName: "test-rules-no-match"},
		{tier: "tier10-policy-patterns", packageName: "test-policy-capacity-query"},
		{tier: "tier10-policy-patterns", packageName: "test-policy-hard-gate-override"},
		{tier: "tier10-policy-patterns", packageName: "test-policy-threshold-three-way"},
		{tier: "tier10-policy-patterns", packageName: "test-policy-timeout-elapsed"},
		{tier: "tier11-flow-composition", packageName: "test-child-flow-absolute-path"},
		{tier: "tier11-flow-composition", packageName: "test-child-flow-pin-wiring"},
		{tier: "tier11-flow-composition", packageName: "test-child-flow-policy-inherit"},
		{tier: "tier11-flow-composition", packageName: "test-data-pin-wiring"},
		{tier: "tier11-flow-composition", packageName: "test-multi-level-policy-inherit"},
		{tier: "tier11-flow-composition", packageName: "test-wildcard-deep-subscription"},
		{tier: "tier12-runtime-fork", packageName: "test-non-agent-replay-fail-closed"},
		{tier: "tier4-cross-entity", packageName: "test-create-entity"},
		{tier: "tier4-cross-entity", packageName: "test-query-filter"},
		{tier: "tier4-cross-entity", packageName: "test-query-group-by"},
		{tier: "tier5-flow-lifecycle", packageName: "test-auto-emit-on-create"},
		{tier: "tier5-flow-lifecycle", packageName: "test-terminal-state-preserves"},
		{tier: "tier5-flow-lifecycle", packageName: "test-terminal-state-rejects"},
		{tier: "tier5-flow-lifecycle", packageName: "test-timer-cancel"},
		{tier: "tier5-flow-lifecycle", packageName: "test-timer-start-on"},
		{tier: "tier5-flow-lifecycle", packageName: "test-wildcard-subscription"},
		{tier: "tier6-event-loop", packageName: "test-atomicity-rollback"},
		{tier: "tier6-event-loop", packageName: "test-dead-letter"},
		{tier: "tier6-event-loop", packageName: "test-on-complete-atomicity-chain"},
		{tier: "tier7-composition", packageName: "test-agent-emits-to-node"},
		{tier: "tier7-composition", packageName: "test-cross-flow-subscription"},
		{tier: "tier7-composition", packageName: "test-dual-delivery"},
		{tier: "tier7-composition", packageName: "test-full-lifecycle"},
		{tier: "tier7-composition", packageName: "test-multi-gate-pipeline"},
		{tier: "tier7-composition", packageName: "test-two-node-chain"},
		{tier: "tier7-composition", packageName: "test-wildcard-cross-flow"},
		{tier: "tier9-composition-patterns", packageName: "test-compose-accumulate-compute-branch"},
		{tier: "tier9-composition-patterns", packageName: "test-compose-clear-gates-reenter"},
		{tier: "tier9-composition-patterns", packageName: "test-compose-gate-chain-three"},
		{tier: "tier9-composition-patterns", packageName: "test-compose-gate-data-advance-emit"},
		{tier: "tier9-composition-patterns", packageName: "test-compose-guard-query-capacity"},
		{tier: "tier9-composition-patterns", packageName: "test-compose-lifecycle-seven-states"},
		{tier: "tier9-composition-patterns", packageName: "test-compose-rules-fanout-data"},
		{tier: "tier9-composition-patterns", packageName: "test-compose-rules-per-rule-data"},
	} {
		t.Run(tc.tier+"/"+tc.packageName, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			setCLIAPITestToken(t, "test-token")
			contractsPath := filepath.Join(repoRoot(), "tests", tc.tier, tc.packageName)
			scenarioPath := filepath.Join(contractsPath, "tests", "visible-smoke.yaml")
			raw, err := os.ReadFile(scenarioPath)
			if err != nil {
				t.Fatalf("read scenario: %v", err)
			}
			doc, err := parseScenarioDocument(raw)
			if err != nil {
				t.Fatalf("parse scenario: %v", err)
			}
			if len(doc.Steps) == 0 {
				t.Fatal("scenario must include at least one publish step")
			}
			for _, step := range doc.Steps {
				if step.Action != "publish" {
					t.Fatalf("scenario step = %#v, want publish", step)
				}
			}
			if len(doc.Expect.Events.Exact) != 0 || len(doc.Expect.Events.Ordered) != 0 {
				t.Fatalf("scenario event expectations = %#v, want include-only", doc.Expect.Events)
			}
			if doc.Expect.NoDeadLetters != nil {
				t.Fatalf("scenario no_dead_letters = %#v, want omitted", *doc.Expect.NoDeadLetters)
			}
			if len(doc.Expect.Events.Include) == 0 && len(doc.Expect.Entities) == 0 {
				t.Fatal("scenario must assert public event presence or entity readback")
			}
			if len(doc.Expect.Entities) > 1 {
				t.Fatalf("scenario entity expectations = %#v, want at most one entity assertion", doc.Expect.Entities)
			}
			if len(doc.Expect.Entities) == 1 && !doc.Expect.Entities[0].hasDetailAssertion() {
				t.Fatalf("scenario entity expectations = %#v, want detail assertion", doc.Expect.Entities)
			}
			assertSwarmTestScenarioThroughPublicRPC(t, contractsPath, doc)
		})
	}
}

func TestSwarmTestRunsSetupPrestateCatalogCompanions(t *testing.T) {
	for _, tc := range []struct {
		tier              string
		packageName       string
		wantPositiveEvent bool
	}{
		{tier: "tier1-primitives", packageName: "test-clear-gates", wantPositiveEvent: true},
		{tier: "tier1-primitives", packageName: "test-guard-compound-condition", wantPositiveEvent: true},
		{tier: "tier1-primitives", packageName: "test-guard-entity-ref", wantPositiveEvent: true},
		{tier: "tier1-primitives", packageName: "test-on-complete-with-state", wantPositiveEvent: true},
		{tier: "tier1-primitives", packageName: "test-payload-transform-multi-source", wantPositiveEvent: true},
		{tier: "tier10-policy-patterns", packageName: "test-policy-counter-escalate", wantPositiveEvent: true},
		{tier: "tier10-policy-patterns", packageName: "test-policy-multi-guard-partial"},
		{tier: "tier2-accumulation", packageName: "test-accumulate-all", wantPositiveEvent: true},
		{tier: "tier2-accumulation", packageName: "test-accumulate-crash-recovery", wantPositiveEvent: true},
		{tier: "tier2-accumulation", packageName: "test-accumulate-expected-from-entity", wantPositiveEvent: true},
		{tier: "tier2-accumulation", packageName: "test-accumulate-from-filter", wantPositiveEvent: true},
		{tier: "tier2-accumulation", packageName: "test-accumulate-idempotent"},
		{tier: "tier2-accumulation", packageName: "test-accumulate-on-complete-rollback", wantPositiveEvent: true},
		{tier: "tier2-accumulation", packageName: "test-accumulate-partial"},
		{tier: "tier2-accumulation", packageName: "test-accumulate-threshold", wantPositiveEvent: true},
		{tier: "tier2-accumulation", packageName: "test-accumulate-with-compute", wantPositiveEvent: true},
		{tier: "tier4-cross-entity", packageName: "test-clear-multiple-targets", wantPositiveEvent: true},
		{tier: "tier4-cross-entity", packageName: "test-clear-state", wantPositiveEvent: true},
		{tier: "tier6-event-loop", packageName: "test-atomicity-guard-rollback"},
		{tier: "tier6-event-loop", packageName: "test-guards-pre-handler-state"},
		{tier: "tier9-composition-patterns", packageName: "test-compose-guard-counter-escalate", wantPositiveEvent: true},
		{tier: "tier9-composition-patterns", packageName: "test-compose-guard-multi-source", wantPositiveEvent: true},
	} {
		t.Run(tc.tier+"/"+tc.packageName, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			setCLIAPITestToken(t, "test-token")
			contractsPath := filepath.Join(repoRoot(), "tests", tc.tier, tc.packageName)
			scenarioPath := filepath.Join(contractsPath, "tests", "visible-smoke.yaml")
			raw, err := os.ReadFile(scenarioPath)
			if err != nil {
				t.Fatalf("read scenario: %v", err)
			}
			doc, err := parseScenarioDocument(raw)
			if err != nil {
				t.Fatalf("parse scenario: %v", err)
			}
			if len(doc.Setup.Entities) != 1 {
				t.Fatalf("scenario setup = %#v, want one public setup entity", doc.Setup.Entities)
			}
			if len(doc.Steps) == 0 {
				t.Fatal("scenario must include at least one publish step")
			}
			for _, step := range doc.Steps {
				if step.Action != "publish" {
					t.Fatalf("scenario step = %#v, want publish", step)
				}
				if step.Target != nil || step.TargetEntityID != nil || step.TargetFlowInstance != nil {
					t.Fatalf("root setup companion must not use publish target fields: %#v", step)
				}
			}
			if tc.wantPositiveEvent && len(doc.Expect.Events.Include) == 0 {
				t.Fatal("scenario must include positive emitted-event proof")
			}
			if !tc.wantPositiveEvent && len(doc.Expect.Events.Include) != 0 {
				t.Fatalf("scenario includes events %#v, want no event-output assertion", doc.Expect.Events.Include)
			}
			if len(doc.Expect.Events.Exact) != 0 || len(doc.Expect.Events.Ordered) != 0 {
				t.Fatalf("scenario event expectations = %#v, want include-only", doc.Expect.Events)
			}
			if doc.Expect.NoDeadLetters != nil {
				t.Fatalf("scenario no_dead_letters = %#v, want omitted", *doc.Expect.NoDeadLetters)
			}
			if len(doc.Expect.Entities) != 1 || doc.Expect.Entities[0].Ref != "entity" || !doc.Expect.Entities[0].StateSet {
				t.Fatalf("scenario entity expectations = %#v, want setup ref current_state assertion", doc.Expect.Entities)
			}
			assertSetupPrestateVisibleManifestations(t, tc.tier, tc.packageName, contractsPath, doc.Expect.Entities[0])
			assertSwarmTestScenarioThroughPublicRPC(t, contractsPath, doc)
		})
	}
}

func assertSetupPrestateVisibleManifestations(t *testing.T, tier, packageName, contractsPath string, entityExpect scenarioEntityExpect) {
	t.Helper()
	fields, gates := loadCatalogExpectedFieldGateManifestations(t, contractsPath)
	key := tier + "/" + packageName
	if override, ok := publicScenarioFieldManifestationOverrides()[key]; ok {
		fields = override
	}
	if split, ok := splitCatalogExpectedGateManifestations()[key]; ok {
		for gate := range split {
			delete(gates, gate)
		}
	}
	if len(fields) > 0 {
		if !entityExpect.FieldsSet {
			t.Fatalf("%s expected public field manifestations %#v, but companion has no expect.entities.fields", key, fields)
		}
		if err := assertScenarioJSONEqual(key+" expect.entities.fields", entityExpect.Fields, fields); err != nil {
			t.Fatal(err)
		}
	}
	if len(gates) > 0 {
		if !entityExpect.GatesSet {
			t.Fatalf("%s expected public gate manifestations %#v, but companion has no expect.entities.gates", key, gates)
		}
		if err := assertScenarioJSONEqual(key+" expect.entities.gates", entityExpect.Gates, gates); err != nil {
			t.Fatal(err)
		}
	}
}

func loadCatalogExpectedFieldGateManifestations(t *testing.T, contractsPath string) (map[string]any, map[string]any) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(contractsPath, "expected.yaml"))
	if err != nil {
		t.Fatalf("read expected.yaml: %v", err)
	}
	var doc struct {
		Expected struct {
			EntityFields map[string]any `yaml:"entity_fields"`
			Gates        map[string]any `yaml:"gates"`
		} `yaml:"expected"`
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse expected.yaml: %v", err)
	}
	fields := cloneAnyMap(doc.Expected.EntityFields)
	gates := cloneAnyMap(doc.Expected.Gates)
	return fields, gates
}

func publicScenarioFieldManifestationOverrides() map[string]map[string]any {
	return map[string]map[string]any{
		"tier2-accumulation/test-accumulate-with-compute": {"composite": 81},
	}
}

func splitCatalogExpectedGateManifestations() map[string]map[string]string {
	return map[string]map[string]string{
		"tier1-primitives/test-clear-gates": {
			"g1_check": "private catalog gate without a declared public gate owner",
		},
	}
}

func assertSwarmTestScenarioThroughPublicRPC(t *testing.T, contractsPath string, doc scenarioDocument) {
	t.Helper()
	bundleHash := servedEventPublishFixtureBundleHash(t, contractsPath)
	for _, step := range doc.Steps {
		if step.Action != "publish" {
			t.Fatalf("scenario step = %#v, want publish", step)
		}
	}
	var entityExpect scenarioEntityExpect
	var currentState string
	fields := map[string]any{}
	gates := map[string]any{}
	if len(doc.Expect.Entities) > 1 {
		t.Fatalf("scenario entity expectations = %#v, want at most one entity assertion", doc.Expect.Entities)
	}
	if len(doc.Expect.Entities) == 1 {
		entityExpect = doc.Expect.Entities[0]
		if entityExpect.StateSet {
			state, ok := entityExpect.CurrentState.(string)
			if !ok || strings.TrimSpace(state) == "" {
				t.Fatalf("entity current_state = %#v, want string", entityExpect.CurrentState)
			}
			currentState = strings.TrimSpace(state)
		}
		if entityExpect.FieldsSet {
			fields = entityExpect.Fields
		}
		if entityExpect.GatesSet {
			gates = entityExpect.Gates
		}
	}

	var calls []jsonRPCRequest
	var publishCalls int
	activeRunID := "run-1"
	setupEntityIDs := map[string]string{}
	setupEntityTypes := map[string]string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/rpc" {
			t.Errorf("path = %q, want /v1/rpc", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-token" {
			t.Errorf("Authorization = %q, want bearer token", got)
		}
		var req jsonRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode request: %v", err)
		}
		calls = append(calls, req)
		switch req.Method {
		case scenarioTestSetupEntitiesMethod:
			if req.Params["bundle_hash"] != bundleHash {
				t.Fatalf("test.setup_entities bundle_hash = %#v, want %s", req.Params["bundle_hash"], bundleHash)
			}
			runID, _ := req.Params["run_id"].(string)
			if _, err := uuid.Parse(runID); err != nil {
				t.Fatalf("test.setup_entities run_id = %#v, want UUID", req.Params["run_id"])
			}
			activeRunID = runID
			entities, ok := req.Params["entities"].([]any)
			if !ok || len(entities) != len(doc.Setup.Entities) {
				t.Fatalf("test.setup_entities entities = %#v, want %d rows", req.Params["entities"], len(doc.Setup.Entities))
			}
			resultRows := make([]map[string]any, 0, len(entities))
			for i, raw := range entities {
				entity, ok := raw.(map[string]any)
				if !ok {
					t.Fatalf("test.setup_entities entities[%d] = %#v, want mapping", i, raw)
				}
				want := doc.Setup.Entities[i]
				if entity["alias"] != want.Alias || entity["entity_type"] != want.EntityType {
					t.Fatalf("test.setup_entities entities[%d] = %#v, want alias %q type %q", i, entity, want.Alias, want.EntityType)
				}
				entityID, _ := entity["entity_id"].(string)
				if _, err := uuid.Parse(entityID); err != nil {
					t.Fatalf("test.setup_entities entities[%d].entity_id = %#v, want UUID", i, entity["entity_id"])
				}
				if strings.TrimSpace(fmt.Sprint(entity["flow_instance"])) == "" {
					if wantID := runtimeflowidentity.EntityID(runID); entityID != wantID {
						t.Fatalf("test.setup_entities root entity_id = %q, want %q", entityID, wantID)
					}
				}
				if want.FieldsSet {
					if err := assertScenarioJSONEqual("test.setup_entities fields", entity["fields"], want.Fields); err != nil {
						t.Fatal(err)
					}
				}
				if want.GatesSet {
					if err := assertScenarioJSONEqual("test.setup_entities gates", entity["gates"], want.Gates); err != nil {
						t.Fatal(err)
					}
				}
				setupEntityIDs[want.Alias] = entityID
				setupEntityTypes[want.Alias] = want.EntityType
				resultRows = append(resultRows, map[string]any{
					"alias":         want.Alias,
					"entity_id":     entityID,
					"flow_instance": strings.TrimSpace(fmt.Sprint(entity["flow_instance"])),
					"entity_type":   want.EntityType,
					"current_state": strings.TrimSpace(fmt.Sprint(entity["current_state"])),
				})
			}
			writeJSONRPCResult(t, w, req.ID, map[string]any{"run_id": runID, "entities": resultRows})
		case eventPublishMethod:
			if publishCalls >= len(doc.Steps) {
				t.Fatalf("unexpected extra event.publish params = %#v", req.Params)
			}
			step := doc.Steps[publishCalls]
			publishCalls++
			eventID := fmt.Sprintf("event-%d", publishCalls)
			if req.Params["event_name"] != step.PublishEvent || req.Params["bundle_hash"] != bundleHash {
				t.Fatalf("event.publish params = %#v", req.Params)
			}
			if len(doc.Setup.Entities) > 0 {
				if req.Params["run_id"] != activeRunID {
					t.Fatalf("event.publish[%d] run_id = %#v, want setup run %s; params=%#v", publishCalls, req.Params["run_id"], activeRunID, req.Params)
				}
			} else if publishCalls == 1 {
				if _, ok := req.Params["run_id"]; ok {
					t.Fatalf("first event.publish unexpectedly sent run_id: %#v", req.Params)
				}
				if _, ok := req.Params["source_event_id"]; ok && step.SourceEventID == nil {
					t.Fatalf("first event.publish unexpectedly sent source_event_id: %#v", req.Params)
				}
			} else {
				if req.Params["run_id"] != activeRunID {
					t.Fatalf("event.publish[%d] run_id = %#v, want run-1; params=%#v", publishCalls, req.Params["run_id"], req.Params)
				}
			}
			if publishCalls > 1 {
				wantSource := fmt.Sprintf("event-%d", publishCalls-1)
				if step.SourceEventID == nil && req.Params["source_event_id"] != wantSource {
					t.Fatalf("event.publish[%d] source_event_id = %#v, want %#v; params=%#v", publishCalls, req.Params["source_event_id"], wantSource, req.Params)
				}
			}
			if step.SourceEventID != nil {
				wantSource := strings.TrimSpace(fmt.Sprint(step.SourceEventID))
				if wantSource == "" {
					if _, ok := req.Params["source_event_id"]; ok {
						t.Fatalf("event.publish[%d] source_event_id = %#v, want omitted", publishCalls, req.Params["source_event_id"])
					}
				} else if req.Params["source_event_id"] != wantSource {
					t.Fatalf("event.publish[%d] source_event_id = %#v, want %#v; params=%#v", publishCalls, req.Params["source_event_id"], wantSource, req.Params)
				}
			}
			wantPayload, ok := step.Payload.(map[string]any)
			if !ok {
				t.Fatalf("scenario payload = %#v, want mapping", step.Payload)
			}
			gotPayload, ok := req.Params["payload"].(map[string]any)
			if !ok {
				t.Fatalf("event.publish payload = %#v, want mapping", req.Params["payload"])
			}
			if err := assertScenarioJSONEqual("event.publish payload", gotPayload, wantPayload); err != nil {
				t.Fatal(err)
			}
			if step.Emitter != nil {
				wantEmitter, ok := step.Emitter.(string)
				if !ok {
					t.Fatalf("scenario emitter = %#v, want string", step.Emitter)
				}
				if req.Params["emitter"] != strings.TrimSpace(wantEmitter) {
					t.Fatalf("event.publish emitter = %#v, want %#v", req.Params["emitter"], step.Emitter)
				}
			} else if _, ok := req.Params["emitter"]; ok {
				t.Fatalf("event.publish unexpectedly sent emitter: %#v", req.Params)
			}
			result := eventPublishTestResult(len(doc.Setup.Entities) == 0 && publishCalls == 1)
			result["event_id"] = eventID
			result["run_id"] = activeRunID
			if source, ok := req.Params["source_event_id"].(string); ok {
				result["source_event_id"] = source
			} else {
				result["source_event_id"] = ""
			}
			writeJSONRPCResult(t, w, req.ID, result)
		case "run.diagnose":
			writeJSONRPCResult(t, w, req.ID, scenarioRunDiagnoseTestResult(activeRunID, true))
		case "run.trace":
			rows := make([]map[string]any, 0, len(doc.Steps)+len(doc.Expect.Events.Include))
			for i, step := range doc.Steps {
				rows = append(rows, scenarioTraceRowForEvent(fmt.Sprintf("event-%d", i+1), step.PublishEvent))
			}
			for i, eventName := range doc.Expect.Events.Include {
				rows = append(rows, scenarioTraceRowForEvent(fmt.Sprintf("observed-%d", i+1), eventName))
			}
			writeJSONRPCResult(t, w, req.ID, map[string]any{"trace": rows})
		case entityListMethod:
			if req.Params["run_id"] != activeRunID || req.Params["type"] != entityExpect.EntityType {
				t.Fatalf("entity.list params = %#v", req.Params)
			}
			entity := validEntitySummary("entity-1")
			entity["run_id"] = activeRunID
			entity["entity_type"] = entityExpect.EntityType
			entity["current_state"] = currentState
			writeJSONRPCResult(t, w, req.ID, map[string]any{"entities": []map[string]any{entity}})
		case entityGetMethod:
			entityID := "entity-1"
			entityType := entityExpect.EntityType
			if entityExpect.Ref != "" {
				var ok bool
				entityID, ok = setupEntityIDs[entityExpect.Ref]
				if !ok {
					t.Fatalf("entity expectation ref %q missing setup binding", entityExpect.Ref)
				}
				entityType = setupEntityTypes[entityExpect.Ref]
			}
			if req.Params["entity_id"] != entityID || req.Params["run_id"] != activeRunID {
				t.Fatalf("entity.get params = %#v, want run %s entity %s", req.Params, activeRunID, entityID)
			}
			result := validEntityFullResult(entityID)
			entity := result["entity"].(map[string]any)
			entity["run_id"] = activeRunID
			entity["entity_type"] = entityType
			entity["current_state"] = currentState
			result["fields"] = fields
			result["gates"] = gates
			writeJSONRPCResult(t, w, req.ID, result)
		default:
			t.Fatalf("unexpected method = %s", req.Method)
		}
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), repoRoot(), []string{
		"test",
		"--contracts", contractsPath,
		"--platform-spec", defaultPlatformSpecPath,
		"--timeout", "2s",
		"--poll-interval", "10ms",
	}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	wantMethods := make([]string, 0, len(doc.Steps)*2+4)
	if len(doc.Setup.Entities) > 0 {
		wantMethods = append(wantMethods, scenarioTestSetupEntitiesMethod)
	}
	for range doc.Steps {
		wantMethods = append(wantMethods, eventPublishMethod, "run.diagnose")
	}
	wantMethods = append(wantMethods, "run.diagnose", "run.trace")
	if len(doc.Expect.Entities) > 0 {
		if doc.Expect.Entities[0].Ref == "" {
			wantMethods = append(wantMethods, entityListMethod)
		}
		wantMethods = append(wantMethods, entityGetMethod)
	}
	assertScenarioTestMethods(t, calls, wantMethods)
	for _, want := range []string{"scenario ok:", "swarm test ok: scenarios=1"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout.String())
		}
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func scenarioTraceRowForEvent(eventID, eventName string) map[string]any {
	row := validRunCommandTraceRow(eventID)
	row["event_name"] = eventName
	return row
}

func TestSwarmTestRejectsInvalidFixtureSchemaBeforePublish(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	setCLIAPITestToken(t, "test-token")
	contractsPath := writeServedEventPublishFollowUpFixture(t)
	writeWorkflowValidationFixtureFile(t, filepath.Join(contractsPath, "tests", "invalid-type.yaml"), `
name: invalid type fixture
steps:
  - publish: thing.created
    payload:
      amount: not-an-integer
      who: operator
`)
	var called bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		writeJSONRPCResult(t, w, "unexpected", map[string]any{})
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), repoRoot(), []string{
		"test",
		filepath.Join(contractsPath, "tests", "invalid-type.yaml"),
		"--contracts", contractsPath,
		"--platform-spec", defaultPlatformSpecPath,
	}, &stdout, &stderr, testRootCommandOptions(server))
	if code != scenarioTestExitValidation {
		t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, scenarioTestExitValidation, stdout.String(), stderr.String())
	}
	if called {
		t.Fatal("event.publish API was called for schema-invalid fixture")
	}
	if !strings.Contains(stderr.String(), "amount must be integer") {
		t.Fatalf("stderr = %q, want integer schema failure", stderr.String())
	}
}

func TestSwarmTestRejectsInvalidSetupBeforeRPC(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	setCLIAPITestToken(t, "test-token")
	contractsPath := writeScenarioSetupFixture(t)
	scenarioPath := filepath.Join(contractsPath, "flows", "operating", "tests", "bad-setup.yaml")
	writeWorkflowValidationFixtureFile(t, scenarioPath, `
name: bad setup
setup:
  entities:
    - as: product
      type: product
      current_state: waiting
      fields: {missing: value}
steps:
  - publish: opco.product_review_requested
    target: product
    payload: {note: approved}
`)
	var called bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		writeJSONRPCResult(t, w, "unexpected", map[string]any{})
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), repoRoot(), []string{
		"test",
		scenarioPath,
		"--contracts", contractsPath,
		"--platform-spec", defaultPlatformSpecPath,
	}, &stdout, &stderr, testRootCommandOptions(server))
	if code != scenarioTestExitValidation {
		t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, scenarioTestExitValidation, stdout.String(), stderr.String())
	}
	if called {
		t.Fatal("setup API was called for invalid setup field")
	}
	if !strings.Contains(stderr.String(), "fields.missing: undeclared field missing") {
		t.Fatalf("stderr = %q, want undeclared field failure", stderr.String())
	}
}

func TestScenarioSetupParserRejectsAmbiguousSetupForms(t *testing.T) {
	for _, raw := range []string{
		`
setup:
  entities:
    - as: product
      type: product
    - as: product
      type: product
steps:
  - publish: opco.product_review_requested
    payload: {note: approved}
`,
		`
setup:
  entities:
    - as: product
      type: product
steps:
  - publish: opco.product_review_requested
    target: product
    target_entity_id: 00000000-0000-0000-0000-000000000001
    target_flow_instance: operating
    payload: {note: approved}
`,
		`
steps:
  - publish: opco.product_review_requested
    payload: {note: approved}
expect:
  entities:
    - ref: product
      count: 1
`,
	} {
		if _, err := parseScenarioDocument([]byte(raw)); err == nil {
			t.Fatalf("parseScenarioDocument unexpectedly accepted:\n%s", raw)
		}
	}
}

func TestSwarmTestRejectsScenariosOutsideSupportedRoots(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	setCLIAPITestToken(t, "test-token")
	contractsPath := writeServedEventPublishFollowUpFixture(t)
	outside := filepath.Join(contractsPath, "private.yaml")
	writeWorkflowValidationFixtureFile(t, outside, `
name: private scenario
steps:
  - publish: thing.created
    payload: {amount: 7, who: operator}
`)

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), repoRoot(), []string{
		"test",
		outside,
		"--contracts", contractsPath,
		"--platform-spec", defaultPlatformSpecPath,
	}, &stdout, &stderr, defaultRootCommandOptions())
	if code != scenarioTestExitValidation {
		t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, scenarioTestExitValidation, stdout.String(), stderr.String())
	}
	if !strings.Contains(stderr.String(), "outside supported roots") {
		t.Fatalf("stderr = %q, want supported-root failure", stderr.String())
	}
}

func TestSwarmTestRejectsSymlinkFixtureEscapeBeforePublish(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	setCLIAPITestToken(t, "test-token")
	contractsPath := writeServedEventPublishFollowUpFixture(t)
	outside := filepath.Join(t.TempDir(), "outside.yaml")
	writeWorkflowValidationFixtureFile(t, outside, `
amount: 7
who: outside
`)
	fixtureDir := filepath.Join(contractsPath, "tests", "fixtures")
	if err := os.MkdirAll(fixtureDir, 0o755); err != nil {
		t.Fatalf("mkdir fixture dir: %v", err)
	}
	if err := os.Symlink(outside, filepath.Join(fixtureDir, "outside.yaml")); err != nil {
		t.Skipf("symlink unsupported: %v", err)
	}
	writeWorkflowValidationFixtureFile(t, filepath.Join(contractsPath, "tests", "symlink-escape.yaml"), `
name: symlink escape
steps:
  - publish: thing.created
    payload:
      from: fixtures/outside.yaml
`)
	var called bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		writeJSONRPCResult(t, w, "unexpected", map[string]any{})
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), repoRoot(), []string{
		"test",
		filepath.Join(contractsPath, "tests", "symlink-escape.yaml"),
		"--contracts", contractsPath,
		"--platform-spec", defaultPlatformSpecPath,
	}, &stdout, &stderr, testRootCommandOptions(server))
	if code != scenarioTestExitValidation {
		t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, scenarioTestExitValidation, stdout.String(), stderr.String())
	}
	if called {
		t.Fatal("event.publish API was called for symlink-escaped fixture")
	}
	if !strings.Contains(stderr.String(), "escapes contract package root") {
		t.Fatalf("stderr = %q, want contract-root escape failure", stderr.String())
	}
}

func TestSwarmTestMailboxApproveFindsExactlyOneThenMutates(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	setCLIAPITestToken(t, "test-token")
	contractsPath := writeServedEventPublishFollowUpFixture(t)
	writeWorkflowValidationFixtureFile(t, filepath.Join(contractsPath, "tests", "mailbox.yaml"), `
name: mailbox scenario
steps:
  - publish: thing.created
    payload: {amount: 7, who: operator}
  - mailbox.approve:
      match:
        type: review_request
      payload:
        approved: true
`)
	var calls []jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req jsonRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode request: %v", err)
		}
		calls = append(calls, req)
		switch req.Method {
		case eventPublishMethod:
			writeJSONRPCResult(t, w, req.ID, eventPublishTestResult(true))
		case "run.diagnose":
			writeJSONRPCResult(t, w, req.ID, scenarioRunDiagnoseTestResult("run-1", true))
		case "mailbox.list":
			writeJSONRPCResult(t, w, req.ID, map[string]any{"items": []map[string]any{mailboxItemResult("mailbox-1", "pending", "normal")}})
		case "mailbox.approve":
			want := map[string]any{
				"mailbox_id":       "mailbox-1",
				"decision_payload": map[string]any{"approved": true},
			}
			if !reflect.DeepEqual(req.Params, want) {
				t.Fatalf("mailbox.approve params = %#v, want %#v", req.Params, want)
			}
			writeJSONRPCResult(t, w, req.ID, map[string]any{
				"ok":                           true,
				"mailbox_decision_id":          "decision-1",
				"status":                       "decided",
				"idempotency_replayed":         false,
				"downstream_event_id":          "event-2",
				"downstream_event_name":        "thing.reviewed",
				"downstream_subscribers":       []string{},
				"downstream_subscriber_source": "none",
			})
		default:
			t.Fatalf("unexpected method = %s", req.Method)
		}
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), repoRoot(), []string{
		"test",
		filepath.Join(contractsPath, "tests", "mailbox.yaml"),
		"--contracts", contractsPath,
		"--platform-spec", defaultPlatformSpecPath,
		"--timeout", "2s",
		"--poll-interval", "10ms",
	}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	assertScenarioTestMethods(t, calls, []string{
		eventPublishMethod,
		"run.diagnose",
		"mailbox.list",
		"mailbox.approve",
		"run.diagnose",
		"run.diagnose",
	})
}

func TestSwarmTestMailboxRejectMissingReasonFailsBeforeMailboxLookup(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	setCLIAPITestToken(t, "test-token")
	contractsPath := writeServedEventPublishFollowUpFixture(t)
	writeWorkflowValidationFixtureFile(t, filepath.Join(contractsPath, "tests", "mailbox-reject-missing-reason.yaml"), `
name: mailbox reject missing reason
steps:
  - publish: thing.created
    payload: {amount: 7, who: operator}
  - mailbox.reject:
      match:
        type: review_request
`)
	var calls []jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req jsonRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode request: %v", err)
		}
		calls = append(calls, req)
		switch req.Method {
		case eventPublishMethod:
			writeJSONRPCResult(t, w, req.ID, eventPublishTestResult(true))
		case "run.diagnose":
			writeJSONRPCResult(t, w, req.ID, scenarioRunDiagnoseTestResult("run-1", true))
		default:
			t.Fatalf("unexpected method before reject validation = %s", req.Method)
		}
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), repoRoot(), []string{
		"test",
		filepath.Join(contractsPath, "tests", "mailbox-reject-missing-reason.yaml"),
		"--contracts", contractsPath,
		"--platform-spec", defaultPlatformSpecPath,
		"--timeout", "2s",
		"--poll-interval", "10ms",
	}, &stdout, &stderr, testRootCommandOptions(server))
	if code != scenarioTestExitValidation {
		t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, scenarioTestExitValidation, stdout.String(), stderr.String())
	}
	assertScenarioTestMethods(t, calls, []string{eventPublishMethod, "run.diagnose"})
	if !strings.Contains(stderr.String(), "mailbox.reject reason is required") {
		t.Fatalf("stderr = %q, want reject reason validation failure", stderr.String())
	}
}

func TestSwarmTestServedSQLiteNoLiveLLMProof(t *testing.T) {
	unsetStoreSelectorEnv(t)
	stubServeRuntimeWorkspaceLifecycle(t)
	sqlitePath := filepath.Join(t.TempDir(), ".swarm", "dev.db")
	t.Setenv(storebackend.EnvSQLitePath, sqlitePath)
	contractsPath := writeScenarioRunnerFixture(t)
	endpoint, _ := startServedEventPublishFollowUpRuntime(t, serveOptions{
		ConfigPath:              writeServeRuntimeTestConfig(t),
		ContractsPath:           contractsPath,
		PlatformSpecPath:        defaultPlatformSpecPath,
		APIListenAddr:           "127.0.0.1:0",
		MCPListenAddr:           "127.0.0.1:0",
		SelfCheck:               true,
		RequireBundleMatch:      false,
		NoRequireBundleMatch:    true,
		Verbose:                 true,
		TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(),
	})

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), repoRoot(), []string{
		"test",
		"--contracts", contractsPath,
		"--platform-spec", defaultPlatformSpecPath,
		"--api-server", strings.TrimSuffix(endpoint, "/v1/rpc"),
		"--timeout", "10s",
		"--poll-interval", "25ms",
	}, &stdout, &stderr, defaultRootCommandOptions())
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if !strings.Contains(stdout.String(), "swarm test ok: scenarios=1") {
		t.Fatalf("stdout missing success:\n%s", stdout.String())
	}
}

func TestScenarioEventExpectationsDeduplicateRunTraceDeliveryRows(t *testing.T) {
	rows := []diagnosticRunTraceRow{
		{EventID: "event-1", EventName: "thing.created", DeliveryID: "delivery-1"},
		{EventID: "event-1", EventName: "thing.created", DeliveryID: "delivery-2"},
		{EventID: "event-2", EventName: "thing.done", DeliveryID: "delivery-3"},
	}
	names := uniqueScenarioTraceEventNames(rows)
	if got := strings.Join(names, ","); got != "thing.created,thing.done" {
		t.Fatalf("unique names = %q", got)
	}
	if err := assertScenarioEventExpectations(names, scenarioEventExpect{
		Exact:   []string{"thing.created", "thing.done"},
		Ordered: []string{"thing.created", "thing.done"},
	}); err != nil {
		t.Fatalf("event expectations over deduplicated trace rows failed: %v", err)
	}
}

func TestScenarioEvaluatorSeedIsContractRelativeAndRecorded(t *testing.T) {
	rootA := t.TempDir()
	rootB := t.TempDir()
	fileA := scenarioTestFile{Path: filepath.Join(rootA, "tests", "same.yaml")}
	fileB := scenarioTestFile{Path: filepath.Join(rootB, "tests", "same.yaml")}
	doc := scenarioDocument{Name: "portable", Seed: "recorded-seed"}
	seedA, err := (scenarioRunner{contractsDir: rootA}).scenarioEvaluatorSeed(fileA, doc)
	if err != nil {
		t.Fatalf("seed A: %v", err)
	}
	seedB, err := (scenarioRunner{contractsDir: rootB}).scenarioEvaluatorSeed(fileB, doc)
	if err != nil {
		t.Fatalf("seed B: %v", err)
	}
	if seedA != seedB {
		t.Fatalf("seed differs across absolute roots:\nA=%q\nB=%q", seedA, seedB)
	}
	evalA, err := newScenarioExpressionEvaluator(seedA, nil)
	if err != nil {
		t.Fatalf("evaluator A: %v", err)
	}
	evalB, err := newScenarioExpressionEvaluator(seedB, nil)
	if err != nil {
		t.Fatalf("evaluator B: %v", err)
	}
	idA, err := evalA.evalExpression(`scenario.uuid("publish")`)
	if err != nil {
		t.Fatalf("uuid A: %v", err)
	}
	idB, err := evalB.evalExpression(`scenario.uuid("publish")`)
	if err != nil {
		t.Fatalf("uuid B: %v", err)
	}
	if idA != idB {
		t.Fatalf("uuid differs across absolute roots: %v vs %v", idA, idB)
	}
}

func TestScenarioEntityExpectationConsumesAllPages(t *testing.T) {
	var calls []jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req jsonRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode request: %v", err)
		}
		calls = append(calls, req)
		entity := validEntitySummary("entity-1")
		entity["entity_type"] = "widget"
		switch len(calls) {
		case 1:
			writeJSONRPCResult(t, w, req.ID, map[string]any{"entities": []map[string]any{entity}, "next_cursor": "page-2"})
		case 2:
			entity["entity_id"] = "entity-2"
			writeJSONRPCResult(t, w, req.ID, map[string]any{"entities": []map[string]any{entity}})
		default:
			t.Fatalf("unexpected extra request %#v", req)
		}
	}))
	defer server.Close()
	client, err := newCLIAPIClient(rootCommandOptions{apiServer: strings.TrimSuffix(server.URL, "/")})
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	runner := scenarioRunner{client: client}
	state := &scenarioRunState{RunID: "run-1", SetupEntities: map[string]scenarioSetupEntityBinding{}}
	if err := runner.assertEntityExpectation(context.Background(), state, mustScenarioExpressionEvaluator(t, nil), scenarioEntityExpect{EntityType: "widget", Count: intPtr(2)}); err != nil {
		t.Fatalf("entity expectation: %v", err)
	}
	assertScenarioTestMethods(t, calls, []string{entityListMethod, entityListMethod})
	if calls[1].Params["cursor"] != "page-2" {
		t.Fatalf("second entity.list cursor = %#v", calls[1].Params)
	}
}

func TestScenarioEntityExpectationConsumesCanonicalDetail(t *testing.T) {
	var calls []jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req jsonRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode request: %v", err)
		}
		calls = append(calls, req)
		switch req.Method {
		case entityListMethod:
			if req.Params["run_id"] != "run-1" || req.Params["type"] != "widget" {
				t.Fatalf("entity.list params = %#v", req.Params)
			}
			entity := validEntitySummary("entity-1")
			entity["entity_type"] = "widget"
			entity["current_state"] = "done"
			writeJSONRPCResult(t, w, req.ID, map[string]any{"entities": []map[string]any{entity}})
		case entityGetMethod:
			if req.Params["entity_id"] != "entity-1" || req.Params["run_id"] != "run-1" {
				t.Fatalf("entity.get params = %#v", req.Params)
			}
			result := validEntityFullResult("entity-1")
			entity := result["entity"].(map[string]any)
			entity["entity_type"] = "widget"
			entity["current_state"] = "done"
			writeJSONRPCResult(t, w, req.ID, result)
		default:
			t.Fatalf("unexpected request %#v", req)
		}
	}))
	defer server.Close()
	client, err := newCLIAPIClient(rootCommandOptions{apiServer: strings.TrimSuffix(server.URL, "/")})
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	runner := scenarioRunner{client: client}
	evaluator := mustScenarioExpressionEvaluator(t, map[string]any{
		"done_state": "done",
		"score":      7,
		"ready":      true,
	})
	state := &scenarioRunState{RunID: "run-1", SetupEntities: map[string]scenarioSetupEntityBinding{}}
	if err := runner.assertEntityExpectation(context.Background(), state, evaluator, scenarioEntityExpect{
		EntityType:   "widget",
		CurrentState: "${vars.done_state}",
		StateSet:     true,
		Fields:       map[string]any{"score": "${vars.score}"},
		FieldsSet:    true,
		Gates:        map[string]any{"ready": "${vars.ready}"},
		GatesSet:     true,
	}); err != nil {
		t.Fatalf("entity detail expectation: %v", err)
	}
	assertScenarioTestMethods(t, calls, []string{entityListMethod, entityGetMethod})
}

func TestScenarioEntityDetailExpectationFailsClosedOnMultipleMatches(t *testing.T) {
	var calls []jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req jsonRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode request: %v", err)
		}
		calls = append(calls, req)
		if req.Method != entityListMethod {
			t.Fatalf("unexpected request %#v", req)
		}
		entity1 := validEntitySummary("entity-1")
		entity1["entity_type"] = "widget"
		entity2 := validEntitySummary("entity-2")
		entity2["entity_type"] = "widget"
		writeJSONRPCResult(t, w, req.ID, map[string]any{"entities": []map[string]any{entity1, entity2}})
	}))
	defer server.Close()
	client, err := newCLIAPIClient(rootCommandOptions{apiServer: strings.TrimSuffix(server.URL, "/")})
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	runner := scenarioRunner{client: client}
	state := &scenarioRunState{RunID: "run-1", SetupEntities: map[string]scenarioSetupEntityBinding{}}
	err = runner.assertEntityExpectation(context.Background(), state, mustScenarioExpressionEvaluator(t, nil), scenarioEntityExpect{
		EntityType:   "widget",
		CurrentState: "done",
		StateSet:     true,
	})
	if err == nil || !strings.Contains(err.Error(), "returned 2 entities, want exactly one") {
		t.Fatalf("entity detail expectation error = %v, want multiple-match failure", err)
	}
	assertScenarioTestMethods(t, calls, []string{entityListMethod})
}

func TestScenarioEntityExpectationRejectsAmbiguousRows(t *testing.T) {
	for _, raw := range []string{
		`
steps:
  - publish: thing.created
    payload: {amount: 7, who: operator}
expect:
  entities:
    - type: widget
`,
		`
steps:
  - publish: thing.created
    payload: {amount: 7, who: operator}
expect:
  entities:
    - type: widget
      count: 1
      current_state: done
`,
	} {
		if _, err := parseScenarioDocument([]byte(raw)); err == nil {
			t.Fatalf("parseScenarioDocument(%s) unexpectedly succeeded", raw)
		}
	}
}

func writeScenarioRunnerFixture(t *testing.T) string {
	t.Helper()
	contractsPath := writeServedEventPublishFollowUpFixture(t)
	writeWorkflowValidationFixtureFile(t, filepath.Join(contractsPath, "tests", "fixtures", "thing-created.yaml"), `
amount: 7
who: fixture
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(contractsPath, "tests", "empire-routing.yaml"), `
name: empire-style deterministic routing
vars:
  who: operator
steps:
  - publish: thing.created
    idempotency_key: ${scenario.sha40("empire-cost-router")}
    payload:
      from: fixtures/thing-created.yaml
      set:
        who: ${vars.who}
        amount: 7
  - publish: thing.reviewed
    payload:
      note: approved
invalid:
  base:
    publish: thing.created
    payload:
      from: fixtures/thing-created.yaml
  cases:
    - name: invalid-amount
      set:
        payload.amount: not-an-integer
      expect: reject
expect:
  events:
    include: [thing.created, thing.reviewed]
  no_dead_letters: true
  entities:
    - type: widget
      current_state: done
`)
	return contractsPath
}

func writeScenarioSetupFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: scenario-setup-fixture
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: operating
    flow: operating
    mode: static
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "schema.yaml"), `
name: scenario-setup-fixture
initial_state: new
terminal_states: [done]
states: [new, done]
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "entities.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "events.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "nodes.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "policy.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "tools.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "agents.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "operating", "schema.yaml"), `
name: operating
mode: static
initial_state: initializing
terminal_states: [ready]
states: [initializing, waiting, ready]
pins:
  inputs:
    events: [opco.product_review_requested]
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "operating", "entities.yaml"), `
product:
  product_id: text
  note: text
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "operating", "events.yaml"), `
opco.product_review_requested:
  swarm:
    source: external
  note: text
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "operating", "nodes.yaml"), `
reviewer:
  id: reviewer
  execution_type: system_node
  subscribes_to: [opco.product_review_requested]
  gate_state:
    gates: [review_ready]
  event_handlers:
    opco.product_review_requested:
      data_accumulation:
        source_event: opco.product_review_requested
        writes:
          - source_field: note
            target_field: note
      clear_gates: [review_ready]
      advances_to: ready
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "operating", "policy.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "operating", "tools.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "operating", "agents.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "flows", "operating", "tests", "setup-target.yaml"), `
name: setup target and expectation
setup:
  entities:
    - as: product
      type: product
      current_state: waiting
      fields: {product_id: p-1, note: seeded}
      gates: {review_ready: true}
steps:
  - publish: opco.product_review_requested
    target: product
    payload: {note: approved}
expect:
  entities:
    - ref: product
      current_state: ready
      fields: {product_id: p-1, note: approved}
      gates: {review_ready: false}
`)
	return root
}

func writeScenarioRootSetupFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: scenario-root-setup-fixture
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "schema.yaml"), `
name: scenario-root-setup-fixture
initial_state: waiting
terminal_states: [done]
states: [waiting, done]
pins:
  inputs:
    events: [widget.scored]
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "entities.yaml"), `
widget:
  score: integer
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "events.yaml"), `
widget.scored:
  swarm:
    source: external
  delta: integer
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "nodes.yaml"), `
scorer:
  id: scorer
  execution_type: system_node
  subscribes_to: [widget.scored]
  event_handlers:
    widget.scored:
      data_accumulation:
        source_event: widget.scored
        writes:
          - target_field: score
            expression: entity.score + payload.delta
      advances_to: done
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "policy.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "tools.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "agents.yaml"), `{}`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "tests", "root-setup.yaml"), `
name: root setup and expectation
setup:
  entities:
    - as: widget
      type: widget
      current_state: waiting
      fields: {score: 5}
steps:
  - publish: widget.scored
    payload: {delta: 7}
expect:
  entities:
    - ref: widget
      current_state: done
      fields: {score: 12}
`)
	return root
}

func scenarioRunDiagnoseTestResult(runID string, ready bool) map[string]any {
	activeDeliveries := 0
	if !ready {
		activeDeliveries = 1
	}
	return map[string]any{
		"run":               validDiagnosticRunHeader(runID),
		"operational_state": "running",
		"blocking_layer":    "",
		"blocking_reason":   "",
		"heuristics":        []string{},
		"failed_deliveries": []any{},
		"test_quiescence": map[string]any{
			"ready":                     ready,
			"active_deliveries":         activeDeliveries,
			"unsettled_pipeline_events": 0,
			"due_timers":                0,
			"active_session_leases":     0,
		},
	}
}

func intPtr(value int) *int {
	return &value
}

func mustScenarioExpressionEvaluator(t *testing.T, vars map[string]any) *scenarioExpressionEvaluator {
	t.Helper()
	evaluator, err := newScenarioExpressionEvaluator("test-seed", vars)
	if err != nil {
		t.Fatalf("newScenarioExpressionEvaluator: %v", err)
	}
	return evaluator
}

func assertScenarioTestMethods(t *testing.T, calls []jsonRPCRequest, want []string) {
	t.Helper()
	if len(calls) != len(want) {
		t.Fatalf("methods = %v, want %v", scenarioTestMethodNames(calls), want)
	}
	for i, req := range calls {
		if req.Method != want[i] {
			t.Fatalf("method[%d] = %q, want %q; all=%v", i, req.Method, want[i], scenarioTestMethodNames(calls))
		}
	}
}

func scenarioTestMethodNames(calls []jsonRPCRequest) []string {
	out := make([]string, 0, len(calls))
	for _, req := range calls {
		out = append(out, req.Method)
	}
	return out
}

func numberEquals(value any, want int) bool {
	switch typed := value.(type) {
	case int:
		return typed == want
	case int64:
		return typed == int64(want)
	case float64:
		return typed == float64(want)
	default:
		return false
	}
}
