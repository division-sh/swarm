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

func assertSwarmTestScenarioThroughPublicRPC(t *testing.T, contractsPath string, doc scenarioDocument) {
	t.Helper()
	bundleHash := servedEventPublishFixtureBundleHash(t, contractsPath)
	step := doc.Steps[0]
	entityExpect := doc.Expect.Entities[0]
	currentState, ok := entityExpect.CurrentState.(string)
	if !ok || strings.TrimSpace(currentState) == "" {
		t.Fatalf("entity current_state = %#v, want string", entityExpect.CurrentState)
	}
	fields := map[string]any{}
	if entityExpect.FieldsSet {
		fields = entityExpect.Fields
	}
	gates := map[string]any{}
	if entityExpect.GatesSet {
		gates = entityExpect.Gates
	}

	var calls []jsonRPCRequest
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
			if req.Params["event_name"] != step.PublishEvent || req.Params["bundle_hash"] != bundleHash {
				t.Fatalf("event.publish params = %#v", req.Params)
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
			writeJSONRPCResult(t, w, req.ID, eventPublishTestResult(true))
		case "run.diagnose":
			writeJSONRPCResult(t, w, req.ID, scenarioRunDiagnoseTestResult("run-1", true))
		case "run.trace":
			rows := []map[string]any{scenarioTraceRowForEvent("event-1", step.PublishEvent)}
			for i, eventName := range doc.Expect.Events.Include {
				rows = append(rows, scenarioTraceRowForEvent(fmt.Sprintf("event-%d", i+2), eventName))
			}
			writeJSONRPCResult(t, w, req.ID, map[string]any{"trace": rows})
		case entityListMethod:
			if req.Params["run_id"] != "run-1" || req.Params["type"] != entityExpect.EntityType {
				t.Fatalf("entity.list params = %#v", req.Params)
			}
			entity := validEntitySummary("entity-1")
			entity["entity_type"] = entityExpect.EntityType
			entity["current_state"] = strings.TrimSpace(currentState)
			writeJSONRPCResult(t, w, req.ID, map[string]any{"entities": []map[string]any{entity}})
		case entityGetMethod:
			if req.Params["entity_id"] != "entity-1" || req.Params["run_id"] != "run-1" {
				t.Fatalf("entity.get params = %#v", req.Params)
			}
			result := validEntityFullResult("entity-1")
			entity := result["entity"].(map[string]any)
			entity["entity_type"] = entityExpect.EntityType
			entity["current_state"] = strings.TrimSpace(currentState)
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
	if err := runner.assertEntityExpectation(context.Background(), "run-1", mustScenarioExpressionEvaluator(t, nil), scenarioEntityExpect{EntityType: "widget", Count: intPtr(2)}); err != nil {
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
	if err := runner.assertEntityExpectation(context.Background(), "run-1", evaluator, scenarioEntityExpect{
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
	err = runner.assertEntityExpectation(context.Background(), "run-1", mustScenarioExpressionEvaluator(t, nil), scenarioEntityExpect{
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
