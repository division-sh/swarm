package cliapp

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	apiv1 "github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func TestEntitiesListUsesEntityListV1RPCWithFilters(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	var captured jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/rpc" {
			t.Errorf("path = %q, want /v1/rpc", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-token" {
			t.Errorf("Authorization = %q, want bearer token", got)
		}
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Errorf("decode request: %v", err)
		}
		writeJSONRPCResult(t, w, captured.ID, map[string]any{
			"entities":    []map[string]any{validEntitySummary("entity-1")},
			"next_cursor": "entity-cursor-2",
		})
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{
		"entity", "list",
		"--run-id", "run-1",
		"--flow", "review",
		"--type", "vertical",
		"--current-state", "collecting",
		"--limit", "25",
		"--cursor", "cursor-1",
	}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if captured.JSONRPC != "2.0" || captured.Method != entityListMethod {
		t.Fatalf("request jsonrpc/method = %s/%s, want 2.0/%s", captured.JSONRPC, captured.Method, entityListMethod)
	}
	wantParams := map[string]any{
		"run_id":        "run-1",
		"flow":          "review",
		"type":          "vertical",
		"current_state": "collecting",
		"limit":         float64(25),
		"cursor":        "cursor-1",
	}
	if !reflect.DeepEqual(captured.Params, wantParams) {
		t.Fatalf("params = %#v, want %#v", captured.Params, wantParams)
	}
	for _, want := range []string{"ENTITY_ID", "entity-1", "review", "vertical", "collecting", "next_cursor=entity-cursor-2"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout.String())
		}
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestEntitiesListEmptyResultOmitsUnsetParams(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	var captured jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Errorf("decode request: %v", err)
		}
		writeJSONRPCResult(t, w, captured.ID, map[string]any{"entities": []any{}})
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"entity", "list"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if captured.Method != entityListMethod {
		t.Fatalf("method = %q, want %s", captured.Method, entityListMethod)
	}
	if len(captured.Params) != 0 {
		t.Fatalf("params = %#v, want empty", captured.Params)
	}
	if !strings.Contains(stdout.String(), "No entities match the current filters.") {
		t.Fatalf("stdout = %q, want empty-state text", stdout.String())
	}
}

func TestEntityCommandsUseSQLiteEntityReadStoreThroughV1API(t *testing.T) {
	ctx := context.Background()
	setCLIAPITestToken(t, "test-token")
	sqliteStore := storetest.StartSQLiteRuntimeStore(t)
	runID := "11111111-1111-1111-1111-111111111111"
	entityA := "22222222-2222-2222-2222-222222222222"
	entityB := "33333333-3333-3333-3333-333333333333"
	now := time.Unix(1700000000, 0).UTC()
	storetest.RequireSQLiteRun(t, ctx, storetest.Database(sqliteStore), storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: runID, StartedAt: now})
	if _, err := storetest.Database(sqliteStore).ExecContext(ctx, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, current_state,
			gates, fields, accumulator, revision, entered_state_at, created_at, updated_at
		) VALUES
			(?, ?, 'scoring/vertical-a', 'vertical', 'discovered',
			 '{}', '{"vertical_name":"Healthcare"}', '{}', 1, ?, ?, ?),
			(?, ?, 'scoring/vertical-b', 'vertical', 'pending',
			 '{}', '{"vertical_name":"Manufacturing"}', '{}', 1, ?, ?, ?)
	`, runID, entityA, now, now, now, runID, entityB, now, now, now); err != nil {
		t.Fatalf("seed sqlite entity_state: %v", err)
	}
	registry, err := apiv1.LoadRegistry(ResolvePath(RepoRoot(), defaultPlatformSpecPath))
	if err != nil {
		t.Fatalf("LoadRegistry: %v", err)
	}
	apiHandler, err := apiv1.NewHandler(apiv1.Options{
		Registry:   registry,
		AuthTokens: []string{"test-token"},
		Handlers:   apiv1.OperatorEntityHandlers(apiv1.EntityHandlerOptions{Entities: sqliteStore}),
	})
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	server := httptest.NewServer(apiHandler)
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(ctx, t.TempDir(), []string{"entity", "list", "--run-id", runID, "--type", "vertical", "--limit", "10"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("entities list code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	for _, want := range []string{"ENTITY_ID", entityA, entityB, "vertical", "discovered", "pending"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("entities list stdout missing %q:\n%s", want, stdout.String())
		}
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("entities list stderr = %q, want empty", stderr.String())
	}

	stdout.Reset()
	stderr.Reset()
	code = executeRootCommandWithOptions(ctx, t.TempDir(), []string{"entity", "view", entityA, "--run-id", runID}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("entity view code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	for _, want := range []string{"Entity " + entityA, "vertical", "discovered", "vertical_name", "Healthcare"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("entity view stdout missing %q:\n%s", want, stdout.String())
		}
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("entity view stderr = %q, want empty", stderr.String())
	}

	stdout.Reset()
	stderr.Reset()
	code = executeRootCommandWithOptions(ctx, t.TempDir(), []string{"entity", "aggregate", "--run-id", runID, "--group-by", "entity_type"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("entity aggregate code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	for _, want := range []string{"GROUP", "COUNT", "vertical  2"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("entity aggregate stdout missing %q:\n%s", want, stdout.String())
		}
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("entity aggregate stderr = %q, want empty", stderr.String())
	}
}

func TestEntityViewUsesEntityGetAndRendersEntityNativeDetail(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	var captured jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer test-token" {
			t.Errorf("Authorization = %q, want bearer token", got)
		}
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Errorf("decode request: %v", err)
		}
		writeJSONRPCResult(t, w, captured.ID, validEntityFullResult("entity-1"))
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"entity", "view", "entity-1", "--run-id", "run-1"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if captured.JSONRPC != "2.0" || captured.Method != entityGetMethod {
		t.Fatalf("request jsonrpc/method = %s/%s, want 2.0/%s", captured.JSONRPC, captured.Method, entityGetMethod)
	}
	if !reflect.DeepEqual(captured.Params, map[string]any{"entity_id": "entity-1", "run_id": "run-1"}) {
		t.Fatalf("params = %#v, want entity_id/run_id", captured.Params)
	}
	for _, want := range []string{
		"Entity entity-1",
		"run-1",
		"review",
		"vertical",
		"collecting",
		"score",
		"ready",
	} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout.String())
		}
	}
	for _, absent := range []string{"accumulator", `"accumulated"`, `fields={"score":7}`} {
		if strings.Contains(stdout.String(), absent) {
			t.Fatalf("stdout should not contain %q:\n%s", absent, stdout.String())
		}
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestEntityViewPreservesExactIdentifiersAndSemanticKeys(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	runID := strings.Repeat("r", 256)
	rawFlow := "scope/" + strings.Repeat("f", 210) + "\nterminal\tsegment"
	wantFlow := "scope/" + strings.Repeat("f", 210) + " terminal segment"
	longKey := func(prefix, discriminator string) string {
		return strings.Repeat(prefix, 187) + discriminator + strings.Repeat("m", 20) + strings.Repeat("z", 12)
	}
	fieldA, fieldB := longKey("f", "a"), longKey("f", "b")
	gateA, gateB := longKey("g", "a"), longKey("g", "b")
	accumulatedA, accumulatedB := longKey("a", "a"), longKey("a", "b")
	loopA, loopB := longKey("l", "a"), longKey("l", "b")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request jsonRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode request: %v", err)
		}
		result := validEntityFullResult("entity-1")
		entity := result["entity"].(map[string]any)
		entity["run_id"] = runID
		entity["flow_instance"] = rawFlow
		result["fields"] = map[string]any{fieldA: "first", fieldB: "second"}
		result["gates"] = map[string]any{gateA: false, gateB: true}
		result["accumulated"] = map[string]any{accumulatedA: 1, accumulatedB: 2}
		result["loops"] = []map[string]any{
			{"id": loopA, "revision_id": "rev-a", "attempt": 1, "max_attempts": 2, "current_stage": "collecting", "status": "open"},
			{"id": loopB, "revision_id": "rev-b", "attempt": 2, "max_attempts": 2, "current_stage": "reviewing", "status": "closed"},
		}
		writeJSONRPCResult(t, w, request.ID, result)
	}))
	defer server.Close()

	render := func(args ...string) string {
		t.Helper()
		var stdout, stderr bytes.Buffer
		code := executeRootCommandWithOptions(context.Background(), t.TempDir(), args, &stdout, &stderr, testRootCommandOptions(server))
		if code != 0 {
			t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
		}
		if stderr.Len() != 0 {
			t.Fatalf("stderr = %q, want empty", stderr.String())
		}
		return stdout.String()
	}

	defaultOutput := render("entity", "view", "entity-1", "--run-id", "run-1")
	for _, want := range []string{runID, wantFlow, fieldA, fieldB, gateA, gateB} {
		if !strings.Contains(defaultOutput, want) {
			t.Fatalf("default entity view missing exact value %q:\n%s", want, defaultOutput)
		}
	}
	if strings.Contains(defaultOutput, "…") || strings.Contains(defaultOutput, "\t") || strings.Contains(defaultOutput, "\nterminal") {
		t.Fatalf("default entity view abbreviated an exact fact or leaked controls:\n%q", defaultOutput)
	}

	verboseOutput := render("entity", "view", "entity-1", "--run-id", "run-1", "--verbose")
	for _, want := range []string{runID, wantFlow, fieldA, fieldB, gateA, gateB, accumulatedA, accumulatedB, loopA, loopB} {
		if !strings.Contains(verboseOutput, want) {
			t.Fatalf("verbose entity view missing exact value %q:\n%s", want, verboseOutput)
		}
	}
	if strings.Contains(verboseOutput, "…") || strings.Contains(verboseOutput, "\t") || strings.Contains(verboseOutput, "\nterminal") {
		t.Fatalf("verbose entity view abbreviated an exact fact or leaked controls:\n%q", verboseOutput)
	}
}

func TestEntityViewJSONIsDataComplete(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	var captured jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Errorf("decode request: %v", err)
		}
		writeJSONRPCResult(t, w, captured.ID, validEntityFullResult("entity-1"))
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"entity", "view", "entity-1", "--run-id", "run-1", "--json"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	for _, want := range []string{
		`"entity_id":"entity-1"`,
		`"run_id":"run-1"`,
		`"slug":"vertical-1"`,
		`"name":"Vertical One"`,
		`"revision":3`,
		`"created_at":"2026-05-20T01:00:00Z"`,
		`"updated_at":"2026-05-20T01:05:00Z"`,
		`"fields":{"score":7}`,
		`"bookkeeping":{}`,
		`"gates":{"ready":true}`,
		`"accumulated":{"accumulator":{"count":2},"notes":["a",{"text":"probe"}],"score":3}`,
	} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("entity view --json missing %q:\n%s", want, stdout.String())
		}
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestEntityViewRendersAbsentSlugAndNameAsDash(t *testing.T) {
	var stdout bytes.Buffer
	writeEntityFullResult(&stdout, entityFull{
		Entity: entitySummary{
			EntityID:     "entity-1",
			RunID:        "run-1",
			FlowInstance: "review",
			EntityType:   "vertical",
			CurrentState: "collecting",
			Revision:     1,
			CreatedAt:    "2026-05-20T01:00:00Z",
			UpdatedAt:    "2026-05-20T01:05:00Z",
		},
		Fields:      map[string]any{},
		Gates:       map[string]bool{},
		Accumulated: map[string]any{},
	}, entityRenderOptions{})

	for _, label := range []string{"slug", "name"} {
		found := false
		for _, line := range strings.Split(stdout.String(), "\n") {
			if reflect.DeepEqual(strings.Fields(line), []string{label, "-"}) {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("absent %s must render as a stated dash:\n%s", label, stdout.String())
		}
	}
}

func TestEntityAggregateUsesEntityAggregateDefaultsAndFilters(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	var captured []jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req jsonRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode request: %v", err)
		}
		captured = append(captured, req)
		writeJSONRPCResult(t, w, req.ID, map[string]any{"counts": map[string]any{"collecting": 2, "reviewing": 1}})
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"entity", "aggregate"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("default code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if captured[0].Method != entityAggregateMethod {
		t.Fatalf("method = %q, want %s", captured[0].Method, entityAggregateMethod)
	}
	if len(captured[0].Params) != 0 {
		t.Fatalf("default params = %#v, want empty for server-owned default group", captured[0].Params)
	}
	if !strings.Contains(stdout.String(), "GROUP") || !strings.Contains(stdout.String(), "COUNT") || !strings.Contains(stdout.String(), "collecting  2") {
		t.Fatalf("stdout = %q, want aggregate table", stdout.String())
	}

	stdout.Reset()
	stderr.Reset()
	code = executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{
		"entity", "aggregate",
		"--run-id", "run-1",
		"--group-by", "fields.priority",
		"--type", "vertical",
	}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("filtered code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	wantParams := map[string]any{"run_id": "run-1", "group_by": "fields.priority", "type": "vertical"}
	if !reflect.DeepEqual(captured[1].Params, wantParams) {
		t.Fatalf("filtered params = %#v, want %#v", captured[1].Params, wantParams)
	}
}

func TestEntityCommandsRejectInvalidInputBeforeRequest(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		writeJSONRPCResult(t, w, "unexpected", map[string]any{"entities": []any{}})
	}))
	defer server.Close()

	for _, tc := range []struct {
		name       string
		args       []string
		wantStderr string
	}{
		{name: "list invalid limit low", args: []string{"entity", "list", "--limit", "0"}, wantStderr: "--limit must be between 1 and 500"},
		{name: "list invalid run id", args: []string{"entity", "list", "--run-id", "bad id!"}, wantStderr: "--run-id must match OpaqueId pattern"},
		{name: "list blank flow", args: []string{"entity", "list", "--flow", " "}, wantStderr: "--flow must not be empty"},
		{name: "view missing id", args: []string{"entity", "view"}, wantStderr: "requires <entity-id>"},
		{name: "view blank id", args: []string{"entity", "view", " "}, wantStderr: "entity id is required"},
		{name: "view invalid id", args: []string{"entity", "view", "bad id!"}, wantStderr: "entity id must match OpaqueId pattern"},
		{name: "view invalid run id", args: []string{"entity", "view", "entity-1", "--run-id", "bad id!"}, wantStderr: "--run-id must match OpaqueId pattern"},
		{name: "aggregate invalid group", args: []string{"entity", "aggregate", "--group-by", "bad field"}, wantStderr: "--group-by must be one of"},
		{name: "aggregate blank type", args: []string{"entity", "aggregate", "--type", " "}, wantStderr: "--type must not be empty"},
		{name: "aggregate extra arg", args: []string{"entity", "aggregate", "extra"}, wantStderr: "unknown command"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls.Store(0)
			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), tc.args, &stdout, &stderr, testRootCommandOptions(server))
			if code != 2 {
				t.Fatalf("code = %d, want 2 stdout=%s stderr=%s", code, stdout.String(), stderr.String())
			}
			if !strings.Contains(stderr.String(), tc.wantStderr) {
				t.Fatalf("stderr = %q, want substring %q", stderr.String(), tc.wantStderr)
			}
			if calls.Load() != 0 {
				t.Fatalf("RPC calls = %d, want 0", calls.Load())
			}
		})
	}
}

func TestEntityCommandsFailClosedWithoutTokenBeforeRequest(t *testing.T) {
	t.Setenv("SWARM_API_TOKEN", "")
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		writeJSONRPCResult(t, w, "unexpected", map[string]any{"entities": []any{}})
	}))
	defer server.Close()

	for _, args := range [][]string{
		{"entity", "list"},
		{"entity", "view", "entity-1"},
		{"entity", "aggregate"},
	} {
		var stdout, stderr bytes.Buffer
		code := executeRootCommandWithOptions(context.Background(), t.TempDir(), args, &stdout, &stderr, testRootCommandOptions(server))
		if code != 4 {
			t.Fatalf("%v code = %d, want 4 stdout=%s stderr=%s", args, code, stdout.String(), stderr.String())
		}
		if !strings.Contains(stderr.String(), "API token source is required") {
			t.Fatalf("%v stderr = %q, want token failure", args, stderr.String())
		}
	}
	if calls.Load() != 0 {
		t.Fatalf("RPC calls = %d, want 0", calls.Load())
	}
}

func TestEntityCommandsMapRuntimeFailuresAndMalformedResults(t *testing.T) {
	for _, tc := range []struct {
		name       string
		args       []string
		handler    http.HandlerFunc
		wantCode   int
		wantStderr string
	}{
		{
			name: "list http auth exits four",
			args: []string{"entity", "list"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
			},
			wantCode:   4,
			wantStderr: "rejected the request with status 401",
		},
		{
			name: "list missing entities exits three",
			args: []string{"entity", "list"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeJSONRPCResult(t, w, req.ID, map[string]any{})
			},
			wantCode:   3,
			wantStderr: "entities is required",
		},
		{
			name: "list malformed entity exits three",
			args: []string{"entity", "list"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				entity := validEntitySummary("entity-1")
				delete(entity, "current_state")
				writeJSONRPCResult(t, w, req.ID, map[string]any{"entities": []map[string]any{entity}})
			},
			wantCode:   3,
			wantStderr: "current_state is required",
		},
		{
			name: "view entity not found exits five",
			args: []string{"entity", "view", "missing"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeEntityJSONRPCError(t, w, req.ID, "ENTITY_NOT_FOUND")
			},
			wantCode:   5,
			wantStderr: "ENTITY_NOT_FOUND",
		},
		{
			name: "view missing fields exits three",
			args: []string{"entity", "view", "entity-1"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				result := validEntityFullResult("entity-1")
				delete(result, "fields")
				writeJSONRPCResult(t, w, req.ID, result)
			},
			wantCode:   3,
			wantStderr: "fields is required",
		},
		{
			name: "view blank entity type exits three",
			args: []string{"entity", "view", "entity-1"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				result := validEntityFullResult("entity-1")
				result["entity"].(map[string]any)["entity_type"] = " "
				writeJSONRPCResult(t, w, req.ID, result)
			},
			wantCode:   3,
			wantStderr: "entity_type is required",
		},
		{
			name: "aggregate unknown rpc exits three",
			args: []string{"entity", "aggregate"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeEntityJSONRPCError(t, w, req.ID, "METHOD_UNAVAILABLE")
			},
			wantCode:   3,
			wantStderr: "METHOD_UNAVAILABLE",
		},
		{
			name: "aggregate missing counts exits three",
			args: []string{"entity", "aggregate"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeJSONRPCResult(t, w, req.ID, map[string]any{})
			},
			wantCode:   3,
			wantStderr: "counts is required",
		},
		{
			name: "aggregate negative count exits three",
			args: []string{"entity", "aggregate"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				writeJSONRPCResult(t, w, req.ID, map[string]any{"counts": map[string]any{"collecting": -1}})
			},
			wantCode:   3,
			wantStderr: "counts[\"collecting\"] must be non-negative",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setCLIAPITestToken(t, "test-token")
			server := httptest.NewServer(tc.handler)
			defer server.Close()

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), tc.args, &stdout, &stderr, testRootCommandOptions(server))
			if code != tc.wantCode {
				t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, tc.wantCode, stdout.String(), stderr.String())
			}
			if !strings.Contains(stderr.String(), tc.wantStderr) {
				t.Fatalf("stderr = %q, want substring %q", stderr.String(), tc.wantStderr)
			}
		})
	}
}

func TestEntityViewGatesRenderFalseValuesInDefaultOutput(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	var captured jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Errorf("decode request: %v", err)
		}
		result := validEntityFullResult("entity-1")
		result["gates"] = map[string]any{"approved": false}
		writeJSONRPCResult(t, w, captured.ID, result)
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"entity", "view", "entity-1", "--run-id", "run-1"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if !strings.Contains(stdout.String(), "approved") || !strings.Contains(stdout.String(), "false") {
		t.Fatalf("Ruling A: a declared false gate must render its name and value in default output:\n%s", stdout.String())
	}
	if strings.Contains(stdout.String(), "Gates  none") {
		t.Fatalf("populated gate map must not render as none:\n%s", stdout.String())
	}
}

func TestEntityViewEmptyGatesRenderNone(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	var captured jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Errorf("decode request: %v", err)
		}
		result := validEntityFullResult("entity-1")
		result["gates"] = map[string]any{}
		writeJSONRPCResult(t, w, captured.ID, result)
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"entity", "view", "entity-1", "--run-id", "run-1"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if !strings.Contains(stdout.String(), "Gates  none") {
		t.Fatalf("empty gates map should render as stated none:\n%s", stdout.String())
	}
}

func TestEntityViewAccumulatedStatedPresenceAndVerboseExpansion(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	var captured jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Errorf("decode request: %v", err)
		}
		result := validEntityFullResult("entity-1")
		writeJSONRPCResult(t, w, captured.ID, result)
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"entity", "view", "entity-1", "--run-id", "run-1"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if !strings.Contains(stdout.String(), "Accumulated  3 keys (--verbose to expand)") {
		t.Fatalf("Ruling B: populated accumulated must be stated in default output:\n%s", stdout.String())
	}

	stdout.Reset()
	stderr.Reset()
	code = executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"entity", "view", "entity-1", "--run-id", "run-1", "--verbose"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("verbose code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	for _, want := range []string{"Accumulated", "accumulator", "notes", "score"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("verbose stdout missing %q:\n%s", want, stdout.String())
		}
	}
}

func TestEntityViewEmptyAccumulatedRendersNone(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	var captured jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Errorf("decode request: %v", err)
		}
		result := validEntityFullResult("entity-1")
		result["accumulated"] = map[string]any{}
		writeJSONRPCResult(t, w, captured.ID, result)
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"entity", "view", "entity-1", "--run-id", "run-1"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if !strings.Contains(stdout.String(), "Accumulated  none") {
		t.Fatalf("empty accumulated should render as stated none:\n%s", stdout.String())
	}
}

func TestEntityViewLoopsRenderAsDataUnderVerbose(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	var captured jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Errorf("decode request: %v", err)
		}
		result := validEntityFullResult("entity-1")
		result["loops"] = []map[string]any{
			{"id": "loop-a", "revision_id": "rev-9", "attempt": 1, "max_attempts": 3, "current_stage": "collecting", "status": "unusual"},
		}
		writeJSONRPCResult(t, w, captured.ID, result)
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"entity", "view", "entity-1", "--run-id", "run-1"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if strings.Contains(stdout.String(), "Loops") {
		t.Fatalf("default output should not render loops:\n%s", stdout.String())
	}

	stdout.Reset()
	stderr.Reset()
	code = executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"entity", "view", "entity-1", "--run-id", "run-1", "--verbose"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("verbose code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	for _, want := range []string{"Loops", "loop-a", "unusual", "attempt 1/3", "rev-9", "collecting"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("verbose stdout missing %q:\n%s", want, stdout.String())
		}
	}
}

func TestEntityViewVerboseShowsBookkeepingAndAbsoluteTimestamps(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	var captured jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Errorf("decode request: %v", err)
		}
		result := validEntityFullResult("entity-1")
		bookkeeping := result["bookkeeping"].(map[string]any)
		bookkeeping["bundle_hash"] = "bundle-v2:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		bookkeeping["activation"] = "standing"
		writeJSONRPCResult(t, w, captured.ID, result)
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"entity", "view", "entity-1", "--run-id", "run-1", "--verbose"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	for _, want := range []string{"Bookkeeping", "bundle_hash", "activation", "2026-05-20T01:05:00Z"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("verbose stdout missing %q:\n%s", want, stdout.String())
		}
	}

	stdout.Reset()
	stderr.Reset()
	code = executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"entity", "view", "entity-1", "--run-id", "run-1"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("default code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if !strings.Contains(stdout.String(), "Bookkeeping  2 facts (--verbose to expand)") {
		t.Fatalf("default stdout missing bookkeeping presence summary:\n%s", stdout.String())
	}
	for _, absent := range []string{"bundle_hash", "activation", "2026-05-20T01:05:00Z"} {
		if strings.Contains(stdout.String(), absent) {
			t.Fatalf("default stdout should not contain %q:\n%s", absent, stdout.String())
		}
	}
}

func TestEntityViewStatedEmptyStatesAndTruncationAndControlChars(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	var captured jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Errorf("decode request: %v", err)
		}
		result := validEntityFullResult("entity-1")
		entity := result["entity"].(map[string]any)
		entity["name"] = "Vertical\nOne"
		result["fields"] = map[string]any{
			"chats":               map[string]any{},
			"empty":               "   ",
			"long":                strings.Repeat("x", 300),
			"broken":              "line one\nline two\tand a control \x01 byte",
			"unicode_separators":  "a\u2028b\u2029c\u0085d",
			"label\nwith_newline": "value",
		}
		result["bookkeeping"] = map[string]any{"fan_out_count": 3}
		result["gates"] = map[string]any{}
		result["accumulated"] = map[string]any{}
		writeJSONRPCResult(t, w, captured.ID, result)
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"entity", "view", "entity-1", "--run-id", "run-1"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	chatsLine, emptyLine := "", ""
	for _, line := range strings.Split(stdout.String(), "\n") {
		if strings.Contains(line, "chats") {
			chatsLine = line
		}
		if strings.Contains(line, "empty") {
			emptyLine = line
		}
	}
	if !strings.Contains(chatsLine, "none") {
		t.Fatalf("empty collection value should render as none:\n%s", stdout.String())
	}
	if !strings.Contains(emptyLine, "none") {
		t.Fatalf("empty string field should render as none:\n%s", stdout.String())
	}
	if strings.Contains(stdout.String(), "fan_out_count") {
		t.Fatalf("platform bookkeeping key leaked into default Fields:\n%s", stdout.String())
	}
	if !strings.Contains(stdout.String(), "…") {
		t.Fatalf("long value missing truncation marker:\n%s", stdout.String())
	}
	if strings.Contains(stdout.String(), "\nline two") || strings.Contains(stdout.String(), "\x01") {
		t.Fatalf("control characters leaked into output line discipline:\n%q", stdout.String())
	}
	if strings.Contains(stdout.String(), "\nOne") {
		t.Fatalf("embedded newline in entity name broke the header line discipline:\n%q", stdout.String())
	}
	if strings.Contains(stdout.String(), "\nwith_newline") {
		t.Fatalf("embedded newline in field label broke the detail line discipline:\n%q", stdout.String())
	}
	unicodeLine := ""
	for _, line := range strings.Split(stdout.String(), "\n") {
		if strings.Contains(line, "unicode_separators") {
			unicodeLine = line
			break
		}
	}
	if !strings.Contains(unicodeLine, "a b c d") {
		t.Fatalf("unicode line/paragraph separators not collapsed to spaces:\n%q", stdout.String())
	}
}

func TestEntityViewQuietRendersEntityID(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	var captured jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Errorf("decode request: %v", err)
		}
		writeJSONRPCResult(t, w, captured.ID, validEntityFullResult("entity-1"))
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"entity", "view", "entity-1", "--run-id", "run-1", "--quiet"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if strings.TrimSpace(stdout.String()) != "entity-1" {
		t.Fatalf("quiet stdout = %q, want entity-1", stdout.String())
	}
}

func TestEntityListQuietRendersEntityIDs(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	var captured jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Errorf("decode request: %v", err)
		}
		writeJSONRPCResult(t, w, captured.ID, map[string]any{
			"entities": []map[string]any{validEntitySummary("entity-a"), validEntitySummary("entity-b")},
		})
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"entity", "list", "--quiet"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	if len(lines) != 2 || lines[0] != "entity-a" || lines[1] != "entity-b" {
		t.Fatalf("quiet stdout = %q, want entity-a/entity-b lines", stdout.String())
	}
}

func TestEntityListUnscopedRetainsRunColumn(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	var captured jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Errorf("decode request: %v", err)
		}
		writeJSONRPCResult(t, w, captured.ID, map[string]any{
			"entities": []map[string]any{validEntitySummary("entity-1")},
		})
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"entity", "list"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	for _, want := range []string{"ENTITY_ID", "RUN", "entity-1", "run-1"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("unscoped list missing %q:\n%s", want, stdout.String())
		}
	}
	if got, want := strings.Fields(strings.Split(stdout.String(), "\n")[0]), []string{"ENTITY_ID", "RUN", "TYPE", "STATE", "FLOW", "UPDATED"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("unscoped columns = %v, want %v:\n%s", got, want, stdout.String())
	}

	stdout.Reset()
	stderr.Reset()
	code = executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"entity", "list", "--run-id", "run-1"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("scoped code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if got, want := strings.Fields(strings.Split(stdout.String(), "\n")[0]), []string{"ENTITY_ID", "TYPE", "STATE", "FLOW", "UPDATED"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("run-scoped columns = %v, want %v:\n%s", got, want, stdout.String())
	}
}

func TestEntityListFlowIdentifiersRemainExactAndDistinct(t *testing.T) {
	sharedPrefix := strings.Repeat("a", 63)
	flowA := "scope/" + sharedPrefix + "1"
	flowB := "scope/" + sharedPrefix + "2"
	var stdout bytes.Buffer
	writeEntityListResult(&stdout, entityListResult{Entities: []entitySummary{
		{EntityID: "entity-a", RunID: "run-1", FlowInstance: flowA, EntityType: "vertical", CurrentState: "collecting", UpdatedAt: "2026-05-20T01:05:00Z"},
		{EntityID: "entity-b", RunID: "run-1", FlowInstance: flowB, EntityType: "vertical", CurrentState: "collecting", UpdatedAt: "2026-05-20T01:05:00Z"},
	}}, entityListRenderOptions{runScoped: true})

	rendered := stdout.String()
	for _, want := range []string{flowA, flowB} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("rendered flow cells must preserve the exact identifier %q:\n%s", want, rendered)
		}
	}
	if strings.Contains(rendered, "…") {
		t.Fatalf("flow identifiers must not be abbreviated:\n%s", rendered)
	}
	lines := strings.Split(strings.TrimSpace(rendered), "\n")
	if len(lines) != 3 || lines[1] == lines[2] {
		t.Fatalf("flow identifiers sharing a long prefix must render as distinct rows:\n%s", rendered)
	}
}

func TestEntityListFlowIdentifierSanitizesControlsWithoutTruncation(t *testing.T) {
	flow := "scope/" + strings.Repeat("b", 64) + "\nterminal\tsegment"
	want := "scope/" + strings.Repeat("b", 64) + " terminal segment"
	var stdout bytes.Buffer
	writeEntityListResult(&stdout, entityListResult{Entities: []entitySummary{
		{EntityID: "entity-a", RunID: "run-1", FlowInstance: flow, EntityType: "vertical", CurrentState: "collecting", UpdatedAt: "2026-05-20T01:05:00Z"},
	}}, entityListRenderOptions{runScoped: true})

	rendered := stdout.String()
	if !strings.Contains(rendered, want) {
		t.Fatalf("rendered flow must preserve the complete identifier while sanitizing controls; want %q:\n%s", want, rendered)
	}
	if strings.Contains(rendered, "\nterminal") || strings.Contains(rendered, "\t") || strings.Contains(rendered, "…") {
		t.Fatalf("flow identifier leaked controls or was abbreviated:\n%q", rendered)
	}
}

func TestEntityListVerboseTimestampsAreRelativePlusAbsolute(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	var captured jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Errorf("decode request: %v", err)
		}
		writeJSONRPCResult(t, w, captured.ID, map[string]any{
			"entities": []map[string]any{validEntitySummary("entity-1")},
		})
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"entity", "list", "--run-id", "run-1", "--verbose"}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if !strings.Contains(stdout.String(), "2026-05-20T01:05:00Z") {
		t.Fatalf("verbose list missing absolute timestamp:\n%s", stdout.String())
	}
}

func TestEntityOutputModesRejectCollisionBeforeRequest(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		writeJSONRPCResult(t, w, "unexpected", map[string]any{"entities": []any{}})
	}))
	defer server.Close()

	for _, args := range [][]string{
		{"entity", "list", "--json", "--quiet"},
		{"entity", "view", "entity-1", "--json", "--quiet"},
	} {
		calls.Store(0)
		var stdout, stderr bytes.Buffer
		code := executeRootCommandWithOptions(context.Background(), t.TempDir(), args, &stdout, &stderr, testRootCommandOptions(server))
		if code != 2 {
			t.Fatalf("%v code = %d, want 2 stdout=%s stderr=%s", args, code, stdout.String(), stderr.String())
		}
		if !strings.Contains(stderr.String(), "mutually exclusive") {
			t.Fatalf("%v stderr = %q, want collision error", args, stderr.String())
		}
		if calls.Load() != 0 {
			t.Fatalf("%v RPC calls = %d, want 0 (collision must fail before any API call)", args, calls.Load())
		}
	}
}

func TestFormatCLIRelativeTime(t *testing.T) {
	now := time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		raw  string
		want string
	}{
		{raw: "2026-08-08T11:59:50Z", want: "just now"},
		{raw: "2026-08-08T11:57:00Z", want: "3m ago"},
		{raw: "2026-08-08T09:00:00Z", want: "3h ago"},
		{raw: "2026-08-05T12:00:00Z", want: "3d ago"},
		{raw: "2026-05-20T01:05:00Z", want: "2mo ago"},
		{raw: "2020-01-01T00:00:00Z", want: "6y ago"},
		{raw: "not-a-timestamp", want: "not-a-timestamp"},
		{raw: "2026-08-08T12:00:30Z", want: "just now"},
		{raw: "2026-08-09T12:00:00Z", want: "2026-08-09T12:00:00Z"},
	} {
		if got := formatCLIRelativeTime(now, tc.raw); got != tc.want {
			t.Errorf("formatCLIRelativeTime(%q) = %q, want %q", tc.raw, got, tc.want)
		}
	}
}

func validEntitySummary(entityID string) map[string]any {
	return map[string]any{
		"entity_id":     entityID,
		"run_id":        "run-1",
		"flow_instance": "review",
		"entity_type":   "vertical",
		"current_state": "collecting",
		"slug":          "vertical-1",
		"name":          "Vertical One",
		"revision":      3,
		"created_at":    "2026-05-20T01:00:00Z",
		"updated_at":    "2026-05-20T01:05:00Z",
	}
}

func validEntityFullResult(entityID string) map[string]any {
	return map[string]any{
		"entity":      validEntitySummary(entityID),
		"fields":      map[string]any{"score": 7},
		"bookkeeping": map[string]any{},
		"gates":       map[string]any{"ready": true},
		"accumulated": map[string]any{
			"score":       3,
			"accumulator": map[string]any{"count": 2},
			"notes":       []any{"a", map[string]any{"text": "probe"}},
		},
	}
}

func writeEntityJSONRPCError(t *testing.T, w http.ResponseWriter, id string, code string) {
	t.Helper()
	w.Header().Set("content-type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"error": map[string]any{
			"code":    -32000,
			"message": code,
			"data": map[string]any{
				"code": code,
			},
		},
	}); err != nil {
		t.Fatalf("encode error response: %v", err)
	}
}
