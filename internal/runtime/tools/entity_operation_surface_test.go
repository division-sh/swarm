package tools_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/mcp"
	"github.com/division-sh/swarm/internal/runtime/mutationlog"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/division-sh/swarm/internal/store/storetest"
)

type operationSurfaceReader struct {
	runtimetools.EntityPersistence
	row map[string]any
}

func (s *operationSurfaceReader) LoadEntityState(_ context.Context, identity runtimetools.EntityIdentity) (map[string]any, bool, error) {
	return s.row, identity.RunID == entityToolTestRunID && identity.EntityID == s.row["entity_id"], nil
}

// This is a tool-boundary fixture, not proof of durable pipeline CAS. Its
// deliberately stale reader would expose accidental whole-field lowering.
type operationSurfaceWriter struct {
	mu       sync.Mutex
	contract entityruntime.Contract
	source   semanticview.Source
	fields   map[string]any
	requests []pipeline.EntityFieldMutation
	revision int
	fault    error
	reject   bool
}

func (w *operationSurfaceWriter) ApplyEntityFieldMutation(_ context.Context, request pipeline.EntityFieldMutation) (pipeline.EntityFieldMutationResult, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.requests = append(w.requests, request)
	if w.reject {
		return pipeline.EntityFieldMutationResult{}, w.fault
	}
	fields, err := entityruntime.ApplyMutations(w.contract, w.fields, []entityruntime.Mutation{request.Mutation})
	if err != nil {
		return pipeline.EntityFieldMutationResult{}, failures.WrapDetail("invalid_tool_input", "operation-test-writer", "apply", nil, err)
	}
	w.fields = fields
	w.revision++
	return pipeline.EntityFieldMutationResult{Revision: w.revision, Acknowledged: true}, w.fault
}

func newOperationSurfaceFixture(t *testing.T) (context.Context, models.AgentConfig, *runtimetools.Executor, *operationSurfaceReader, *operationSurfaceWriter) {
	t.Helper()
	actor := models.AgentConfig{ExecutionMode: "live", ID: "writer", Role: "operator", FlowID: "review"}
	bundle := loadWave1EntityToolMultiFlowBundle(t, map[string]entityToolFlowFixture{
		"review": {
			SchemaYAML:   "name: review\nstages:\n  queued: {initial: true}\n  marginal_review: {}\n  closed: {terminal: true}\n",
			EntitiesYAML: "case:\n  status: text\n  items: list<text>\n  labels: map[text]text\n  protected: text\n",
			AgentsYAML:   "writer:\n  role: operator\n  intent: {inline: Write declared fields.}\n  entity_writes:\n    case:\n      save: [status, items, labels]\n",
		},
	})
	source := semanticview.Wrap(bundle)
	declaration, ok := semanticview.ResolveAgentDeclaration(source, actor)
	if !ok {
		t.Fatal("fixture actor declaration missing")
	}
	plan, err := semanticview.ScopedAgentNamePlan(source, declaration)
	if err != nil {
		t.Fatal(err)
	}
	name, err := plan.Materialize()
	if err != nil {
		t.Fatal(err)
	}
	instance, err := flowidentity.StandingForGeneration(source, actor.FlowID, entityToolTestRunID)
	if err != nil {
		t.Fatal(err)
	}
	route, err := instance.Route().AgentIdentityRoute()
	if err != nil {
		t.Fatal(err)
	}
	actor.Identity, err = agentidentity.New(entityToolTestRunID, name, route)
	if err != nil {
		t.Fatal(err)
	}
	actor.ID, actor.EntityID, actor.FlowPath = name.AgentID, instance.EntityID, instance.InstancePath
	contract, ok := entityruntime.ResolveForActor(source, actor)
	if !ok {
		t.Fatal("fixture actor contract missing")
	}
	fields, err := entityruntime.NormalizeState(contract, map[string]any{
		"status": "before", "items": []any{"a", "b"}, "labels": map[string]any{"old": "kept"},
	})
	if err != nil {
		t.Fatal(err)
	}
	reader := &operationSurfaceReader{row: map[string]any{
		"entity_id": instance.EntityID, "flow_instance": instance.InstancePath, "entity_type": "case",
		"current_state": "marginal_review", "fields": fields,
	}}
	fresh, err := entityruntime.NormalizeState(contract, fields)
	if err != nil {
		t.Fatal(err)
	}
	writer := &operationSurfaceWriter{contract: contract, source: source, fields: fresh, revision: 1}
	exec := runtimetools.NewExecutorWithOptions(nil, runtimetools.ExecutorOptions{EntityStore: reader, EntityWriter: writer, WorkflowSource: source})
	inbound := eventtest.RunCreatingRootIngress("current-operation", events.EventType("review.started"), "", "", nil, 0, entityToolTestRunID, "",
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, instance.EntityID), instance.InstancePath), time.Time{})
	ctx := runtimebus.WithInboundEvent(runtimetools.WithActor(correlation.WithRunID(unmanagedToolTestContext(), entityToolTestRunID), actor), inbound)
	return ctx, actor, exec, reader, writer
}

func TestEntityOperationSurfaceDistinctIndicesAppendAndMapKeys(t *testing.T) {
	ctx, actor, exec, _, writer := newOperationSurfaceFixture(t)
	inputs := []struct {
		tool  string
		input map[string]any
	}{
		{"save_case_items", map[string]any{"op": "update", "index": 0, "value": "index-0"}},
		{"save_case_items", map[string]any{"op": "update", "index": 1, "value": "index-1"}},
		{"save_case_items", map[string]any{"op": "append", "value": "equal"}},
		{"save_case_items", map[string]any{"op": "append", "value": "equal"}},
		{"save_case_labels", map[string]any{"op": "set", "key": "first", "value": "one"}},
		{"save_case_labels", map[string]any{"op": "set", "key": "second", "value": "two"}},
	}
	start := make(chan struct{})
	results := make(chan error, len(inputs))
	for _, input := range inputs {
		go func(tool string, payload map[string]any) {
			<-start
			_, err := exec.Execute(ctx, tool, payload)
			results <- err
		}(input.tool, input.input)
	}
	close(start)
	for range inputs {
		select {
		case err := <-results:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("bounded operation calls did not finish")
		}
	}
	if !reflect.DeepEqual(writer.fields["items"], []any{"index-0", "index-1", "equal", "equal"}) ||
		!reflect.DeepEqual(writer.fields["labels"], map[string]any{"old": "kept", "first": "one", "second": "two"}) {
		t.Fatalf("element operations lost a sibling or duplicate: %#v", writer.fields)
	}
	if len(writer.requests) != len(inputs) || writer.revision != 1+len(inputs) {
		t.Fatalf("requests=%d revision=%d", len(writer.requests), writer.revision)
	}
	for _, request := range writer.requests {
		if request.RunID != entityToolTestRunID || request.EntityID != actor.EntityID || request.Owner.Route.InstancePath != actor.FlowPath || request.Owner.Route.ScopeKey != actor.Identity.Route.ScopeKey ||
			request.Owner.Route.InstanceID != actor.Identity.Route.InstanceID || request.Owner.RunID != request.RunID || request.FlowID != "review" ||
			request.Writer != (mutationlog.Writer{Type: "agent", ID: actor.ID, HandlerStep: "save_entity_field"}) || request.Source == nil {
			t.Fatalf("request lost captured route/source/agent attribution: %#v", request)
		}
		if _, frozen := request.Mutation.Value.([]any); frozen {
			t.Fatalf("element operation became a stale whole list: %#v", request.Mutation)
		}
	}
}

func TestEntityOperationSurfaceSameSlotOrderingAndWholeSetControls(t *testing.T) {
	ctx, _, exec, _, writer := newOperationSurfaceFixture(t)
	for _, input := range []struct {
		tool  string
		input map[string]any
	}{
		{"save_case_items", map[string]any{"op": "update", "index": 1, "value": "first"}},
		{"save_case_items", map[string]any{"op": "update", "index": 1, "value": "last"}},
		{"save_case_labels", map[string]any{"op": "set", "key": "same", "value": "first"}},
		{"save_case_labels", map[string]any{"op": "set", "key": "same", "value": "last"}},
	} {
		if _, err := exec.Execute(ctx, input.tool, input.input); err != nil {
			t.Fatal(err)
		}
	}
	if !reflect.DeepEqual(writer.fields["items"], []any{"a", "last"}) || writer.fields["labels"].(map[string]any)["same"] != "last" || len(writer.requests) != 4 {
		t.Fatalf("same-slot ordering = %#v, requests=%d", writer.fields, len(writer.requests))
	}
	for _, op := range []string{"", "set"} {
		for _, tc := range []struct {
			field string
			value any
		}{
			{"status", "replaced"}, {"items", []any{"whole"}}, {"labels", map[string]any{"whole": "only"}},
		} {
			input := map[string]any{"value": tc.value}
			if op != "" {
				input["op"] = op
			}
			if _, err := exec.Execute(ctx, "save_case_"+tc.field, input); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(writer.fields[tc.field], tc.value) {
				t.Fatalf("whole set implicitly merged %s: %#v", tc.field, writer.fields)
			}
		}
	}
}

func TestEntityOperationSurfaceFreshSlotAndAtomicInvalidIndex(t *testing.T) {
	ctx, _, exec, _, writer := newOperationSurfaceFixture(t)
	writer.fields["items"] = []any{"replacement-0", "replacement-1", "replacement-2"}
	if _, err := exec.Execute(ctx, "save_case_items", map[string]any{"op": "update", "index": 1, "value": "updated"}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(writer.fields["items"], []any{"replacement-0", "updated", "replacement-2"}) {
		t.Fatalf("index did not refer to fresh slot: %#v", writer.fields)
	}
	before, revision := len(writer.requests), writer.revision
	for _, index := range []any{-1, 0.5, "0", nil, true} {
		if out, err := exec.Execute(ctx, "save_case_items", map[string]any{"op": "update", "index": index, "value": "bad"}); err == nil || out != nil {
			t.Fatalf("invalid optimistic index %#v accepted: %#v, %v", index, out, err)
		}
	}
	if out, err := exec.Execute(ctx, "save_case_items", map[string]any{"op": "update", "index": 2, "value": "fresh-slot"}); err != nil || out == nil {
		t.Fatalf("fresh valid position was refused by stale read: %#v, %v", out, err)
	}
	if len(writer.requests) != before+1 || writer.revision != revision+1 {
		t.Fatal("valid fresh index was incorrectly refused based on stale reader")
	}
	revision = writer.revision
	writer.fields["items"] = []any{}
	out, err := exec.Execute(ctx, "save_case_items", map[string]any{"op": "update", "index": 1, "value": "bad"})
	if err == nil || out != nil || writer.revision != revision || len(writer.fields["items"].([]any)) != 0 {
		t.Fatalf("index removed in fresh state did not reject atomically: %#v, %v", out, err)
	}
	requireToolFailure(t, err, failures.ClassSchemaInvalid, "invalid_tool_input")
}

func TestEntityOperationSurfaceFreshIndexAdmissionOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			actor := models.AgentConfig{ExecutionMode: "live", ID: "writer", Role: "writer"}
			bundle := loadWave1EntityToolBundle(t, actor, "review", "case", "", "case:\n  items: list<text>\n")
			var selected runtimetools.EntityPersistence
			if backend == "sqlite" {
				selected = storetest.StartSQLiteRuntimeStore(t)
			} else {
				pg, _ := storetest.StartPostgresRuntimeStoreWithReopen(t)
				selected = pg
			}
			ctx := seedEntityToolSourceRun(t, selected, bundle)
			fixture := ctx.Value(entityToolImportFixtureKey{}).(entityToolImportFixture)
			actor = entityToolFixtureActor(t, fixture.source, actor, entityToolTestRunID, "review", "review/inst-1")
			entityID := seedImportedEntityForToolTest(t, ctx, map[string]any{"flow_instance": "review/inst-1", "fields": map[string]any{"items": []any{"fresh-0", "fresh-1"}}})
			inbound := eventtest.RunCreatingRootIngress("fresh-index", events.EventType("review.started"), "", "", nil, 0, entityToolTestRunID, "",
				events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), "review/inst-1"), time.Time{})
			ctx = runtimetools.WithActor(runtimebus.WithInboundEvent(ctx, inbound), actor)
			stale := &operationSurfaceReader{row: map[string]any{"entity_id": entityID, "flow_instance": "review/inst-1", "entity_type": "case", "current_state": "queued", "fields": map[string]any{"items": []any{}}}}
			exec := runtimetools.NewExecutorWithOptions(nil, runtimetools.ExecutorOptions{EntityStore: stale, EntityWriter: fixture.pipeline, WorkflowSource: fixture.source})
			if out, err := exec.Execute(ctx, "save_case_items", map[string]any{"op": "update", "index": 1, "value": "updated"}); err != nil || out == nil {
				t.Fatalf("stale reader refused fresh valid slot: %#v, %v", out, err)
			}
			identity := runtimetools.EntityIdentity{RunID: entityToolTestRunID, EntityID: entityID}
			before, found, err := selected.LoadEntityState(ctx, identity)
			if err != nil || !found || !reflect.DeepEqual(before["fields"].(map[string]any)["items"], []any{"fresh-0", "updated"}) {
				t.Fatalf("fresh index effect: %#v found=%v err=%v", before, found, err)
			}
			stale.row["fields"] = map[string]any{"items": []any{"stale-0", "stale-1", "stale-2"}}
			out, err := exec.Execute(ctx, "save_case_items", map[string]any{"op": "update", "index": 2, "value": "invalid"})
			if out != nil {
				t.Fatalf("fresh invalid slot returned acknowledgment: %#v", out)
			}
			requireToolFailure(t, err, failures.ClassSchemaInvalid, "invalid_tool_input")
			after, found, err := selected.LoadEntityState(ctx, identity)
			if err != nil || !found || !reflect.DeepEqual(before, after) {
				t.Fatalf("fresh index refusal changed fields/revision: %#v err=%v", after, err)
			}
			count := 0
			for _, mutation := range storetest.ObserveEntityMutationHistory(t, ctx, selected, entityToolTestRunID) {
				if mutation.EntityID == entityID && mutation.Path == "items" && mutation.WriterType == "agent" {
					count++
				}
			}
			if count != 1 {
				t.Fatalf("fresh index mutation evidence count=%d, want 1", count)
			}
		})
	}
}

func TestEntityOperationSurfaceWriterDependencyAndPostCommitResponse(t *testing.T) {
	ctx, _, exec, reader, writer := newOperationSurfaceFixture(t)
	withoutWriter := runtimetools.NewExecutorWithOptions(nil, runtimetools.ExecutorOptions{EntityStore: reader, WorkflowSource: writer.source})
	if out, err := withoutWriter.Execute(ctx, "save_case_status", map[string]any{"value": "bad"}); out != nil || err == nil || !strings.Contains(err.Error(), "dependency_unavailable") {
		t.Fatalf("missing canonical writer = %#v, %v", out, err)
	}
	writer.fault = errors.New("cleanup password=private-token")
	out, err := exec.Execute(ctx, "save_case_items", map[string]any{"op": "append", "value": "equal"})
	if err != nil {
		t.Fatal(err)
	}
	response := out.(map[string]any)
	if response["write_committed"] != true || response["retry_write"] != false || response["status"] != "committed_with_post_commit_error" || response["revision"] != writer.revision || len(writer.requests) != 1 {
		t.Fatalf("post-commit response = %#v, calls=%d", response, len(writer.requests))
	}
	encoded, err := json.Marshal(response)
	if err != nil || strings.Contains(string(encoded), "private-token") {
		t.Fatalf("post-commit response leaked internal cause: %s, %v", encoded, err)
	}
	writer.reject = true
	if out, err := exec.Execute(ctx, "save_case_items", map[string]any{"op": "append", "value": "uncommitted"}); out != nil || !errors.Is(err, writer.fault) {
		t.Fatalf("unacknowledged response = %#v, %v", out, err)
	}
	if !reflect.DeepEqual(writer.fields["items"], []any{"a", "b", "equal"}) {
		t.Fatalf("append repeated after acknowledgment or persisted on refusal: %#v", writer.fields)
	}
}

func TestEntityOperationSurfaceActorRouteAndWriteOwnership(t *testing.T) {
	ctx, actor, exec, reader, writer := newOperationSurfaceFixture(t)
	for _, tc := range []struct {
		name  string
		alter func(*models.AgentConfig)
	}{
		{"unknown declaration", func(a *models.AgentConfig) { a.ID, a.Identity.Name.AgentID = "unknown", "unknown" }},
		{"wrong declaration owner", func(a *models.AgentConfig) { a.Identity.Name.Owner = "unknown-owner" }},
		{"missing concrete identity", func(a *models.AgentConfig) { a.Identity = agentidentity.Identity{} }},
		{"foreign run", func(a *models.AgentConfig) { a.Identity.RunID = "22222222-2222-2222-2222-222222222222" }},
		{"foreign actor entity", func(a *models.AgentConfig) { a.EntityID = "22222222-2222-2222-2222-222222222222" }},
		{"foreign route", func(a *models.AgentConfig) { a.FlowPath += "/foreign"; a.Identity.Route.InstancePath = a.FlowPath }},
		{"foreign flow", func(a *models.AgentConfig) { a.FlowID = "." }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := actor
			tc.alter(&changed)
			if out, err := exec.Execute(runtimetools.WithActor(ctx, changed), "save_case_status", map[string]any{"value": "forbidden"}); err == nil || out != nil {
				t.Fatalf("wrong actor authority accepted: %#v, %v", out, err)
			}
		})
	}
	if out, err := exec.Execute(ctx, "save_case_protected", map[string]any{"value": "forbidden"}); err == nil || out != nil {
		t.Fatalf("undeclared write field accepted: %#v, %v", out, err)
	}
	reader.row["entity_id"] = "22222222-2222-2222-2222-222222222222"
	if out, err := exec.Execute(ctx, "save_case_status", map[string]any{"value": "forbidden"}); err == nil || out != nil {
		t.Fatalf("wrong stored entity accepted: %#v, %v", out, err)
	}
	if len(writer.requests) != 0 {
		t.Fatalf("unauthorized writes reached canonical writer: %#v", writer.requests)
	}
}

func TestEntityOperationSurfaceSchemaAndMCPParity(t *testing.T) {
	ctx, actor, exec, _, writer := newOperationSurfaceFixture(t)
	_, defs, release, err := exec.AcquireToolDefinitionsForActorInContext(ctx, actor)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	plain := roleScopedToolDefinitionMap(defs)
	for _, tc := range []struct {
		tool  string
		input map[string]any
		valid bool
	}{
		{"save_case_items", map[string]any{"value": []any{"whole"}}, true},
		{"save_case_items", map[string]any{"op": "set", "value": []any{"whole"}}, true},
		{"save_case_items", map[string]any{"op": "append", "value": "element"}, true},
		{"save_case_items", map[string]any{"op": "update", "index": 0, "value": "element"}, true},
		{"save_case_labels", map[string]any{"op": "set", "key": "entry", "value": "element"}, true},
		{"save_case_labels", map[string]any{"value": map[string]any{"entry": "whole"}}, true},
		{"save_case_items", map[string]any{"op": "append", "value": []any{"wrong"}}, false},
		{"save_case_items", map[string]any{"op": "update", "value": "missing-index"}, false},
		{"save_case_items", map[string]any{"op": "update", "index": "0", "value": "wrong-index"}, false},
		{"save_case_items", map[string]any{"op": "update", "index": -1, "value": "wrong-index"}, false},
		{"save_case_items", map[string]any{"op": "update", "index": 0.5, "value": "wrong-index"}, false},
		{"save_case_items", map[string]any{"op": "append", "key": "id", "value": "wrong-selector"}, false},
		{"save_case_items", map[string]any{"op": "set", "key": "id", "value": "keyed-list"}, false},
		{"save_case_items", map[string]any{"op": "clear", "value": []any{}}, false},
		{"save_case_items", map[string]any{"op": "append", "index": 0, "value": "wrong-selector"}, false},
		{"save_case_labels", map[string]any{"op": "set", "key": "entry", "value": map[string]any{"whole": "wrong"}}, false},
		{"save_case_labels", map[string]any{"op": "set", "key": "", "value": "empty-key"}, false},
		{"save_case_labels", map[string]any{"op": "set", "key": "entry", "index": 0, "value": "wrong-selector"}, false},
		{"save_case_labels", map[string]any{"op": "merge", "key": "entry", "value": "wrong-op"}, false},
		{"save_case_status", map[string]any{"op": "append", "value": "wrong-op"}, false},
		{"save_case_status", map[string]any{"op": "set", "value": "valid", "extra": nil}, false},
	} {
		schema, ok := plain[tc.tool].Schema.(map[string]any)
		if !ok {
			t.Fatalf("missing generated schema for %s", tc.tool)
		}
		if err := runtimetools.ValidatePayloadAgainstSchema(schema, tc.input); (err == nil) != tc.valid {
			t.Fatalf("schema %s payload %#v: valid=%v, err=%v", tc.tool, tc.input, tc.valid, err)
		}
		if !tc.valid {
			if out, err := exec.Execute(ctx, tc.tool, tc.input); err == nil || out != nil {
				t.Fatalf("runtime disagreed with schema for %#v: %#v, %v", tc.input, out, err)
			}
		}
	}
	if errs := runtimetools.ValidateGeneratedToolSchemaClosureForSource(writer.source); len(errs) != 0 {
		t.Fatalf("generated schema closure: %v", errs)
	}
	inbound, _ := runtimebus.InboundEventFromContext(ctx)
	allowed := map[string]struct{}{}
	for name := range plain {
		allowed[name] = struct{}{}
	}
	turn := mcp.TurnContext{RunID: entityToolTestRunID, Actor: actor, Inbound: inbound, HasInbound: true, ForkSandboxAllowed: allowed}
	gateway := mcp.NewGateway(exec, "operation-token", mcp.GatewayHooks{
		WithActor: runtimetools.WithActor, WithInboundEvent: runtimebus.WithInboundEvent,
		ResolveTurnContext: func(string) (mcp.TurnContext, bool) { return turn, true },
	})
	req := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":"schemas","method":"tools/list"}`)).WithContext(ctx)
	req.Header.Set("Authorization", "Bearer operation-token")
	req.Header.Set("X-Swarm-Context-Token", "operation-context")
	response := httptest.NewRecorder()
	gateway.Handler().ServeHTTP(response, req)
	var rpc struct {
		Result struct {
			Tools []struct {
				Name        string         `json:"name"`
				InputSchema map[string]any `json:"inputSchema"`
			} `json:"tools"`
		} `json:"result"`
		Error any `json:"error"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &rpc); err != nil || rpc.Error != nil {
		t.Fatalf("MCP tools/list: %s, %v", response.Body.String(), err)
	}
	matched := 0
	for _, tool := range rpc.Result.Tools {
		if !strings.HasPrefix(tool.Name, "save_case_") {
			continue
		}
		expected, err := json.Marshal(plain[tool.Name].Schema)
		if err != nil {
			t.Fatal(err)
		}
		actual, err := json.Marshal(tool.InputSchema)
		if err != nil || string(expected) != string(actual) {
			t.Fatalf("MCP/plain schema mismatch for %s: %s != %s, %v", tool.Name, actual, expected, err)
		}
		matched++
	}
	if matched != 3 {
		t.Fatalf("MCP advertised %d generated save schemas, want 3: %s", matched, response.Body.String())
	}
}
