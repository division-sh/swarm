package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/durabledata"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/core/toolresultpolicy"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/flowdata"
	"github.com/division-sh/swarm/internal/runtime/llm"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type flowDataResourceStore struct {
	item durabledata.ResourceAccessItem
}

func (s flowDataResourceStore) LoadRunResourceAccess(_ context.Context, _ string, refs []durabledata.DeclarationRef) ([]durabledata.ResourceAccessItem, error) {
	if len(refs) != 1 || refs[0] != s.item.Declaration {
		return nil, fmt.Errorf("unexpected resource access refs: %#v", refs)
	}
	return []durabledata.ResourceAccessItem{s.item}, nil
}

func TestExecutorReadFlowDataReadsDeclaredFlowFile(t *testing.T) {
	source, _ := loadFlowDataToolSource(t)
	actor := flowDataActor()
	exec := NewExecutorWithOptions(nil, ExecutorOptions{WorkflowSource: source})

	defs := exec.ToolDefinitionsForActor(actor)
	if !containsToolName(toolDefinitionNames(defs), "read_flow_data") {
		t.Fatalf("read_flow_data missing from actor definitions: %v", toolDefinitionNames(defs))
	}
	if containsToolName(toolDefinitionNames(defs), "read_file") || containsToolName(toolDefinitionNames(defs), "write_file") {
		t.Fatalf("flow_data_access implied native file tools: %v", toolDefinitionNames(defs))
	}
	caps := exec.ToolCapabilitiesForActor(actor, []string{"read_flow_data"}, nil)
	cap, ok := caps.Capability("read_flow_data")
	if !ok || !cap.Visible || !cap.Callable || cap.AuthorizationClass != "flow_data_access" {
		t.Fatalf("read_flow_data capability = %#v, want visible/callable flow_data_access", cap)
	}

	out, err := exec.Execute(flowDataToolContext(actor), "read_flow_data", flowDataToolInput(t, source, actor, "exclusions.yaml"))
	if err != nil {
		t.Fatalf("Execute(read_flow_data): %v", err)
	}
	result, ok := out.(map[string]any)
	if !ok {
		t.Fatalf("result type = %T, want map[string]any", out)
	}
	if got := strings.TrimSpace(asString(result["content"])); got != "blocked: true" {
		t.Fatalf("content = %q, want blocked YAML", got)
	}
	if got := strings.TrimSpace(asString(result["content_type"])); got != "yaml" {
		t.Fatalf("content_type = %q, want yaml", got)
	}
	if got, ok := result["total_bytes"].(int); !ok || got == 0 {
		t.Fatalf("total_bytes = %#v, want non-zero int", result["total_bytes"])
	}
}

func TestExecutorReadFlowDataFailsClosedForUndeclaredAndEscapingFiles(t *testing.T) {
	source, _ := loadFlowDataToolSourceWithAccess(t, []string{"exclusions.yaml"})
	actor := flowDataActor()
	exec := NewExecutorWithOptions(nil, ExecutorOptions{WorkflowSource: source})

	if _, err := exec.Execute(flowDataToolContext(actor), "read_flow_data", map[string]any{"filename": "missing.yaml"}); err == nil {
		t.Fatal("Execute(read_flow_data missing.yaml) succeeded, want undeclared failure")
	}
	if _, err := exec.Execute(flowDataToolContext(actor), "read_flow_data", map[string]any{"relative_path": "../other/secret.yaml"}); err == nil {
		t.Fatal("Execute(read_flow_data traversal) succeeded, want traversal failure")
	}
}

func TestExecutorReadFlowDataNotVisibleWithoutDeclaration(t *testing.T) {
	source, _ := loadFlowDataToolSource(t)
	actor := models.AgentConfig{
		ExecutionMode:  "live",
		ID:             "other-agent",
		Role:           "other",
		FlowID:         "support",
		FlowPath:       "support",
		FlowDataAccess: []string{"exclusions.yaml"},
	}
	exec := NewExecutorWithOptions(nil, ExecutorOptions{WorkflowSource: source})

	if containsToolName(toolDefinitionNames(exec.ToolDefinitionsForActor(actor)), "read_flow_data") {
		t.Fatal("read_flow_data visible without flow_data_access declaration")
	}
	if _, err := exec.Execute(flowDataToolContext(actor), "read_flow_data", map[string]any{"filename": "exclusions.yaml"}); err == nil {
		t.Fatal("Execute(read_flow_data) succeeded without declaration")
	}
}

func TestExecutorReadFlowDataRejectsRoleModeImpersonation(t *testing.T) {
	source, _ := loadFlowDataToolSource(t)
	actor := models.AgentConfig{
		ExecutionMode: "live",
		ID:            "impostor",
		Role:          "factory_cto",
		FlowID:        "static",
		FlowPath:      "support",
	}
	exec := NewExecutorWithOptions(nil, ExecutorOptions{WorkflowSource: source})

	if containsToolName(toolDefinitionNames(exec.ToolDefinitionsForActor(actor)), "read_flow_data") {
		t.Fatal("read_flow_data visible through role/mode fallback impersonation")
	}
	caps := exec.ToolCapabilitiesForActor(actor, []string{"read_flow_data"}, nil)
	cap, ok := caps.Capability("read_flow_data")
	if !ok || cap.Visible || cap.Callable || cap.AuthorizationClass != "flow_data_access" {
		t.Fatalf("capability = %#v, want denied flow_data_access for role/mode impersonation", cap)
	}
	if _, err := exec.Execute(flowDataToolContext(actor), "read_flow_data", map[string]any{"filename": "exclusions.yaml"}); err == nil {
		t.Fatal("Execute(read_flow_data) succeeded through role/mode fallback impersonation")
	}
}

func TestExecutorReadFlowDataIgnoresMutableActorFlowDataAccess(t *testing.T) {
	source, root := loadFlowDataToolSource(t)
	actor := flowDataActor()
	actor.FlowDataAccess = []string{"escape.md"}
	exec := NewExecutorWithOptions(nil, ExecutorOptions{WorkflowSource: source})

	if err := os.WriteFile(filepath.Join(root, "support", "data", "escape.md"), []byte("mutable grant\n"), 0o644); err != nil {
		t.Fatalf("write escape.md: %v", err)
	}
	_, err := exec.Execute(flowDataToolContext(actor), "read_flow_data", map[string]any{"filename": "escape.md"})
	requireToolFailure(t, err, runtimefailures.ClassSchemaInvalid, "invalid_tool_input")
}

func TestExecutorReadFlowDataSelectsActorScopeForSharedEffectiveName(t *testing.T) {
	source, root := loadFlowDataToolSource(t)
	if err := os.MkdirAll(filepath.Join(root, "other", "data"), 0o755); err != nil {
		t.Fatalf("mkdir other data: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "other", "data", "exclusions.yaml"), []byte("blocked: other-flow\n"), 0o644); err != nil {
		t.Fatalf("write other exclusions: %v", err)
	}
	actor := flowDataActor()
	actor.FlowID = "other"
	actor.FlowPath = "other"
	exec := NewExecutorWithOptions(nil, ExecutorOptions{WorkflowSource: source})

	out, err := exec.Execute(flowDataToolContext(actor), "read_flow_data", flowDataToolInput(t, source, actor, "exclusions.yaml"))
	if err != nil {
		t.Fatalf("Execute(read_flow_data): %v", err)
	}
	result, ok := out.(map[string]any)
	if !ok {
		t.Fatalf("result type = %T, want map[string]any", out)
	}
	if got := strings.TrimSpace(asString(result["content"])); got != "blocked: other-flow" {
		t.Fatalf("content = %q, want other-flow contract-owned data root", got)
	}
}

func TestFlowBackedAgentConsumersUseOwningFlowInsteadOfStorageScopeKind(t *testing.T) {
	source, _ := loadFlowDataToolSource(t)
	actor := flowDataActor()
	declaration, ok := semanticview.ResolveAgentDeclaration(source, actor)
	if !ok || declaration.ScopeKind != "flow" || declaration.OwnerFlowID != "support" || declaration.Source.FlowPath != "support" {
		t.Fatalf("flow-backed declaration = %#v ok %v", declaration, ok)
	}
	staticData := flowdata.AllowedStaticData(source, actor)
	if len(staticData) != 1 || staticData[0].RelativePath != "exclusions.yaml" {
		t.Fatalf("flow-data static projection = %#v", staticData)
	}
	entry, flowID, ok := criteriaAgentContractDeclaration(source, actor)
	if !ok || flowID != "support" || entry.Role != "factory_cto" {
		t.Fatalf("criteria declaration = entry %#v flow %q ok %v", entry, flowID, ok)
	}
	entity, ok := entityruntime.ResolveForActor(source, actor)
	if !ok || entity.FlowID != "support" || entity.EntityType != "support_state" {
		t.Fatalf("entity contract = %#v ok %v", entity, ok)
	}
}

func TestExecutorReadFlowDataDiagnosticsUseFlowDataAuthorization(t *testing.T) {
	source, _ := loadFlowDataToolSource(t)
	actor := flowDataActor()
	bus := &telemetryBusStub{}
	exec := NewExecutorWithOptions(bus, ExecutorOptions{WorkflowSource: source})

	if _, err := exec.Execute(flowDataToolContext(actor), "read_flow_data", flowDataToolInput(t, source, actor, "exclusions.yaml")); err != nil {
		t.Fatalf("Execute(read_flow_data): %v", err)
	}
	if len(bus.logs) != 1 {
		t.Fatalf("runtime log count = %d, want 1", len(bus.logs))
	}
	detail, _ := bus.logs[0].Detail.(map[string]any)
	if got := strings.TrimSpace(asString(detail["authorization_class"])); got != "flow_data_access" {
		t.Fatalf("authorization_class = %q, want flow_data_access (detail=%#v)", got, detail)
	}
	if got := strings.TrimSpace(asString(detail["context_requirement"])); got != "actor_context" {
		t.Fatalf("context_requirement = %q, want actor_context", got)
	}
}

func TestExecutorReadFlowDataCursorBindsRunActorContentAndOffset(t *testing.T) {
	_, root := loadFlowDataToolSource(t)
	content := strings.Repeat("<tag>&", 6_000) + strings.Repeat("é", 5_000) + strings.Repeat("z", 37)
	if err := os.WriteFile(filepath.Join(root, "support", "data", "exclusions.yaml"), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(runtimepipeline.WorkflowRepoRoot(), root, runtimecontracts.DefaultPlatformSpecFile(runtimepipeline.WorkflowRepoRoot()))
	if err != nil {
		t.Fatal(err)
	}
	source := semanticview.Wrap(bundle)
	actor := flowDataActor()
	declaration, ok := semanticview.ResolveAgentDeclaration(source, actor)
	if !ok {
		t.Fatal("resolve flow-data actor declaration")
	}
	plan, err := semanticview.ScopedAgentNamePlan(source, declaration)
	if err != nil {
		t.Fatal(err)
	}
	actor.Identity = agentidentitytest.Declared(t, plan.AgentID, plan.OwnerURI, actor.FlowID, "one", actor.FlowID+"/one")
	exec := NewExecutorWithOptions(nil, ExecutorOptions{WorkflowSource: source})
	input := flowDataToolInput(t, source, actor, "exclusions.yaml")
	var reconstructed strings.Builder
	var firstContinuation durabledata.PageContinuation
	pages := 0
	for {
		pageRaw, executeErr := exec.Execute(flowDataToolContext(actor), "read_flow_data", input)
		if executeErr != nil {
			t.Fatal(executeErr)
		}
		page := pageRaw.(map[string]any)
		encoded, marshalErr := json.Marshal(page)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		if len(encoded) > toolresultpolicy.MaxInlineToolResultBytes {
			t.Fatalf("serialized page bytes = %d, exceed inline result limit %d", len(encoded), toolresultpolicy.MaxInlineToolResultBytes)
		}
		chunk := page["content"].(string)
		if chunk == "" {
			t.Fatal("nonterminal static-data page made no progress")
		}
		reconstructed.WriteString(chunk)
		continuation := page["continuation"].(durabledata.PageContinuation)
		pages++
		if pages == 1 {
			firstContinuation = continuation
		}
		if continuation.State == "end" {
			break
		}
		if continuation.State != "more" || continuation.Cursor == "" {
			t.Fatalf("page %d continuation = %#v", pages, continuation)
		}
		input = flowDataToolInput(t, source, actor, "exclusions.yaml")
		input["cursor"] = continuation.Cursor
	}
	if pages < 3 || reconstructed.String() != content {
		t.Fatalf("paged content = %d bytes over %d pages, want exact %d bytes over multiple pages", reconstructed.Len(), pages, len(content))
	}
	if firstContinuation.State != "more" || firstContinuation.Cursor == "" {
		t.Fatalf("first continuation = %#v", firstContinuation)
	}
	secondInput := flowDataToolInput(t, source, actor, "exclusions.yaml")
	secondInput["cursor"] = firstContinuation.Cursor

	otherRun := runtimecorrelation.WithRunID(unmanagedToolTestContext(), "22222222-2222-4222-8222-222222222222")
	if _, err := exec.Execute(models.WithActor(otherRun, actor), "read_flow_data", secondInput); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("cross-run cursor error = %v", err)
	}
	otherActor := actor
	otherActor.Identity = agentidentitytest.Declared(t, plan.AgentID, plan.OwnerURI, actor.FlowID, "two", actor.FlowID+"/two")
	if _, err := exec.Execute(flowDataToolContext(otherActor), "read_flow_data", secondInput); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("cross-actor cursor error = %v", err)
	}
	tampered := secondInput
	cursor := []byte(firstContinuation.Cursor)
	if cursor[len(cursor)-1] == 'A' {
		cursor[len(cursor)-1] = 'B'
	} else {
		cursor[len(cursor)-1] = 'A'
	}
	tampered["cursor"] = string(cursor)
	if _, err := exec.Execute(flowDataToolContext(actor), "read_flow_data", tampered); err == nil || !strings.Contains(err.Error(), "cursor is invalid") {
		t.Fatalf("tampered cursor error = %v", err)
	}
}

func TestExecutorReadResourceDataFitsInlineEnvelopeAndExactArrayBudget(t *testing.T) {
	source, _ := loadResourceDataToolSource(t)
	actor := flowDataActorWithIdentity(t, source, "resource-reader")
	refs := flowdata.AllowedResourceData(source, actor)
	if len(refs) != 1 {
		t.Fatalf("resource refs = %#v, want one", refs)
	}
	payloads := make([]string, 9)
	for index := range payloads {
		payloads[index] = strings.Repeat("<é", 700) + fmt.Sprintf("-%02d", index)
	}
	item := flowDataResourceAccessItem(t, refs[0], payloads)
	exec := NewExecutorWithOptions(nil, ExecutorOptions{WorkflowSource: source, DataAccessStore: flowDataResourceStore{item: item}})

	var ordinals []uint64
	page := map[string]any{"limit": durabledata.MaxPublicPageItems, "byte_limit": durabledata.MaxToolPageBytes}
	pages := 0
	for {
		resultRaw, err := exec.Execute(flowDataToolContext(actor), "read_flow_data", map[string]any{
			"kind": "resource_rows", "declaration": refs[0], "page": page,
		})
		if err != nil {
			t.Fatal(err)
		}
		result := resultRaw.(map[string]any)
		encoded, err := json.Marshal(result)
		if err != nil {
			t.Fatal(err)
		}
		if len(encoded) > toolresultpolicy.MaxInlineToolResultBytes {
			t.Fatalf("serialized resource page bytes = %d, exceed inline limit %d", len(encoded), toolresultpolicy.MaxInlineToolResultBytes)
		}
		rows := result["rows"].(durabledata.PageResult[map[string]any])
		if err := rows.Validate(); err != nil || rows.EncodedItemsBytes > durabledata.MaxToolPageBytes {
			t.Fatalf("resource page = %#v, validation error %v", rows, err)
		}
		for _, row := range rows.Items {
			ordinals = append(ordinals, row["ordinal"].(uint64))
		}
		pages++
		if rows.Continuation.State == "end" {
			break
		}
		if rows.Continuation.State != "more" || rows.Continuation.Cursor == "" || rows.ItemCount == 0 {
			t.Fatalf("resource page %d made no progress: %#v", pages, rows)
		}
		page = map[string]any{
			"limit": durabledata.MaxPublicPageItems, "byte_limit": durabledata.MaxToolPageBytes,
			"cursor": rows.Continuation.Cursor,
		}
	}
	if pages < 3 || len(ordinals) != len(payloads) {
		t.Fatalf("resource pagination = %d rows over %d pages, want %d rows over multiple pages", len(ordinals), pages, len(payloads))
	}
	for index, ordinal := range ordinals {
		if ordinal != uint64(index+1) {
			t.Fatalf("resource ordinals = %#v", ordinals)
		}
	}

	singleRaw, err := exec.Execute(flowDataToolContext(actor), "read_flow_data", map[string]any{
		"kind": "resource_row", "declaration": refs[0], "key": "row-00",
	})
	if err != nil {
		t.Fatal(err)
	}
	single := singleRaw.(map[string]any)
	if encoded, err := json.Marshal(single); err != nil || len(encoded) > toolresultpolicy.MaxInlineToolResultBytes {
		t.Fatalf("single resource result bytes = %d, error %v", len(encoded), err)
	}
	rowArray, err := json.Marshal([]map[string]any{single["row"].(map[string]any)})
	if err != nil {
		t.Fatal(err)
	}
	exactRaw, err := exec.Execute(flowDataToolContext(actor), "read_flow_data", map[string]any{
		"kind": "resource_rows", "declaration": refs[0],
		"page": map[string]any{"limit": 1, "byte_limit": len(rowArray)},
	})
	if err != nil {
		t.Fatal(err)
	}
	exactRows := exactRaw.(map[string]any)["rows"].(durabledata.PageResult[map[string]any])
	if exactRows.ItemCount != 1 || exactRows.EncodedItemsBytes != len(rowArray) {
		t.Fatalf("exact-budget resource page = %#v, want %d encoded bytes", exactRows, len(rowArray))
	}
	if _, err := exec.Execute(flowDataToolContext(actor), "read_flow_data", map[string]any{
		"kind": "resource_rows", "declaration": refs[0],
		"page": map[string]any{"limit": 1, "byte_limit": len(rowArray) - 1},
	}); err == nil || !strings.Contains(err.Error(), "page byte_limit") {
		t.Fatalf("undersized exact-array budget error = %v", err)
	}

	emptyItem := flowDataResourceAccessItem(t, refs[0], nil)
	emptyExec := NewExecutorWithOptions(nil, ExecutorOptions{WorkflowSource: source, DataAccessStore: flowDataResourceStore{item: emptyItem}})
	if _, err := emptyExec.Execute(flowDataToolContext(actor), "read_flow_data", map[string]any{
		"kind": "resource_rows", "declaration": refs[0],
		"page": map[string]any{"limit": 1, "byte_limit": 1},
	}); err == nil || !strings.Contains(err.Error(), "page byte_limit") {
		t.Fatalf("undersized empty-array budget error = %v", err)
	}
	emptyRaw, err := emptyExec.Execute(flowDataToolContext(actor), "read_flow_data", map[string]any{
		"kind": "resource_rows", "declaration": refs[0],
		"page": map[string]any{"limit": 1, "byte_limit": 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	emptyRows := emptyRaw.(map[string]any)["rows"].(durabledata.PageResult[map[string]any])
	if err := emptyRows.Validate(); err != nil || emptyRows.ItemCount != 0 || emptyRows.EncodedItemsBytes != 2 || emptyRows.Continuation.State != "end" {
		t.Fatalf("empty resource page = %#v, validation error %v", emptyRows, err)
	}
}

func TestExecutorReadResourceDataCursorIsBoundedAndTargetBound(t *testing.T) {
	eventName := strings.Repeat("a", 3_072)
	source, _ := loadResourceDataToolSourceWithEventName(t, eventName)
	actor := flowDataActorWithIdentity(t, source, "long-resource-reader")
	refs := flowdata.AllowedResourceData(source, actor)
	if len(refs) != 1 {
		t.Fatalf("resource refs = %#v, want one", refs)
	}
	item := flowDataResourceAccessItem(t, refs[0], []string{"first", "second"})
	exec := NewExecutorWithOptions(nil, ExecutorOptions{WorkflowSource: source, DataAccessStore: flowDataResourceStore{item: item}})

	firstRaw, err := exec.Execute(flowDataToolContext(actor), "read_flow_data", map[string]any{
		"kind": "resource_rows", "declaration": refs[0],
		"page": map[string]any{"limit": 1, "byte_limit": durabledata.MaxToolPageBytes},
	})
	if err != nil {
		t.Fatal(err)
	}
	first := firstRaw.(map[string]any)["rows"].(durabledata.PageResult[map[string]any])
	if first.ItemCount != 1 || first.Continuation.State != "more" {
		t.Fatalf("first resource page = %#v, want one row and continuation", first)
	}
	if len(first.Continuation.Cursor) >= 1_024 {
		t.Fatalf("resource cursor bytes = %d, want fixed-size cursor below 1024 bytes", len(first.Continuation.Cursor))
	}
	page, err := (durabledata.PageRequest{
		Limit: 1, ByteLimit: durabledata.MaxToolPageBytes, Cursor: first.Continuation.Cursor,
	}).WithDefaults()
	if err != nil {
		t.Fatalf("bounded resource cursor rejected by public page contract: %v", err)
	}
	secondRaw, err := exec.Execute(flowDataToolContext(actor), "read_flow_data", map[string]any{
		"kind": "resource_rows", "declaration": refs[0], "page": page,
	})
	if err != nil {
		t.Fatal(err)
	}
	second := secondRaw.(map[string]any)["rows"].(durabledata.PageResult[map[string]any])
	if second.ItemCount != 1 || second.Items[0]["ordinal"] != uint64(2) || second.Continuation.State != "end" {
		t.Fatalf("second resource page = %#v, want terminal ordinal 2", second)
	}

	changed := flowDataResourceAccessItem(t, refs[0], []string{"first", "changed"})
	selectedFingerprint, err := resourceDataTargetFingerprint(refs[0], item.VersionID)
	if err != nil {
		t.Fatal(err)
	}
	changedVersionFingerprint, err := resourceDataTargetFingerprint(refs[0], changed.VersionID)
	if err != nil {
		t.Fatal(err)
	}
	otherDeclaration := refs[0]
	otherDeclaration.EventName = strings.Repeat("b", 3_072)
	changedDeclarationFingerprint, err := resourceDataTargetFingerprint(otherDeclaration, item.VersionID)
	if err != nil {
		t.Fatal(err)
	}
	if selectedFingerprint == changedVersionFingerprint || selectedFingerprint == changedDeclarationFingerprint {
		t.Fatal("resource target fingerprint does not bind both declaration and version")
	}
	changedExec := NewExecutorWithOptions(nil, ExecutorOptions{WorkflowSource: source, DataAccessStore: flowDataResourceStore{item: changed}})
	if _, err := changedExec.Execute(flowDataToolContext(actor), "read_flow_data", map[string]any{
		"kind": "resource_rows", "declaration": refs[0], "page": page,
	}); err == nil || !strings.Contains(err.Error(), "cursor does not match") {
		t.Fatalf("cross-version resource cursor error = %v", err)
	}
}

func TestExecutorReadResourceDataRejectsIntrinsicallyOversizedRowsBeforeProjection(t *testing.T) {
	source, _ := loadResourceDataToolSource(t)
	actor := flowDataActorWithIdentity(t, source, "oversized-resource-reader")
	ref := flowdata.AllowedResourceData(source, actor)[0]
	item := flowDataResourceAccessItem(t, ref, []string{strings.Repeat("<é", 4_000)})
	exec := NewExecutorWithOptions(nil, ExecutorOptions{WorkflowSource: source, DataAccessStore: flowDataResourceStore{item: item}})
	for _, input := range []map[string]any{
		{"kind": "resource_row", "declaration": ref, "key": "row-00"},
		{"kind": "resource_rows", "declaration": ref, "page": map[string]any{"limit": 1, "byte_limit": durabledata.MaxToolPageBytes}},
	} {
		if _, err := exec.Execute(flowDataToolContext(actor), "read_flow_data", input); err == nil || !strings.Contains(err.Error(), "inline tool result byte limit") {
			t.Fatalf("oversized %s error = %v", input["kind"], err)
		}
	}
}

func TestExecutorReadFlowDataRequiresWorkflowSource(t *testing.T) {
	actor := flowDataActor()
	exec := NewExecutorWithOptions(nil, ExecutorOptions{})

	if containsToolName(toolDefinitionNames(exec.ToolDefinitionsForActor(actor)), "read_flow_data") {
		t.Fatal("read_flow_data visible without workflow source")
	}
	caps := exec.ToolCapabilitiesForActor(actor, []string{"read_flow_data"}, nil)
	cap, ok := caps.Capability("read_flow_data")
	if !ok || cap.Visible || cap.Callable || cap.AuthorizationClass != "flow_data_access" {
		t.Fatalf("capability = %#v, want denied flow_data_access without source", cap)
	}
	if _, err := exec.Execute(flowDataToolContext(actor), "read_flow_data", map[string]any{"filename": "exclusions.yaml"}); err == nil {
		t.Fatal("Execute(read_flow_data) succeeded without workflow source")
	}
}

func flowDataActor() models.AgentConfig {
	return models.AgentConfig{
		ExecutionMode:  "live",
		ID:             "public-factory-cto",
		Role:           "factory_cto",
		FlowID:         "support",
		FlowPath:       "support",
		FlowDataAccess: []string{"exclusions.yaml"},
	}
}

func flowDataToolContext(actor models.AgentConfig) context.Context {
	ctx := runtimecorrelation.WithRunID(unmanagedToolTestContext(), "11111111-1111-4111-8111-111111111111")
	return models.WithActor(ctx, actor)
}

func flowDataToolInput(t *testing.T, source semanticview.Source, actor models.AgentConfig, relativePath string) map[string]any {
	t.Helper()
	for _, item := range flowdata.AllowedStaticData(source, actor) {
		if item.RelativePath == relativePath {
			return map[string]any{"kind": "static_file", "static_id": item.StaticID}
		}
	}
	t.Fatalf("actor %s has no static data for %s", actor.ID, relativePath)
	return nil
}

func loadFlowDataToolSource(t *testing.T) (semanticview.Source, string) {
	t.Helper()
	return loadFlowDataToolSourceWithAccess(t, []string{"exclusions.yaml"})
}

func loadFlowDataToolSourceWithAccess(t *testing.T, access []string) (semanticview.Source, string) {
	t.Helper()
	root := t.TempDir()

	writeToolFlowDataFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: flow-data-test\n")

	writeToolFlowDataFixtureFile(t, filepath.Join(root, "support", "schema.yaml"), "name: support\nmode: static\n")
	writeToolFlowDataFixtureFile(t, filepath.Join(root, "support", "entities.yaml"), "support_state:\n  support_id: string\n")
	writeToolFlowDataFixtureFile(t, filepath.Join(root, "support", "agents.yaml"), `
factory-cto:
  id: public-factory-cto
  role: factory_cto
  intent: {inline: "Read only the flow data declared by this contract."}
`+toolFlowDataAccessYAML(access))
	writeToolFlowDataFixtureFile(t, filepath.Join(root, "support", "data", "exclusions.yaml"), "blocked: true\n")

	writeToolFlowDataFixtureFile(t, filepath.Join(root, "other", "schema.yaml"), "name: other\nmode: static\n")
	writeToolFlowDataFixtureFile(t, filepath.Join(root, "other", "entities.yaml"), "other_state:\n  other_id: string\n")
	writeToolFlowDataFixtureFile(t, filepath.Join(root, "other", "agents.yaml"), `
factory-cto:
  id: public-factory-cto
  role: other_factory_cto
  intent: {inline: "Read only the other flow data declared by this contract."}
`+toolFlowDataAccessYAML(access))
	writeToolFlowDataFixtureFile(t, filepath.Join(root, "other", "data", "exclusions.yaml"), "blocked: other-flow\n")

	repoRoot := runtimepipeline.WorkflowRepoRoot()
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides(%s): %v", root, err)
	}
	return semanticview.Wrap(bundle), root
}

func loadResourceDataToolSource(t *testing.T) (semanticview.Source, string) {
	return loadResourceDataToolSourceWithEventName(t, "records.loaded")
}

func loadResourceDataToolSourceWithEventName(t *testing.T, eventName string) (semanticview.Source, string) {
	t.Helper()
	root := t.TempDir()

	writeToolFlowDataFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: resource-data-test\n")

	writeToolFlowDataFixtureFile(t, filepath.Join(root, "support", "schema.yaml"), "name: support\nmode: static\n")
	writeToolFlowDataFixtureFile(t, filepath.Join(root, "support", "entities.yaml"), "support_state:\n  support_id: string\n")
	writeToolFlowDataFixtureFile(t, filepath.Join(root, "support", "agents.yaml"), fmt.Sprintf(`
factory-cto:
  id: public-factory-cto
  role: factory_cto
  intent: {inline: "Read only the durable data declared by this contract."}
  data_access:
    - data: support/%s
`, eventName))
	writeToolFlowDataFixtureFile(t, filepath.Join(root, "support", "events.yaml"), fmt.Sprintf(`
? %q
:
  id: text
  payload: text
`, eventName))

	repoRoot := runtimepipeline.WorkflowRepoRoot()
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides(%s): %v", root, err)
	}
	return semanticview.Wrap(bundle), root
}

func flowDataActorWithIdentity(t *testing.T, source semanticview.Source, instance string) models.AgentConfig {
	t.Helper()
	actor := flowDataActor()
	declaration, ok := semanticview.ResolveAgentDeclaration(source, actor)
	if !ok {
		t.Fatal("resolve resource-data actor declaration")
	}
	plan, err := semanticview.ScopedAgentNamePlan(source, declaration)
	if err != nil {
		t.Fatal(err)
	}
	actor.Identity = agentidentitytest.Declared(t, plan.AgentID, plan.OwnerURI, actor.FlowID, instance, actor.FlowID+"/"+instance)
	return actor
}

func flowDataResourceAccessItem(t *testing.T, ref durabledata.DeclarationRef, payloads []string) durabledata.ResourceAccessItem {
	t.Helper()
	schema := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"id":      map[string]any{"type": "string"},
			"payload": map[string]any{"type": "string"},
		},
		"required":             []any{"id", "payload"},
		"additionalProperties": false,
	}
	var input strings.Builder
	for index, payload := range payloads {
		raw, err := json.Marshal(map[string]any{"id": fmt.Sprintf("row-%02d", index), "payload": payload})
		if err != nil {
			t.Fatal(err)
		}
		input.Write(raw)
		input.WriteByte('\n')
	}
	compiled, defects := durabledata.CompileJSONL(ref, schema, "id", []byte(input.String()))
	if len(defects) != 0 {
		t.Fatalf("CompileJSONL defects = %#v", defects)
	}
	return durabledata.ResourceAccessItem{
		Kind: "resource", Declaration: ref, VersionID: compiled.VersionID, SchemaDigest: compiled.Manifest.SchemaDigest,
		RowCount: uint64(len(compiled.Rows)), BusinessKey: "id", Schema: compiled.CanonicalSchema, Content: compiled.CanonicalJSONL,
	}
}

func toolFlowDataAccessYAML(access []string) string {
	if len(access) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString("  flow_data_access:\n")
	for _, item := range access {
		b.WriteString("    - ")
		b.WriteString(item)
		b.WriteString("\n")
	}
	return b.String()
}

func writeToolFlowDataFixtureFile(t *testing.T, path string, contents string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func toolDefinitionNames(defs []llm.ToolDefinition) []string {
	names := make([]string, 0, len(defs))
	for _, def := range defs {
		names = append(names, def.Name)
	}
	return names
}
