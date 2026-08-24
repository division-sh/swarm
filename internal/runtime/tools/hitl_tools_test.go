package tools

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	llm "github.com/division-sh/swarm/internal/runtime/llm"
	runtimemcp "github.com/division-sh/swarm/internal/runtime/mcp"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestManagedHITLProjectionOwnsDefinitionsGrantsAndExecution(t *testing.T) {
	actor := models.AgentConfig{ExecutionMode: "live", ID: "operator-agent"}
	exec := NewExecutorWithOptions(nil, ExecutorOptions{})

	withoutPermission := definitionMap(exec.ToolDefinitionsForActor(actor))
	notify, ok := withoutPermission[NotifyHumanToolName]
	if !ok {
		t.Fatal("notify_human is not auto-granted")
	}
	if _, ok := withoutPermission[AskHumanToolName]; ok {
		t.Fatal("ask_human was delivered without its permission")
	}
	if _, ok := withoutPermission[WithheldAgentMessageTool]; ok {
		t.Fatal("agent_message was delivered while typed recipient authority is unavailable")
	}
	if notify.Description != "Sends an informational notice to the human operator. Does NOT request approval and does not pause the flow - to ask for a decision that gates the flow, use ask_human." {
		t.Fatalf("notify_human description = %q", notify.Description)
	}
	properties, _ := notify.Schema.(map[string]any)["properties"].(map[string]any)
	if len(properties) != 2 || properties["summary"] == nil || properties["context"] == nil {
		t.Fatalf("notify_human schema properties = %#v", properties)
	}

	actor.Permissions = []string{AskHumanToolName}
	withPermission := definitionMap(exec.ToolDefinitionsForActor(actor))
	if _, ok := withPermission[AskHumanToolName]; !ok {
		t.Fatal("ask_human was not delivered with its exact permission")
	}

	actor.Tools = []string{WithheldAgentMessageTool}
	if _, ok := definitionMap(exec.ToolDefinitionsForActor(actor))[WithheldAgentMessageTool]; ok {
		t.Fatal("authored tools reintroduced agent_message")
	}
	if err := NewToolAuthorizer(nil, nil).Authorize(unmanagedToolTestContext(), actor, WithheldAgentMessageTool); err == nil {
		t.Fatal("agent_message execution authorization succeeded")
	}
}

func TestNotifyHumanPersistsExactRuntimeOwnedNotice(t *testing.T) {
	store := &mailboxStoreStub{id: "4fc7648d-61d8-4ed6-a940-fbf8819a81f5"}
	exec := NewExecutorWithOptions(nil, ExecutorOptions{MailboxStore: store, AuthorityProvider: allowMailboxAuthority{}})
	actor := models.AgentConfig{ExecutionMode: "live", ID: "reviewer", EntityID: "entity-1", FlowPath: "review/instance-1"}
	ctx := WithActor(context.Background(), actor)

	result, err := exec.Execute(ctx, NotifyHumanToolName, map[string]any{
		"summary": "Strong match found",
		"context": map[string]any{"candidate": "case-7"},
	})
	if err != nil {
		t.Fatalf("Execute(notify_human): %v", err)
	}
	if got := result.(map[string]any)["mailbox_id"]; got != store.id {
		t.Fatalf("mailbox_id = %v, want %s", got, store.id)
	}
	if store.last.Type != NotifyHumanMailboxItemType || store.last.Priority != "normal" || store.last.Status != "pending" {
		t.Fatalf("stored notice semantics = %#v", store.last)
	}
	if store.last.FromAgent != actor.ID || store.last.EntityID != actor.EntityID || store.last.FlowInstance != actor.FlowPath {
		t.Fatalf("stored notice provenance = %#v", store.last)
	}
	if !store.last.TimeoutAt.IsZero() || strings.TrimSpace(store.last.Summary) != "Strong match found" {
		t.Fatalf("stored notice timing/summary = %#v", store.last)
	}
	if _, err := exec.Execute(ctx, NotifyHumanToolName, map[string]any{"summary": "bad", "priority": "urgent"}); err == nil {
		t.Fatal("notify_human admitted author-controlled priority")
	}
}

func TestManagedHITLNamesRejectEveryNonPlatformOwnerAndHandlerDisposition(t *testing.T) {
	var transportCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		transportCalls.Add(1)
	}))
	defer server.Close()

	dispositions := []struct {
		name  string
		entry runtimecontracts.ToolSchemaEntry
	}{
		{name: "platform_builtin", entry: retiredToolEntry(runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerPlatformBuiltin))},
		{name: "http", entry: retiredToolEntry(
			runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerHTTP),
			runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{Method: http.MethodPost, URL: server.URL}),
		)},
		{name: "mcp", entry: retiredToolEntry(
			runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerMCP),
			runtimecontracts.WithToolMCP(runtimecontracts.MustToolMCPBinding("hostile", "send")),
		)},
		{name: "channel", entry: retiredToolEntry(
			runtimecontracts.WithToolCategory("channel_operation"),
			runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerChannel),
		)},
		{name: "unspecified", entry: retiredToolEntry()},
	}

	for _, toolName := range []string{NotifyHumanToolName, AskHumanToolName} {
		for _, scope := range []string{"root", "project", "flow"} {
			for _, disposition := range dispositions {
				t.Run(toolName+"/"+scope+"/"+disposition.name, func(t *testing.T) {
					entry := runtimecontracts.AgentRegistryEntry{ID: "worker", Tools: []string{toolName}}
					if toolName == AskHumanToolName {
						entry.Permissions = []string{AskHumanToolName}
					}
					source := retiredToolSourceForScope(scope,
						map[string]runtimecontracts.AgentRegistryEntry{"worker": entry},
						map[string]runtimecontracts.ToolSchemaEntry{toolName: disposition.entry},
						runtimecontracts.PolicyDocument{},
					)
					actor := managedHITLTestActor(t, source)
					exec := NewExecutorWithOptions(nil, ExecutorOptions{WorkflowSource: source})

					if _, err := exec.contractDefinitionsForActor(&actor); err == nil || !strings.Contains(err.Error(), "owned by the platform HITL contract") {
						t.Fatalf("provider catalog error = %v, want exact platform-owner rejection", err)
					}
					findings := ValidateConfiguredToolFulfillability(source, nil)
					if len(findings) != 1 || findings[0].ToolName != "" || !strings.Contains(findings[0].Reason, toolName) || !strings.Contains(findings[0].Reason, "owned by the platform HITL contract") {
						t.Fatalf("verification findings = %#v, want exact managed-owner hard invalidity", findings)
					}
					if _, err := exec.dispatchTool(WithActor(context.Background(), actor), actor, toolName, map[string]any{}); err == nil || !strings.Contains(err.Error(), "owned by the platform HITL contract") {
						t.Fatalf("dispatch error = %v, want exact platform-owner rejection", err)
					}
				})
			}
		}
	}
	if got := transportCalls.Load(); got != 0 {
		t.Fatalf("hostile managed-tool declarations reached external transport %d time(s)", got)
	}
}

func TestManagedHITLNamesRejectDiscoveredMCPRedefinition(t *testing.T) {
	for _, toolName := range []string{NotifyHumanToolName, AskHumanToolName} {
		t.Run(toolName, func(t *testing.T) {
			entry := retiredToolEntry(
				runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerMCP),
				runtimecontracts.WithToolMCP(runtimecontracts.MustToolMCPBinding("hostile", "send")),
			)
			_, err := executionToolsForRuntime(semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}), map[string]runtimemcp.DiscoveredTool{
				toolName: {Name: toolName, Contract: entry},
			})
			if err == nil || !strings.Contains(err.Error(), "discovered MCP declaration") {
				t.Fatalf("discovered redefinition error = %v, want exact owner rejection", err)
			}
		})
	}
}

func TestHITLIdentityLifecycleRejectsSourceDefinitionsWithoutAgents(t *testing.T) {
	var transportCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		transportCalls.Add(1)
	}))
	defer server.Close()

	identities := []struct {
		name     string
		teaching string
	}{
		{name: NotifyHumanToolName, teaching: "owned by the platform HITL contract"},
		{name: AskHumanToolName, teaching: "owned by the platform HITL contract"},
		{name: WithheldAgentMessageTool, teaching: agentMessageUnavailableTeaching},
		{name: "mailbox_send", teaching: "use notify_human"},
		{name: "human_task_request", teaching: "use ask_human"},
	}
	for _, identity := range identities {
		for _, scope := range []string{"root", "project", "flow"} {
			t.Run(identity.name+"/"+scope, func(t *testing.T) {
				source := retiredToolSourceForScope(scope, nil, map[string]runtimecontracts.ToolSchemaEntry{
					identity.name: retiredToolEntry(
						runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerHTTP),
						runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{Method: http.MethodPost, URL: server.URL}),
					),
				}, runtimecontracts.PolicyDocument{})

				if declarations := semanticview.AgentDeclarations(source); len(declarations) != 0 {
					t.Fatalf("agent declarations = %#v, want zero", declarations)
				}
				if _, err := ValidateToolImplementations(source); err == nil || !strings.Contains(err.Error(), identity.teaching) {
					t.Fatalf("implementation validation error = %v, want %q", err, identity.teaching)
				}
				findings := ValidateConfiguredToolFulfillability(source, nil)
				if len(findings) != 1 || !strings.Contains(findings[0].Reason, identity.teaching) {
					t.Fatalf("configured-tool findings = %#v, want one %q rejection", findings, identity.teaching)
				}
				if _, err := toolDefinitionsForRuntime(source, nil); err == nil || !strings.Contains(err.Error(), identity.teaching) {
					t.Fatalf("provider catalog error = %v, want %q", err, identity.teaching)
				}
				exec := NewExecutorWithOptions(nil, ExecutorOptions{WorkflowSource: source})
				actor := models.AgentConfig{ExecutionMode: "live", ID: "hostile", Tools: []string{identity.name}}
				if _, err := exec.dispatchTool(WithActor(context.Background(), actor), actor, identity.name, map[string]any{}); err == nil || !strings.Contains(err.Error(), identity.teaching) {
					t.Fatalf("dispatch error = %v, want %q", err, identity.teaching)
				}
			})
		}
	}
	if got := transportCalls.Load(); got != 0 {
		t.Fatalf("invalid HITL source definitions reached external transport %d time(s)", got)
	}
}

func TestHITLIdentityLifecycleRejectsDiscoveredCandidates(t *testing.T) {
	identities := []struct {
		name     string
		teaching string
	}{
		{name: NotifyHumanToolName, teaching: "owned by the platform HITL contract"},
		{name: AskHumanToolName, teaching: "owned by the platform HITL contract"},
		{name: WithheldAgentMessageTool, teaching: agentMessageUnavailableTeaching},
		{name: "mailbox_send", teaching: "use notify_human"},
		{name: "human_task_request", teaching: "use ask_human"},
	}
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{})
	for _, identity := range identities {
		t.Run(identity.name, func(t *testing.T) {
			discovered := map[string]runtimemcp.DiscoveredTool{
				identity.name: {
					Name: identity.name,
					Contract: retiredToolEntry(
						runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerMCP),
						runtimecontracts.WithToolMCP(runtimecontracts.MustToolMCPBinding("hostile", "send")),
					),
				},
			}
			if _, err := executionToolsForRuntime(source, discovered); err == nil || !strings.Contains(err.Error(), identity.teaching) {
				t.Fatalf("runtime catalog error = %v, want %q", err, identity.teaching)
			}
			findings := ValidateConfiguredToolFulfillability(source, discovered)
			if len(findings) != 1 || findings[0].ToolName != identity.name || !strings.Contains(findings[0].Reason, identity.teaching) {
				t.Fatalf("discovered findings = %#v, want one %q rejection", findings, identity.teaching)
			}
		})
	}
}

func TestHITLIdentityLifecycleRejectsRuntimeMCPAliases(t *testing.T) {
	identities := []struct {
		name     string
		teaching string
	}{
		{name: NotifyHumanToolName, teaching: "owned by the platform HITL contract"},
		{name: AskHumanToolName, teaching: "owned by the platform HITL contract"},
		{name: WithheldAgentMessageTool, teaching: agentMessageUnavailableTeaching},
		{name: "mailbox_send", teaching: "use notify_human"},
		{name: "human_task_request", teaching: "use ask_human"},
	}
	for _, identity := range identities {
		t.Run(identity.name, func(t *testing.T) {
			alias := "mcp__runtime-tools__" + identity.name
			entry := retiredToolEntry(
				runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerMCP),
				runtimecontracts.WithToolMCP(runtimecontracts.MustToolMCPBinding("hostile", "send")),
			)
			source := retiredToolSourceForScope("root", nil, map[string]runtimecontracts.ToolSchemaEntry{alias: entry}, runtimecontracts.PolicyDocument{})
			if errs := ValidateHITLIdentityLifecycleReferences(source); len(errs) != 1 || !strings.Contains(errs[0].Error(), identity.teaching) {
				t.Fatalf("source lifecycle errors = %v, want one %q rejection", errs, identity.teaching)
			}
			discovered := map[string]runtimemcp.DiscoveredTool{alias: {Name: alias, Contract: entry}}
			if _, err := executionToolsForRuntime(semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}), discovered); err == nil || !strings.Contains(err.Error(), identity.teaching) {
				t.Fatalf("discovered alias error = %v, want %q", err, identity.teaching)
			}
		})
	}
}

func TestHITLIdentityLifecycleRejectsWithheldAndRetiredReferences(t *testing.T) {
	identities := []struct {
		name     string
		teaching string
	}{
		{name: WithheldAgentMessageTool, teaching: agentMessageUnavailableTeaching},
		{name: "mailbox_send", teaching: "use notify_human"},
		{name: "human_task_request", teaching: "use ask_human"},
	}
	for _, identity := range identities {
		for _, scope := range []string{"root", "project", "flow"} {
			for _, surface := range []string{"configured_tool", "direct_permission", "permission_bundle"} {
				t.Run(identity.name+"/"+scope+"/"+surface, func(t *testing.T) {
					entry := runtimecontracts.AgentRegistryEntry{ID: "worker"}
					policy := runtimecontracts.PolicyDocument{}
					agents := map[string]runtimecontracts.AgentRegistryEntry{"worker": entry}
					switch surface {
					case "configured_tool":
						entry.Tools = []string{identity.name}
						agents["worker"] = entry
					case "direct_permission":
						entry.Permissions = []string{identity.name}
						agents["worker"] = entry
					case "permission_bundle":
						agents = nil
						policy = hitlLifecyclePermissionBundle(identity.name)
					}
					source := retiredToolSourceForScope(scope, agents, nil, policy)
					errs := ValidateHITLIdentityLifecycleReferences(source)
					if len(errs) != 1 || !strings.Contains(errs[0].Error(), identity.teaching) {
						t.Fatalf("lifecycle errors = %v, want one %q rejection", errs, identity.teaching)
					}
					if findings := ValidateConfiguredToolFulfillability(source, nil); len(findings) != 1 || !strings.Contains(findings[0].Reason, identity.teaching) {
						t.Fatalf("configured-tool findings = %#v, want one %q rejection", findings, identity.teaching)
					}
					_, permissionErrors := ValidateAgentPermissions(source)
					if len(permissionErrors) != 1 || !strings.Contains(permissionErrors[0].Error(), identity.teaching) {
						t.Fatalf("permission errors = %v, want one %q rejection", permissionErrors, identity.teaching)
					}
				})
			}
		}
	}
}

func TestHITLIdentityLifecycleRejectsDirectDispatchBeforeResolution(t *testing.T) {
	for _, identity := range []struct {
		name     string
		teaching string
	}{
		{name: WithheldAgentMessageTool, teaching: agentMessageUnavailableTeaching},
		{name: "mailbox_send", teaching: "use notify_human"},
		{name: "human_task_request", teaching: "use ask_human"},
	} {
		for _, name := range []string{identity.name, "mcp__runtime-tools__" + identity.name} {
			t.Run(name, func(t *testing.T) {
				var resolverCalls atomic.Int32
				var mcpCalls atomic.Int32
				dispatcher := NewToolDispatcher(nil,
					func(models.AgentConfig, string) (ExecutionTool, bool, error) {
						resolverCalls.Add(1)
						entry := retiredToolEntry(
							runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerMCP),
							runtimecontracts.WithToolMCP(runtimecontracts.MustToolMCPBinding("hostile", "send")),
						)
						tool, _ := executionToolFromAdmitted(identity.name, entry)
						return tool, true, nil
					}, nil,
					func(context.Context, models.AgentConfig, ExecutionTool, any) (any, error) {
						mcpCalls.Add(1)
						return map[string]any{"mutated": true}, nil
					}, nil, nil, nil,
				)
				if _, err := dispatcher.Dispatch(context.Background(), models.AgentConfig{ID: "hostile"}, name, map[string]any{}); err == nil || !strings.Contains(err.Error(), identity.teaching) {
					t.Fatalf("Dispatch error = %v, want %q", err, identity.teaching)
				}
				if resolverCalls.Load() != 0 || mcpCalls.Load() != 0 {
					t.Fatalf("invalid identity reached resolver=%d mcp=%d", resolverCalls.Load(), mcpCalls.Load())
				}

				mailbox := &mailboxStoreStub{}
				exec := NewExecutorWithOptions(nil, ExecutorOptions{MailboxStore: mailbox})
				actor := models.AgentConfig{ExecutionMode: "live", ID: "hostile", Tools: []string{identity.name}, Permissions: []string{identity.name}}
				if _, err := exec.Execute(WithActor(context.Background(), actor), name, map[string]any{}); err == nil || !strings.Contains(err.Error(), identity.teaching) {
					t.Fatalf("Execute error = %v, want %q", err, identity.teaching)
				}
				if mailbox.last.Type != "" {
					t.Fatalf("invalid identity mutated mailbox: %#v", mailbox.last)
				}
			})
		}
	}
}

func TestHITLIdentityLifecycleRejectsPermissionExpansion(t *testing.T) {
	for _, identity := range []struct {
		name     string
		teaching string
	}{
		{name: WithheldAgentMessageTool, teaching: agentMessageUnavailableTeaching},
		{name: "mailbox_send", teaching: "use notify_human"},
		{name: "human_task_request", teaching: "use ask_human"},
	} {
		for _, surface := range []string{"direct", "bundle"} {
			t.Run(identity.name+"/"+surface, func(t *testing.T) {
				entry := runtimecontracts.AgentRegistryEntry{ID: "worker"}
				policy := runtimecontracts.PolicyDocument{}
				if surface == "direct" {
					entry.Permissions = []string{identity.name}
				} else {
					entry.PermissionsBundle = "operators"
					policy = hitlLifecyclePermissionBundle(identity.name)
				}
				if _, err := resolveAgentPermissionsFromPolicy(entry, policy); err == nil || !strings.Contains(err.Error(), identity.teaching) {
					t.Fatalf("permission expansion error = %v, want %q", err, identity.teaching)
				}
			})
		}
	}
}

func TestHITLIdentityLifecyclePreservesActiveReferencesAndAskHumanPermission(t *testing.T) {
	entry := runtimecontracts.AgentRegistryEntry{
		ID:          "worker",
		Tools:       []string{NotifyHumanToolName, AskHumanToolName},
		Permissions: []string{AskHumanToolName},
	}
	source := retiredToolSourceForScope("root", map[string]runtimecontracts.AgentRegistryEntry{"worker": entry}, nil, runtimecontracts.PolicyDocument{})
	if errs := ValidateHITLIdentityLifecycleReferences(source); len(errs) != 0 {
		t.Fatalf("active references rejected: %v", errs)
	}
	if findings := ValidateConfiguredToolFulfillability(source, nil); len(findings) != 0 {
		t.Fatalf("active references unfulfillable: %#v", findings)
	}
	permissions, err := ResolveAgentPermissions(source, "", entry)
	if err != nil {
		t.Fatalf("ResolveAgentPermissions: %v", err)
	}
	if got := fmt.Sprint(permissions); got != "[ask_human]" {
		t.Fatalf("permissions = %s, want [ask_human]", got)
	}
}

func hitlLifecyclePermissionBundle(name string) runtimecontracts.PolicyDocument {
	return runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
		"permission_bundles": {Value: map[string]any{
			"operators": map[string]any{"permissions": []any{name}},
		}},
	}}
}

func managedHITLTestActor(t *testing.T, source semanticview.Source) models.AgentConfig {
	t.Helper()
	declarations := semanticview.AgentDeclarations(source)
	if len(declarations) != 1 {
		t.Fatalf("agent declarations = %#v, want one", declarations)
	}
	plan, err := semanticview.ScopedAgentNamePlan(source, declarations[0])
	if err != nil {
		t.Fatalf("resolve agent name plan: %v", err)
	}
	actor := nativeToolAgentConfig(plan.AgentID, plan.EffectiveRole(declarations[0].Entry), declarations[0].Entry)
	actor.FlowID = plan.OwnerFlowID
	permissions, err := ResolveAgentPermissions(source, plan.OwnerFlowID, declarations[0].Entry)
	if err != nil {
		t.Fatalf("resolve agent permissions: %v", err)
	}
	actor.Permissions = permissions
	if strings.TrimSpace(actor.ID) == "" {
		t.Fatal("managed HITL test actor has no public id")
	}
	return actor
}

func TestRetiredHITLNamesFailClosed(t *testing.T) {
	exec := NewExecutorWithOptions(nil, ExecutorOptions{})
	actor := models.AgentConfig{ExecutionMode: "live", ID: "reviewer", Tools: []string{"mailbox_send", "human_task_request"}}
	definitions := definitionMap(exec.ToolDefinitionsForActor(actor))
	for _, retired := range actor.Tools {
		if _, ok := definitions[retired]; ok {
			t.Fatalf("retired tool %s was delivered", retired)
		}
		if _, err := exec.Execute(WithActor(context.Background(), actor), retired, map[string]any{}); err == nil {
			t.Fatalf("retired tool %s executed", retired)
		}
	}
}

func definitionMap(definitions []llm.ToolDefinition) map[string]llm.ToolDefinition {
	out := make(map[string]llm.ToolDefinition, len(definitions))
	for _, definition := range definitions {
		out[definition.Name] = definition
	}
	return out
}
