package llm

import (
	"context"
	"encoding/json"
	"slices"
	"testing"

	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/toolcapabilities"
)

func cliInventoryResponseForTest(inventory string) *Response {
	acc := newCLIStreamAccumulator()
	if inventory == "" {
		acc.AddLine([]byte(`{"type":"system","subtype":"init"}`))
	} else {
		acc.AddLine([]byte(`{"type":"system","subtype":"init","tools":` + inventory + `}`))
	}
	return acc.Response()
}

func TestCLIInventoryPresence(t *testing.T) {
	for _, tc := range []struct {
		name, inventory string
		state           CLIInventoryObservation
	}{
		{"empty", `[]`, CLIInventoryValid},
		{"controls", `["ExitPlanMode"]`, CLIInventoryValid},
		{"objects", `[{"name":"ExitPlanMode"}]`, CLIInventoryValid},
		{"missing", ``, CLIInventoryNotObserved},
		{"null", `null`, CLIInventoryInvalid},
		{"string", `"Bash"`, CLIInventoryInvalid},
		{"object", `{"name":"Bash"}`, CLIInventoryInvalid},
		{"number_entry", `[42]`, CLIInventoryInvalid},
		{"null_entry", `[null]`, CLIInventoryInvalid},
		{"empty_entry", `[""]`, CLIInventoryInvalid},
		{"blank_entry", `["  "]`, CLIInventoryInvalid},
		{"nameless_entry", `[{}]`, CLIInventoryInvalid},
		{"invalid_name", `[{"name":42}]`, CLIInventoryInvalid},
		{"partial", `["ExitPlanMode",42]`, CLIInventoryInvalid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp := cliInventoryResponseForTest(tc.inventory)
			if resp.CLIInventory != tc.state {
				t.Fatalf("inventory=%q, want %q", resp.CLIInventory, tc.state)
			}
			// Persisted completion responses retain valid-empty versus absent.
			raw, err := json.Marshal(resp)
			if err != nil {
				t.Fatal(err)
			}
			var restored Response
			if err := json.Unmarshal(raw, &restored); err != nil {
				t.Fatal(err)
			}
			surface := cliInventoryPlanForTest(t, false, nil)
			_, observationErr := observeCLIResponse(surface, &restored)
			for name, err := range map[string]error{
				"observer": observationErr,
				"managed":  ValidateCLIProviderCapabilitySurface(surface, &restored),
				"fork":     validateClaudeInvocationProviderBuiltins(newClaudeInvocationToolProjection(nil, nil), &restored),
			} {
				if (err == nil) != (tc.state == CLIInventoryValid) {
					t.Fatalf("%s error=%v, inventory=%q", name, err, tc.state)
				}
			}
		})
	}
}

func TestCLIInventoryCannotBeManufacturedOrRepairedByMessages(t *testing.T) {
	for _, prefix := range []string{"", `{"type":"system","subtype":"init","tools":null}`} {
		acc := newCLIStreamAccumulator()
		acc.AddLine([]byte(prefix))
		for _, line := range []string{
			`{"type":"assistant","tools":[],"message":{"content":[{"type":"text","text":"ok"}]}}`,
			`{"type":"result","tools":[],"mcp_servers":[{"name":"runtime-tools","status":"connected"}],"result":"ok"}`,
		} {
			acc.AddLine([]byte(line))
		}
		if _, err := exactCLIProviderVisibleTools(acc.Response()); err == nil {
			t.Fatal("message metadata manufactured valid inventory")
		}
		if len(acc.Response().MCPServers) != 0 {
			t.Fatal("message fabricated MCP connection")
		}
		if prefix != "" {
			acc.AddLine([]byte(`{"type":"system","subtype":"init","tools":[]}`))
			if acc.Response().CLIInventory != CLIInventoryInvalid {
				t.Fatal("invalid observation was erased")
			}
		}
	}
}

func cliInventoryPlanForTest(t *testing.T, bash bool, names []string) managedcapabilities.Surface {
	t.Helper()
	actor := runtimeactors.AgentConfig{ID: "inventory-agent", NativeTools: runtimeactors.NativeToolConfig{Bash: bash}}
	actor.Identity = testAgentIdentity(actor.ID, "")
	var definitions []ToolDefinition
	var capabilities []toolcapabilities.Capability
	for _, name := range names {
		definitions = append(definitions, ToolDefinition{Name: name, Schema: map[string]any{"type": "object"}})
		capabilities = append(capabilities, toolcapabilities.Capability{Name: name, Kind: toolcapabilities.KindStandard, Visible: true, Callable: true})
	}
	surface, err := managedCapabilityPlanForTest(runtimeactors.WithActor(context.Background(), actor), &ClaudeCLIRuntime{}, "", definitions, toolcapabilities.NewSet(capabilities), nativeCapabilityTestAuthority())
	if err != nil {
		t.Fatal(err)
	}
	var evidence []managedcapabilities.DeliveryEvidence
	for _, name := range surface.PlannedBindingNames(managedcapabilities.BindingMCPTool) {
		evidence = append(evidence, managedcapabilities.DeliveryEvidence{BindingKind: managedcapabilities.BindingMCPTool, ExactName: name, Kind: evidenceMCPListed, Status: managedcapabilities.EvidenceConfirmed})
	}
	if len(evidence) > 0 {
		surface, err = surface.Observe(evidence...)
		if err != nil {
			t.Fatal(err)
		}
	}
	return surface
}

func TestCLIInventoryChannelIntegrity(t *testing.T) {
	for _, tc := range []struct {
		name, inventory  string
		bash             bool
		mcp              []string
		connected, valid bool
	}{
		{"mcp_only", `["ExitPlanMode","mcp__runtime-tools__read_chat","mcp__runtime-tools__emit_telegram_reply_requested"]`, false, []string{"read_chat", "emit_telegram_reply_requested"}, true, true},
		{"native_only", `["Bash"]`, true, nil, false, true},
		{"mixed", `["Bash",{"name":"mcp__runtime-tools__read_chat"}]`, true, []string{"read_chat"}, true, true},
		{"collision", `["Bash","mcp__runtime-tools__bash"]`, true, []string{"bash"}, true, false},
		{"mcp_collision_only", `["mcp__runtime-tools__Bash"]`, false, nil, true, false},
		{"none", `[]`, false, nil, false, true},
		{"controls", `["ExitPlanMode"]`, false, nil, false, true},
		{"unexpected_native", `["Bash"]`, false, nil, false, false},
		{"unknown_native", `["FutureBuiltin"]`, false, nil, false, false},
		{"wrong_native_family", `["Read"]`, true, nil, false, false},
		{"missing_native", `["mcp__runtime-tools__bash"]`, true, []string{"bash"}, true, false},
		{"unplanned_mcp", `["mcp__runtime-tools__unexpected"]`, false, nil, true, false},
		{"disconnected_mcp", `["mcp__runtime-tools__read_chat"]`, false, []string{"read_chat"}, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			surface := cliInventoryPlanForTest(t, tc.bash, tc.mcp)
			resp := cliInventoryResponseForTest(tc.inventory)
			if tc.connected {
				resp.MCPServers = map[string]string{"runtime-tools": "connected"}
			}
			observed, err := observeCLIResponse(surface, resp)
			if err != nil {
				t.Fatal(err)
			}
			if err := ValidateCLIProviderCapabilitySurface(observed, resp); (err == nil) != tc.valid {
				t.Fatalf("managed validity=%v, want %v", err, tc.valid)
			}
			if !tc.connected {
				for _, name := range tc.mcp {
					if slices.Contains(observed.EffectiveNames(), name) {
						t.Fatalf("disconnected MCP tool %s became callable", name)
					}
				}
			}
			// Fork sandbox never inherits actor-native authority, even with the
			// same canonical MCP display name. Its native inventory must be zero.
			err = validateClaudeInvocationProviderBuiltins(newClaudeInvocationToolProjection(nil, nil), resp)
			if (err == nil) != (len(resp.ProviderVisibleTools) == 0) {
				t.Fatalf("fork validation=%v, native=%v", err, resp.ProviderVisibleTools)
			}
		})
	}
}

func TestCLIInventoryForkDisplayPreservesEmptyAndLocalPolicy(t *testing.T) {
	tools := []ToolDefinition{{Name: "query_entities"}, {Name: "emit_done"}}
	empty := cliInventoryResponseForTest(`[]`)
	if !conversationForkSandboxHasObservedSurface(empty) {
		t.Fatal("explicit empty inventory treated as absent")
	}
	if got := conversationForkSandboxObservedToolsForTurn(tools, empty); len(got) != 0 {
		t.Fatalf("empty observed=%v", got)
	}
	if got := conversationForkSandboxUsableToolsForTurn(tools, empty); !slices.Equal(got, conversationForkSandboxLocalFallbackTools(tools)) {
		t.Fatalf("empty invented MCP authority: %v", got)
	}
	mcp := cliInventoryResponseForTest(`["mcp__runtime-tools__query_entities"]`)
	if got := conversationForkSandboxObservedToolsForTurn(tools, mcp); !slices.Equal(got, []string{"query_entities"}) {
		t.Fatalf("canonical display=%v", got)
	}
	if native, err := exactCLIProviderVisibleTools(mcp); err != nil || len(native) != 0 {
		t.Fatalf("display became native: %v %v", native, err)
	}
	collision := cliInventoryResponseForTest(`["Bash","mcp__runtime-tools__bash"]`)
	if !slices.Equal(collision.ProviderVisibleTools, []string{"Bash"}) || !slices.Equal(collision.MCPVisibleTools, []string{"mcp__runtime-tools__bash"}) || !slices.Equal(collision.VisibleTools, []string{"bash"}) {
		t.Fatalf("collision lost exact channel: %#v", collision)
	}
}
