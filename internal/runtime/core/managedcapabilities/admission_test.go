package managedcapabilities

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/toolcapabilities"
)

func TestPlannedToolAdmissionDoesNotSupplyAuthorityOrDeliveryEvidence(t *testing.T) {
	tool := PlannedTool{
		Name: "event.publish", DefinitionHash: "definition-hash",
		Capability: toolcapabilities.Capability{Name: "event.publish", Visible: true, Callable: true},
		Bindings:   []DeliveryBinding{{Kind: BindingMCPTool, ExactName: "event.publish", RequiredEvidenceKind: "mcp_listed"}},
	}
	plan := Plan{
		ActorPlan: managedCapabilityTestPlan(t, "worker"), RuntimeMode: "startup_probe",
		Provider: "claude_cli", Transport: "cli", ProviderContract: "cli.v1",
		CreatedAt: time.Unix(1, 0).UTC(), Tools: []PlannedTool{tool},
	}
	if err := ValidatePlannedTools(plan.Tools); err != nil {
		t.Fatal(err)
	}
	if _, err := New(plan); err == nil {
		t.Fatal("successful input admission supplied missing execution authority")
	}
	plan.Authority = Authority{
		Kind: AuthorityStartupProbe, ID: "00000000-0000-4000-8000-000000000801",
		ExecutionKind: ExecutionNormalAgent, ExecutionAuthorityID: "runtime-owner",
		StartupOwnerID: "startup-owner", StartupGeneration: 1,
	}
	before, err := New(plan)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidatePlannedTools(plan.Tools); err != nil {
		t.Fatal(err)
	}
	after, err := New(plan)
	if err != nil || !reflect.DeepEqual(before, after) || len(after.EffectiveNames()) != 0 || len(after.Tools[0].Evidence) != 0 {
		t.Fatalf("admission changed identity or confirmed delivery: before=%#v after=%#v err=%v", before, after, err)
	}
	if err := after.ValidateEffective(); err == nil {
		t.Fatal("admitted input replaced the required provider/MCP delivery proof")
	}
}

func TestPlannedToolAdmissionAndSurfaceShareShapeRefusals(t *testing.T) {
	base := PlannedTool{
		Name: "probe", DefinitionHash: "definition-hash",
		Bindings: []DeliveryBinding{{Kind: BindingMCPTool, ExactName: "probe", RequiredEvidenceKind: "mcp_listed"}},
	}
	for _, item := range []struct {
		name string
		edit func(*PlannedTool)
	}{
		{"missing_name", func(tool *PlannedTool) { tool.Name = "" }},
		{"missing_identity", func(tool *PlannedTool) { tool.DefinitionHash = "" }},
		{"invalid_binding", func(tool *PlannedTool) { tool.Bindings[0].Kind = "not-a-binding" }},
		{"missing_binding_name", func(tool *PlannedTool) { tool.Bindings[0].ExactName = "" }},
		{"missing_evidence_kind", func(tool *PlannedTool) { tool.Bindings[0].RequiredEvidenceKind = "" }},
	} {
		t.Run(item.name, func(t *testing.T) {
			tool := base
			tool.Bindings = append([]DeliveryBinding(nil), base.Bindings...)
			item.edit(&tool)
			admissionErr := ValidatePlannedTools([]PlannedTool{tool})
			_, surfaceErr := New(Plan{
				ActorPlan: managedCapabilityTestPlan(t, "worker"), RuntimeMode: "startup_probe",
				Provider: "claude_cli", Transport: "cli", ProviderContract: "cli.v1", Tools: []PlannedTool{tool},
				Authority: Authority{
					Kind: AuthorityStartupProbe, ID: "00000000-0000-4000-8000-000000000801",
					ExecutionKind: ExecutionNormalAgent, ExecutionAuthorityID: "runtime-owner",
					StartupOwnerID: "startup-owner", StartupGeneration: 1,
				},
			})
			if admissionErr == nil || surfaceErr == nil || admissionErr.Error() != surfaceErr.Error() {
				t.Fatalf("shape interpreters disagree: admission=%v surface=%v", admissionErr, surfaceErr)
			}
		})
	}
	if err := ValidatePlannedTools([]PlannedTool{base, base}); err == nil || !strings.Contains(err.Error(), "duplicated") {
		t.Fatalf("duplicate tool admitted: %v", err)
	}
}
