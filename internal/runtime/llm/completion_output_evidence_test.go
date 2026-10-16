package llm

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/toolcapabilities"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
	"github.com/google/uuid"
)

func TestCompletionOutputEvidenceUsesExecutionCoordinateAndFrozenGrants(t *testing.T) {
	for _, adapter := range []string{"anthropic_api", "claude_cli"} {
		t.Run(adapter, func(t *testing.T) {
			harness := effecttest.New()
			ctx := managedEffectHarnessContext(t, harness, t.Name())
			base, ok := managedcapabilities.FromContext(ctx)
			if !ok {
				t.Fatal("missing fixture surface")
			}
			surface, err := managedcapabilities.New(managedcapabilities.Plan{
				ActorIdentity: base.ActorIdentity, RuntimeMode: base.RuntimeMode, Provider: base.Provider, Transport: base.Transport,
				ProviderContract: base.ProviderContract, Authority: base.Authority, CreatedAt: base.CreatedAt,
				Tools: []managedcapabilities.PlannedTool{{Name: "emit_done", DefinitionHash: strings.Repeat("a", 64),
					Capability: toolcapabilities.Capability{Name: "emit_done", Kind: toolcapabilities.KindEmit, Visible: true, Callable: true},
					Bindings:   []managedcapabilities.DeliveryBinding{{Kind: managedcapabilities.BindingAPIDefinition, ExactName: "emit_done", RequiredEvidenceKind: "api_definition"}}}},
			})
			if err != nil {
				t.Fatal(err)
			}
			surface, err = surface.Observe(managedcapabilities.DeliveryEvidence{BindingKind: managedcapabilities.BindingAPIDefinition, ExactName: "emit_done", Kind: "api_definition", Status: managedcapabilities.EvidenceConfirmed})
			if err != nil {
				t.Fatal(err)
			}
			authority := ToolOutputAuthority{ProviderOperationID: uuid.NewString(), SettledAt: time.Unix(10, 0).UTC()}
			call := ToolCall{ID: " original-call ", Name: " emit_done "}
			messages := []Message{{Role: "user", Content: "start"}, {Role: "assistant", Content: "emit"}}
			usage := unavailableCompletionUsage()
			usage.Evidence = runtimeeffects.CompletionEvidence{
				Request:        runtimeeffects.CompletionRequest{ModelAlias: "regular", RequestedModel: "frozen", BackendProfile: "anthropic", Provider: "anthropic", Transport: "api"},
				CostProvenance: runtimeeffects.CompletionCostUnavailable,
			}
			usage.Evidence = usage.Evidence.Canonical()
			envelope := completionContinuationEnvelope{
				Version: completionContinuationVersion, Adapter: adapter, Usage: usage,
				Response:   Response{Message: messages[1], Raw: json.RawMessage(`{"result":"emit"}`), ToolCalls: []ToolCall{call}, ToolOutputAuthority: &authority},
				Projection: completionProjection{SessionID: uuid.NewString(), ExpectedTurnCount: 6, TurnCount: 7, Messages: messages, Identity: base.ActorIdentity},
			}
			payload, err := json.Marshal(envelope)
			if err != nil {
				t.Fatal(err)
			}
			got, err := ReadCompletionOutputCandidates(payload, adapter, authority.ProviderOperationID, surface)
			if err != nil {
				t.Fatal(err)
			}
			executed, err := authority.eventIdentity(completionToolCallIdentity(7, 0, call))
			if err != nil {
				t.Fatal(err)
			}
			want := []CompletionOutputCandidate{{EventID: executed.EventID(), ToolName: call.Name, CreatedAt: executed.CreatedAt()}}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("read identity=%+v, execution=%+v", got, want)
			}
			if _, err := ReadCompletionOutputCandidates(payload, adapter, uuid.NewString(), surface); err == nil {
				t.Fatal("foreign provider operation admitted")
			}
			if denied, err := ReadCompletionOutputCandidates(payload, adapter, authority.ProviderOperationID, base); err != nil || len(denied) != 0 {
				t.Fatalf("denied proposal became output evidence or invalidated readable history: %+v %v", denied, err)
			}
			// The immutable provider row and proposed call remain insufficient to
			// claim that any publication happened.
			if harness.CompletionCount() != 0 {
				t.Fatal("read projection performed completion settlement")
			}
		})
	}
}
