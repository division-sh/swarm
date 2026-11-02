package manager

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/eventschema"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
)

func TestAgentContentCapturesExactPlatformSubscriptions(t *testing.T) {
	for _, nested := range []bool{false, true} {
		t.Run(fmt.Sprintf("nested_%t", nested), func(t *testing.T) {
			root := canonicalrouting.CopyLifecycleEmitterHumanTaskObserver(t, nested)
			flow, directory := ".", root
			if nested {
				flow, directory = "child", filepath.Join(root, "child")
			}
			path := filepath.Join(directory, "agents.yaml")
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			writeFlowActivationFixtureFile(t, path, string(raw)+"  content: |-\n    {\"task\": (event.type).endsWith(\"observer.requested\") ? \"request\" : \"outcome\", \"payload\": payload}\n")
			repo := canonicalrouting.RepoRoot(t)
			bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
			if err != nil {
				t.Fatal(err)
			}
			source, err := semanticview.CompileActivityToolBindings(semanticview.Wrap(bundle))
			if err != nil {
				t.Fatal(err)
			}
			declarations := semanticview.AgentDeclarationsForOwner(source, flow)
			if len(declarations) != 1 {
				t.Fatalf("captured declarations = %d, want 1", len(declarations))
			}
			declaration := declarations[0]
			cfg := runtimeactors.AgentConfig{ID: declaration.LocalID, FlowID: flow, FlowPath: source.FlowPath(flow), Intent: declaration.Entry.ResolvedIntent,
				Content: declaration.Entry.Content, Subscriptions: declaration.Entry.Subscriptions}
			before, err := semanticview.AdmitFlowOwnedAgentSubscriptions(source, semanticview.FlowOwnedAgentSubscriptionRequest{
				AgentID: cfg.ID, FlowID: cfg.FlowID, FlowPath: cfg.CanonicalFlowPath(), Subscriptions: cfg.Subscriptions,
			})
			if err != nil {
				t.Fatal(err)
			}
			admission, err := admitAgentConfigSubscriptions(source, &cfg, nil)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before.RoutePatterns(), admission.RoutePatterns()) || len(cfg.ContentPrograms) != 4 {
				t.Fatalf("content changed routes or omitted exact input programs: %v -> %v, programs=%d", before.RoutePatterns(), admission.RoutePatterns(), len(cfg.ContentPrograms))
			}
			for _, event := range []string{"human_task.approved", "human_task.rejected", "human_task.deferred", "human_task.expired"} {
				schema := semanticview.ResolveEventSchema(source, flow, event)
				if !schema.HasSchema || !schema.HasStructural || cfg.ContentPrograms[event] == nil {
					t.Fatalf("captured callback coverage missing for %s", event)
				}
				payload := map[string]any{"card_id": "00000000-0000-0000-0000-000000000130", "status": event[len("human_task."):]}
				if event == "human_task.approved" || event == "human_task.rejected" {
					payload["requester_agent_id"], payload["fields"], payload["decided_by"], payload["decided_at"] = "operator", map[string]any{}, "synthetic", "2026-11-02T00:00:00Z"
				} else {
					payload["cause"] = "synthetic"
					if event == "human_task.deferred" {
						payload["resume_at"] = "2026-11-02T00:00:00Z"
					} else {
						payload["deadline_at"] = "2026-11-02T00:00:00Z"
					}
				}
				if err := eventschema.ValidatePayloadAgainstSchema(schema.Schema.Schema, payload); err != nil {
					t.Fatal(err)
				}
				value, err := workflowexpr.EvalAgentContentExpression(cfg.ContentPrograms[event], workflowexpr.ValueContext{Payload: payload, Event: map[string]any{"type": event}})
				if err != nil {
					t.Fatal(err)
				}
				encoded, err := json.Marshal(value)
				if err != nil {
					t.Fatal(err)
				}
				var got map[string]any
				if json.Unmarshal(encoded, &got) != nil || !reflect.DeepEqual(got, map[string]any{"task": "outcome", "payload": payload}) {
					t.Fatalf("callback projection changed declared payload: %s", encoded)
				}
			}
			for _, subscription := range []string{"foreign/human_task.approved", "human_task.unknown", "human_task.*", "platform.runtime_log"} {
				t.Run(subscription, func(t *testing.T) {
					candidate := cfg
					candidate.ContentPrograms = nil
					candidate.Subscriptions = []string{subscription}
					if _, err := admitAgentConfigSubscriptions(source, &candidate, nil); err == nil || len(candidate.ContentPrograms) != 0 {
						t.Fatalf("content acquired a foreign, unknown, swept or schema-less input: %s, err=%v", subscription, err)
					}
				})
			}
		})
	}
}
