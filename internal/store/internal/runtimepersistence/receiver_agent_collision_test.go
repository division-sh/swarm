package runtimepersistence

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/runforkreadiness"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestReceiverAgentBusinessNamespaceNativeBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, scenario := range []string{"runtime_names", "inert_prompt_json"} {
			t.Run(backend+"/"+scenario, func(t *testing.T) {
				f := newReceiverConfigActivationFixture(t, backend)
				req := f.request("business-key", "ti-collision", "authored-label")
				values := canonicalrouting.ReceiverAgentCollisionValues()
				for key, value := range values {
					prompt := key == "nested" || key == "records"
					if (scenario == "inert_prompt_json") != prompt {
						continue
					}
					f.bundle.FlowTree.ByID["review"].Schema.InstanceVariables.Variables[key] = contracts.FlowVariable{Type: "json"}
					req.Config[key] = value
				}
				plan, err := f.manager.PrepareFlowInstanceActivation(f.ctx, req)
				if err != nil {
					t.Fatalf("admitted business values rejected before creation: %v", err)
				}
				if err := f.manager.ActivateFlowInstance(f.ctx, req); err != nil {
					t.Fatalf("admitted business values rejected by native agent activation: %v", err)
				}
				f.requireConfig(t, plan)
				recheckHistory := receiverAgentHistoricalProjection(t, f, plan)
				read := func() actors.AgentConfig {
					agents, err := f.store.LoadAgents(f.ctx)
					if err != nil || len(agents) != 1 {
						t.Fatalf("agent hydration: agents=%d err=%v", len(agents), err)
					}
					cfg := agents[0].Config
					requireNativeReceiverAgentCarrier(t, cfg, plan.Instance.Config)
					if cfg.FlowPath != plan.Identity.InstancePath || cfg.Model != "regular" || cfg.Role != "reviewer" || len(cfg.Tools) != 0 || len(cfg.Permissions) != 0 || cfg.NativeTools.Any() {
						t.Fatalf("business config replaced actor authority: %+v", cfg)
					}
					if cfg.Memory.Enabled || !reflect.DeepEqual(cfg.Intent, f.bundle.FlowTree.ByID["review"].Agents["reviewer"].ResolvedIntent) {
						t.Fatalf("business config replaced authored memory/intent: %+v", cfg)
					}
					return cfg
				}
				before := read()
				req.Config = map[string]any{"request_id": "foreign-key", "model": "replacement", "nested": false}
				if created, err := f.manager.EnsureFlowInstance(f.ctx, req); err != nil || created {
					t.Fatalf("reuse must retain committed config: created=%v err=%v", created, err)
				}
				if !reflect.DeepEqual(before, read()) {
					t.Fatal("reuse changed agent descriptor or business values")
				}
				if err := f.manager.Shutdown(); err != nil {
					t.Fatal(err)
				}
				restarted := f.restartedReceiverConfigManager(t, nil)
				if created, err := restarted.manager.EnsureFlowInstance(f.ctx, req); err != nil || created {
					t.Fatalf("restart must retain committed config: created=%v err=%v", created, err)
				}
				restarted.requireConfig(t, plan)
				if !reflect.DeepEqual(before, read()) {
					t.Fatal("restart changed agent authority or business values")
				}
				recheckHistory()
			})
		}
	}
}

// Native creation writes the historical config. Only the recipient frontier is
// a component projection input: this does not execute an unsupported template fork.
func receiverAgentHistoricalProjection(t *testing.T, f receiverConfigActivationFixture, activation pipeline.FlowInstanceActivationPlan) func() {
	t.Helper()
	var revision int64
	var raw string
	query := `SELECT revision,CAST(fact AS TEXT) FROM run_fork_fact_revisions WHERE run_id=$1 AND family='entity_metadata' AND fact_key=$2 AND present=TRUE ORDER BY revision LIMIT 1`
	if err := f.db.QueryRowContext(f.ctx, query, activation.Readiness.RunID, activation.Identity.EntityID).Scan(&revision, &raw); err != nil {
		t.Fatal(err)
	}
	project := func() {
		var metadata struct {
			EntityID     string          `json:"entity_id"`
			FlowInstance string          `json:"flow_instance"`
			EntityType   string          `json:"entity_type"`
			FlowConfig   json.RawMessage `json:"flow_config"`
		}
		if err := json.Unmarshal([]byte(raw), &metadata); err != nil {
			t.Fatal(err)
		}
		if metadata.EntityID != activation.Identity.EntityID || metadata.FlowInstance != activation.Identity.InstancePath {
			t.Fatalf("foreign historical metadata: %s", raw)
		}
		eventID := "11111111-1111-4111-8111-111111111111"
		plan := runfork.RunForkPlan{SourceRunID: activation.Readiness.RunID, ForkPoint: runfork.RunForkPoint{Revision: revision}, Entities: []runfork.RunForkEntityState{{EntityID: metadata.EntityID, MaterializationMetadata: &runfork.RunForkMaterializedEntitySnapshotMetadata{
			Owner: runfork.RunForkMaterializedEntitySnapshotMetadataOwner, Source: runfork.RunForkMaterializedEntitySnapshotMetadataSourceEntityState, FlowInstance: metadata.FlowInstance, EntityType: metadata.EntityType, FlowConfig: metadata.FlowConfig,
		}}}}
		plan = plan.WithHistoricalEvents(revision, []string{eventID})
		target, err := events.NewExistingEntityTarget(events.RouteIdentity{FlowID: "review", FlowInstance: metadata.FlowInstance, EntityID: metadata.EntityID})
		if err != nil {
			t.Fatal(err)
		}
		plan.PendingWork = []runfork.RunForkPendingWork{{EventID: eventID, EventName: metadata.FlowInstance + "/task.started", FlowInstance: metadata.FlowInstance, RoutingSource: eventtest.ConcreteTemplateRoutingSource("review", metadata.FlowInstance, metadata.EntityID), DeliveryRoute: events.DeliveryRoute{Target: target}, Classification: runfork.RunForkPendingClassificationPending}}
		source := semanticview.Wrap(f.bundle)
		frontier, err := runforkadmission.AdmitContractFrontier(runforkadmission.ContractFrontierRequest{Plan: plan, Source: source, ContractSelection: runfork.RunForkContractSelection{Mode: "selected_contracts"}})
		if err != nil {
			t.Fatal(err)
		}
		planning := runfork.RunForkSelectedContractRecipientPlanning{}
		for _, event := range frontier.FrontierEvents {
			planning.RecipientPlanEvents = append(planning.RecipientPlanEvents, runfork.RunForkSelectedContractRecipientPlanEvent{SourceEventID: event.SourceEventID, EventName: event.EventName, Recipients: event.DerivedRecipients})
		}
		projection, err := runforkreadiness.Project(plan, source, planning, map[string]executionmode.Mode{eventID: executionmode.Live}, manager.AgentManagerOptions{ExecutionPosture: executionposture.Live, LLMBackend: "anthropic"})
		if err != nil {
			t.Fatal(err)
		}
		if len(projection.Blueprints) != 1 || len(projection.States) != 1 || len(projection.Flows) != 1 {
			t.Fatalf("missing historical agent projection: %+v", projection)
		}
		requireNativeReceiverAgentCarrier(t, projection.Blueprints[0].Config, activation.Instance.Config)
		for _, values := range []map[string]any{projection.States[0].Config, projection.Flows[0].Config} {
			got, err := canonicaljson.MarshalPreservingNumberKinds(values)
			if err != nil {
				t.Fatal(err)
			}
			want, err := canonicaljson.MarshalPreservingNumberKinds(activation.Instance.Config)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != string(want) {
				t.Fatalf("historical config=%s want=%s", got, want)
			}
		}
	}
	project()
	return func() {
		var later string
		if err := f.db.QueryRowContext(f.ctx, `SELECT CAST(fact AS TEXT) FROM run_fork_fact_revisions WHERE run_id=$1 AND family='entity_metadata' AND fact_key=$2 AND revision=$3`, activation.Readiness.RunID, activation.Identity.EntityID, revision).Scan(&later); err != nil {
			t.Fatal(err)
		}
		if later != raw {
			t.Fatal("reuse/restart rewrote selected historical facts")
		}
		project()
	}
}

func requireNativeReceiverAgentCarrier(t *testing.T, cfg actors.AgentConfig, want map[string]any) {
	t.Helper()
	// Inspect the wire contract so the regression compiles on the pre-fix head.
	wire, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var carrier map[string]json.RawMessage
	if err := json.Unmarshal(wire, &carrier); err != nil {
		t.Fatal(err)
	}
	expected, err := canonicaljson.MarshalPreservingNumberKinds(want)
	if err != nil {
		t.Fatal(err)
	}
	requireReceiverConfigWire(t, carrier["receiver_config"], expected)
	if string(cfg.Config) != "{}" {
		t.Fatalf("receiver values leaked into opaque config: %s", cfg.Config)
	}
}

func TestReceiverAgentBusinessNamespaceKeepsAuthoredPromptGuard(t *testing.T) {
	for _, raw := range []string{`{"system_prompt":"override"}`, `{"nested":{"system_prompt":"override"}}`, `{"records":[{"system_prompt":"override"}]}`} {
		if err := actors.ValidateNoAuthoredSystemPrompt(json.RawMessage(raw)); err == nil || !strings.Contains(err.Error(), "RETIRED: authored config.") {
			t.Fatalf("authored opaque prompt override accepted: %s: %v", raw, err)
		}
	}
}
