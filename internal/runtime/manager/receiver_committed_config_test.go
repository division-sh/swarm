package manager

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestTemplateFlowMaterializationValidatesCompleteCommittedConfig(t *testing.T) {
	for _, tc := range []struct {
		name      string
		variables map[string]runtimecontracts.FlowVariable
		config    map[string]any
		wantErr   bool
	}{
		{"nil", nil, nil, true},
		{"empty", nil, map[string]any{}, false},
		{"optional default absent", map[string]runtimecontracts.FlowVariable{"count": {Type: "integer", IsOptional: true, HasDefault: true, Default: 99}}, map[string]any{}, false},
		{"required absent", map[string]runtimecontracts.FlowVariable{"count": {Type: "integer"}}, map[string]any{}, true},
		{"required default absent", map[string]runtimecontracts.FlowVariable{"count": {Type: "integer", HasDefault: true, Default: 99}}, map[string]any{}, true},
		{"wrong type", map[string]runtimecontracts.FlowVariable{"count": {Type: "integer"}}, map[string]any{"count": "7"}, true},
		{"null", map[string]runtimecontracts.FlowVariable{"count": {Type: "integer"}}, map[string]any{"count": nil}, true},
		{"selected type change", map[string]runtimecontracts.FlowVariable{"count": {Type: "boolean"}}, map[string]any{"count": int64(7)}, true},
		{"undeclared", nil, map[string]any{"count": int64(7)}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bundle := testFlowBundle(t, "")
			declareReceiverConfig(t, bundle, tc.variables)
			plan, err := TemplateFlowMaterialization(semanticview.Wrap(bundle), "review", "review/one", "entity-1", tc.config)
			if tc.wantErr {
				if err == nil || !strings.Contains(err.Error(), "receiver configuration") || len(plan.Agents) != 0 || len(plan.ActivationVariables) != 0 {
					t.Fatalf("invalid config acquired materialization: %#v %v", plan, err)
				}
				return
			}
			if err != nil || plan.Config == nil || !reflect.DeepEqual(plan.Config, tc.config) || len(plan.Agents) != 1 {
				t.Fatalf("valid committed config changed: %#v %v", plan, err)
			}
			if string(plan.Agents[0].Config.ReceiverConfig) != "{}" {
				t.Fatalf("empty config acquired defaults: %s", plan.Agents[0].Config.ReceiverConfig)
			}
		})
	}
}

func TestReceiverConfigRecoveryRejectsInvalidEvidenceBeforeMutation(t *testing.T) {
	for _, entry := range []string{"ensure", "standing", "startup", "source reconciliation", "callback"} {
		for _, mode := range []string{"nil", "empty", "wrong type", "null", "undeclared", "selected type change", "new required default"} {
			// Standing preparation and old callbacks require source canonicalization
			// first; Ensure/startup/source reconciliation own that transition below.
			if (entry == "callback" || entry == "standing") && (mode == "selected type change" || mode == "new required default") {
				continue
			}
			t.Run(entry+"/"+mode, func(t *testing.T) {
				instances := &flowActivationTestInstanceStore{}
				agents := &flowActivationTestStore{}
				firstBus := &flowActivationTestBus{routeStore: &flowActivationTestRouteStore{}}
				first := newFlowActivationManager(t, firstBus, instances, agents)
				bundle := testFlowBundle(t, "")
				declareReceiverConfig(t, bundle, map[string]runtimecontracts.FlowVariable{"count": {Type: "integer"}})
				setFlowActivationManagerSemanticSource(first, semanticview.Wrap(bundle))
				req := testActivationRequest(bundle, "review", "one", "parent", "review/one")
				req.Config = map[string]any{"count": 7}
				ctx := testAuthorActivityContext(context.Background())
				prepared, err := first.PrepareFlowInstanceActivation(ctx, req)
				if err != nil {
					t.Fatal(err)
				}
				if err := first.ActivateFlowInstance(ctx, req); err != nil {
					t.Fatal(err)
				}
				source := bundle
				stored := instances.byStorageRef["review/one"]
				validConfig := cloneFlowConfig(stored.Config)
				switch mode {
				case "nil":
					stored.Config = nil
				case "empty":
					stored.Config = map[string]any{}
				case "wrong type":
					stored.Config = map[string]any{"count": "corrupt"}
				case "null":
					stored.Config = map[string]any{"count": nil}
				case "undeclared":
					stored.Config = map[string]any{"count": 7, "foreign": true}
				case "selected type change", "new required default":
					source = testFlowBundle(t, "")
					variables := map[string]runtimecontracts.FlowVariable{"count": {Type: "boolean"}}
					if mode == "new required default" {
						variables = map[string]runtimecontracts.FlowVariable{"count": {Type: "integer"}, "new": {Type: "integer", HasDefault: true, Default: 99}}
					}
					declareReceiverConfig(t, source, variables)
				}
				instances.byStorageRef["review/one"] = stored
				fact := authorActivityTestSourceArtifactFact
				if source != bundle || entry == "source reconciliation" {
					fact, err = runtimecorrelation.NewSourceArtifactFact("bundle-v2:sha256:" + strings.Repeat("d", 64))
					if err != nil {
						t.Fatal(err)
					}
					ctx = runtimecorrelation.WithSourceArtifactFact(ctx, fact)
				}
				bus := &flowActivationTestBus{
					routeStore:         firstBus.routeStore,
					addedPaths:         append([]string(nil), firstBus.addedPaths...),
					addedRouteRequests: firstBus.addedRouteRequests,
				}
				restarted := newFlowActivationManager(t, bus, instances, agents)
				setFlowActivationManagerSemanticSource(restarted, semanticview.Wrap(source), fact)
				req.ContractBundle = semanticview.Wrap(source)
				req.Config = nil
				before, found, err := instances.LoadDynamicFlowRuntimeReadiness(ctx, req.TriggerEvent.RunID(), req.Instance.Route())
				if err != nil || !found {
					t.Fatalf("readiness missing: %v", err)
				}
				beforeJSON, err := canonicaljson.MarshalPreservingNumberKinds(before)
				if err != nil {
					t.Fatal(err)
				}
				upserts, armed := len(agents.upserts), len(instances.armedEntries)
				recover := func() error {
					switch entry {
					case "ensure":
						_, err := restarted.EnsureFlowInstance(ctx, req)
						return err
					case "standing":
						created, finish, err := restarted.PrepareStandingFlowInstance(ctx, req)
						if err != nil {
							return err
						}
						if created || finish == nil {
							t.Fatal("standing reuse lost existing identity")
						}
						return finish()
					case "startup":
						return reconcileDynamicFlowRuntimeStartupForTest(restarted, ctx, fact, true)
					case "source reconciliation":
						runCtx := worklifetime.WithOccurrence(runtimecorrelation.WithRunID(ctx, req.TriggerEvent.RunID()), restarted.workOwner)
						return restarted.ReconcileDynamicFlowRuntimeReadinessPlansForRun(runCtx, time.Now().UTC())
					default:
						return restarted.FinalizeCommittedFlowInstanceActivation(ctx, runtimepipeline.CommittedFlowInstanceActivation{Plan: prepared})
					}
				}
				for attempt := 0; attempt < 2; attempt++ {
					if err := recover(); err == nil || !strings.Contains(err.Error(), "receiver configuration") {
						t.Fatalf("invalid evidence recovery = %v", err)
					}
					after, found, err := instances.LoadDynamicFlowRuntimeReadiness(ctx, req.TriggerEvent.RunID(), req.Instance.Route())
					afterJSON, encodeErr := canonicaljson.MarshalPreservingNumberKinds(after)
					if err != nil || encodeErr != nil || !found || string(beforeJSON) != string(afterJSON) {
						t.Fatalf("rejection mutated readiness: %s => %s (%v, %v)", beforeJSON, afterJSON, err, encodeErr)
					}
					if len(agents.upserts) != upserts || len(agents.terminated) != 0 || len(instances.armedEntries) != armed || len(instances.creates) != 1 || !reflect.DeepEqual(bus.addedPaths, firstBus.addedPaths) || !reflect.DeepEqual(bus.addedRouteRequests, firstBus.addedRouteRequests) || len(bus.removedPairs) != 0 || len(bus.published) != 0 || len(restarted.ListAgentConfigs()) != 0 {
						t.Fatal("rejected evidence reached agent/route/lifecycle consumers")
					}
				}
				// A failed attempt must not poison a subsequent valid recovery.
				stored.Config = validConfig
				if mode == "selected type change" {
					stored.Config = map[string]any{"count": true}
				}
				if mode == "new required default" {
					stored.Config["new"] = int64(5)
				}
				instances.byStorageRef["review/one"] = stored
				if err := recover(); err != nil {
					t.Fatalf("retry with valid evidence: %v", err)
				}
				cfg, found := testFlowActivationAgentConfig(t, restarted, "reviewer", "review/one")
				want, err := canonicaljson.MarshalPreservingNumberKinds(stored.Config)
				if err != nil || !found || string(cfg.ReceiverConfig) != string(want) {
					t.Fatalf("retry lost exact config: %s want %s, %v", cfg.ReceiverConfig, want, err)
				}
			})
		}
	}
}

func TestEnsureReceiverConfigPreservesEmptyVersusMissingEvidence(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(map[bool]string{false: "empty", true: "missing"}[missing], func(t *testing.T) {
			instances := &flowActivationTestInstanceStore{}
			agents := &flowActivationTestStore{}
			first := newFlowActivationManager(t, &flowActivationTestBus{}, instances, agents)
			bundle := testFlowBundle(t, "")
			declareReceiverConfig(t, bundle, map[string]runtimecontracts.FlowVariable{})
			setFlowActivationManagerSemanticSource(first, semanticview.Wrap(bundle))
			req := testActivationRequest(bundle, "review", "one", "parent", "review/one")
			ctx := testAuthorActivityContext(context.Background())
			if err := first.ActivateFlowInstance(ctx, req); err != nil {
				t.Fatal(err)
			}
			if missing {
				stored := instances.byStorageRef["review/one"]
				stored.Config = nil
				instances.byStorageRef["review/one"] = stored
			}
			bus := &flowActivationTestBus{}
			restarted := newFlowActivationManager(t, bus, instances, agents)
			setFlowActivationManagerSemanticSource(restarted, semanticview.Wrap(bundle))
			upserts := len(agents.upserts)
			created, err := restarted.EnsureFlowInstance(ctx, req)
			if missing {
				if err == nil || !strings.Contains(err.Error(), "exact committed receiver configuration") || created || len(agents.upserts) != upserts || len(bus.addedPaths) != 0 {
					t.Fatalf("missing evidence became empty config: created=%v err=%v", created, err)
				}
				return
			}
			if err != nil || created {
				t.Fatalf("valid empty evidence: created=%v err=%v", created, err)
			}
			cfg, found := testFlowActivationAgentConfig(t, restarted, "reviewer", "review/one")
			if !found || string(cfg.ReceiverConfig) != "{}" {
				t.Fatalf("empty evidence changed: %s", cfg.ReceiverConfig)
			}
		})
	}
}
