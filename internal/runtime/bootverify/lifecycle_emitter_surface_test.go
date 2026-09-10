package bootverify_test

import (
	"context"
	"reflect"
	"sort"
	"strings"
	"testing"

	runtime "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/bootverify"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/scenarioderivation"
	"github.com/division-sh/swarm/internal/runtime/scenarioexecution"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestCompiledLifecycleEmitterStrictValidationSurface(t *testing.T) {
	for _, tc := range []struct {
		name, event string
		root        func(*testing.T) string
		blocked     bool
	}{
		{"gate_local", "work.completed", func(t *testing.T) string {
			return canonicalrouting.CopyLifecycleEmitter(t, canonicalrouting.LifecycleGateLocal)
		}, false},
		{"gate_dangling", "work.completed", func(t *testing.T) string {
			return canonicalrouting.CopyLifecycleEmitterStatic(t, canonicalrouting.LifecycleStaticGateDanglingStrict)
		}, true},
		{"loop_connected", "loop.escaped", func(t *testing.T) string {
			return canonicalrouting.CopyLifecycleEmitter(t, canonicalrouting.LifecycleLoopConnected)
		}, false},
		{"loop_dangling", "loop.escaped", func(t *testing.T) string {
			return canonicalrouting.CopyLifecycleEmitterStatic(t, canonicalrouting.LifecycleStaticLoopDangling)
		}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := lifecycleSurfaceSource(t, tc.root(t))
			result, err := runtime.ValidateWorkflowContractSurface(context.Background(), source, runtime.WorkflowContractValidationOptions{ExecutionPosture: executionposture.Live, FatalBootWarnings: true, StrictEmitSchemas: true})
			if !tc.blocked {
				if err != nil {
					t.Fatalf("valid lifecycle source rejected: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), "[BLOCKER] event_consumer_exists") || !strings.Contains(err.Error(), tc.event) {
				t.Fatalf("strict surface did not reject dangling lifecycle event: %v", err)
			}
			for _, finding := range result.BootReport.Findings {
				if finding.CheckID == "event_consumer_exists" && strings.Contains(finding.Location, tc.event) && finding.Severity != bootverify.SeveritySemanticDriftWarn {
					t.Fatalf("surface escalation changed diagnostic severity: %#v", finding)
				}
			}
		})
	}
}

func TestLifecycleProducerDoesNotGrantScenarioInputAuthority(t *testing.T) {
	for _, variant := range []canonicalrouting.LifecycleEmitterVariant{canonicalrouting.LifecycleGateLocal, canonicalrouting.LifecycleLoopConnected} {
		source := lifecycleSurfaceSource(t, canonicalrouting.CopyLifecycleEmitter(t, variant))
		bundle, _ := semanticview.Bundle(source)
		hash, err := runtimecontracts.BundleHash(bundle)
		if err != nil {
			t.Fatal(err)
		}
		fact, err := correlation.NewSourceArtifactFact(hash)
		if err != nil {
			t.Fatal(err)
		}
		// This source-only fixture has no overlay. Bind the scenario profile to
		// its actual bundle fact without treating the bundle ID as a digest.
		digest, err := canonicaljson.Hash(map[string]any{"source_bundle_hash": hash})
		if err != nil {
			t.Fatal(err)
		}
		identity, err := scenarioexecution.NewEffectiveSourceIdentity(fact, digest)
		if err != nil {
			t.Fatal(err)
		}
		plans, err := scenarioderivation.Compile(source, identity, scenarioderivation.Request{FlowID: ".", AllInputs: true})
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		for _, plan := range plans {
			got = append(got, plan.PinName)
		}
		want := []string{"work.requested"}
		event := "work.completed"
		if variant == canonicalrouting.LifecycleLoopConnected {
			want = []string{"work.requested", "loop.start", "loop.admit", "loop.repeat", "loop.close"}
			event = "loop.escaped"
		}
		sort.Strings(got)
		sort.Strings(want)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("scenario inputs = %q, want %q", got, want)
		}
		if _, err := scenarioderivation.Compile(source, identity, scenarioderivation.Request{FlowID: ".", Input: event}); err == nil || !strings.Contains(err.Error(), "has no public input matching") {
			t.Fatalf("producer-only scenario input admitted: %v", err)
		}
	}
}

func TestCompiledLifecycleEmitterInvalidPlansRejectedByValidationSurface(t *testing.T) {
	for _, tc := range []struct {
		name, check, detail string
		variant             canonicalrouting.LifecycleEmitterStaticVariant
	}{
		{"gate_unknown", "stage_gate_validation", "absent.event", canonicalrouting.LifecycleStaticGateUnknownEvent},
		{"gate_wrong_flow", "stage_gate_validation", "work.completed", canonicalrouting.LifecycleStaticGateWrongFlow},
		{"gate_bad_field", "stage_gate_validation", "unknown", canonicalrouting.LifecycleStaticGateBadField},
		{"gate_missing_field", "stage_gate_validation", "result", canonicalrouting.LifecycleStaticGateMissingField},
		{"loop_unknown", "loop_validation", "absent.event", canonicalrouting.LifecycleStaticLoopUnknownEvent},
		{"loop_wrong_flow", "loop_validation", "loop.escaped", canonicalrouting.LifecycleStaticLoopWrongFlow},
		{"loop_bad_field", "loop_validation", "unknown", canonicalrouting.LifecycleStaticLoopBadField},
		{"loop_missing_field", "loop_validation", "revision_id", canonicalrouting.LifecycleStaticLoopMissingField},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := lifecycleSurfaceSource(t, canonicalrouting.CopyLifecycleEmitterStatic(t, tc.variant))
			_, err := runtime.ValidateWorkflowContractSurface(context.Background(), source, runtime.WorkflowContractValidationOptions{ExecutionPosture: executionposture.Live, FatalBootWarnings: true, StrictEmitSchemas: true})
			if err == nil || !strings.Contains(err.Error(), tc.check) || !strings.Contains(err.Error(), tc.detail) {
				t.Fatalf("invalid lifecycle plan reached validation success: %v", err)
			}
		})
	}
}

func lifecycleSurfaceSource(t *testing.T, root string) semanticview.Source {
	t.Helper()
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	return semanticview.Wrap(bundle)
}
