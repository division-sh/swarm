package bootverify

import (
	"context"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/routingtopology"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestCompiledLifecycleEmitterSourceMatrix(t *testing.T) {
	for _, tc := range []struct {
		name                         string
		variant                      canonicalrouting.LifecycleEmitterVariant
		kind                         semanticview.EventEndpointKind
		count                        int
		dangling, dead, disconnected bool
	}{
		{"gate_local", canonicalrouting.LifecycleGateLocal, semanticview.EventEndpointGateOutcome, 1, false, false, false},
		{"gate_dangling", canonicalrouting.LifecycleGateDangling, semanticview.EventEndpointGateOutcome, 1, true, false, false},
		{"gate_shared", canonicalrouting.LifecycleGateSharedEvent, semanticview.EventEndpointGateOutcome, 2, false, false, false},
		{"gate_no_emit", canonicalrouting.LifecycleGateNoEmit, semanticview.EventEndpointGateOutcome, 0, false, true, false},
		{"gate_output_disconnected", canonicalrouting.LifecycleGateOutputDisconnected, semanticview.EventEndpointGateOutcome, 1, true, false, true},
		{"gate_nested", canonicalrouting.LifecycleGateNested, semanticview.EventEndpointGateOutcome, 1, false, false, false},
		{"loop_connected", canonicalrouting.LifecycleLoopConnected, semanticview.EventEndpointLoopEscape, 1, false, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := canonicalrouting.CopyLifecycleEmitter(t, tc.variant)
			repo, err := filepath.Abs("../../..")
			if err != nil {
				t.Fatal(err)
			}
			bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
			if err != nil {
				t.Fatal(err)
			}
			source := semanticview.Wrap(bundle)
			census := semanticview.BuildAuthoredEventEndpointCensus(source)
			var endpoints []semanticview.AuthoredEventEndpoint
			for _, endpoint := range census.Producers() {
				if endpoint.Kind != tc.kind {
					continue
				}
				endpoints = append(endpoints, endpoint)
				if endpoint.Node.Valid() || endpoint.NodeID != "" || endpoint.AgentID != "" || endpoint.TimerID != "" {
					t.Fatalf("fabricated actor: %#v", endpoint)
				}
				if endpoint.SourceFile == "" || endpoint.SourceLine == 0 || endpoint.Site == "" || !endpoint.Event.HasSchema {
					t.Fatalf("missing admitted evidence: %#v", endpoint)
				}
				if tc.variant == canonicalrouting.LifecycleGateNested && endpoint.FlowID != "outer/inner" {
					t.Fatalf("wrong scope: %#v", endpoint)
				}
			}
			if len(endpoints) != tc.count {
				t.Fatalf("lifecycle endpoints = %#v", endpoints)
			}
			if tc.count == 2 && endpoints[0].ID == endpoints[1].ID {
				t.Fatal("shared event collapsed verdicts")
			}
			if !reflect.DeepEqual(census.Producers(), semanticview.BuildAuthoredEventEndpointCensus(source).Producers()) {
				t.Fatal("nondeterministic census")
			}
			topology := routingtopology.Build(source)
			for _, endpoint := range endpoints {
				matched := false
				for _, edge := range topology.Edges {
					if edge.Producer.ID == endpoint.ID {
						matched = true
					}
				}
				if matched == tc.dangling {
					t.Fatalf("edge presence=%v, dangling=%v: %#v", matched, tc.dangling, topology)
				}
			}
			report := Run(context.Background(), source, Options{})
			seen := map[string]bool{}
			for _, finding := range report.Findings {
				if strings.Contains(finding.Location, "work.completed") || strings.Contains(finding.Location, "loop.escaped") || finding.CheckID == "pin_target_resolution" {
					seen[finding.CheckID] = true
				}
				if finding.CheckID == "stage_gate_validation" || finding.CheckID == "loop_validation" {
					t.Errorf("invalid lifecycle fixture: %#v", finding)
				}
			}
			if seen["event_producer_exists"] || seen["event_consumer_exists"] != tc.dangling || seen["semantic_drift_dead_event_schema"] != tc.dead || seen["pin_target_resolution"] != tc.disconnected {
				t.Fatalf("diagnostics = %#v, report=%#v", seen, report.Findings)
			}
		})
	}
}

func TestCompiledLifecycleEmitterReservedClaims(t *testing.T) {
	for _, variant := range []canonicalrouting.LifecycleEmitterVariant{canonicalrouting.LifecycleGateLocal, canonicalrouting.LifecycleLoopConnected} {
		source := semanticview.Wrap(loadLifecycleEmitterBundle(t, variant))
		for _, endpoint := range semanticview.BuildAuthoredEventEndpointCensus(source).Producers() {
			if endpoint.Kind != semanticview.EventEndpointGateOutcome && endpoint.Kind != semanticview.EventEndpointLoopEscape {
				continue
			}
			t.Run(string(endpoint.Kind), func(t *testing.T) {
				for _, name := range []string{"platform.unregistered_claim", runtimecontracts.WorkflowStageTimerInternalEvent} {
					endpoint.Event.Authored = name
					if findings := platformProducerEndpointFindings(source, endpoint); len(findings) == 0 {
						t.Fatalf("reserved claim silently accepted: %#v", endpoint)
					}
				}
				endpoint.Kind = semanticview.EventEndpointKind("future_producer")
				if findings := platformProducerEndpointFindings(source, endpoint); len(findings) == 0 {
					t.Fatal("new kind bypasses reserved claim validation")
				}
				endpoint.Kind = semanticview.EventEndpointPlatform
				if findings := platformProducerEndpointFindings(source, endpoint); len(findings) != 0 {
					t.Fatalf("genuine platform producer rejected: %#v", findings)
				}
			})
		}
	}
}

func TestCompiledLifecycleEmitterMetadataRoles(t *testing.T) {
	for _, tc := range []struct {
		name, event, role string
		variant           canonicalrouting.LifecycleEmitterVariant
	}{
		{"gate_decision", "work.completed", "review_decision", canonicalrouting.LifecycleGateLocal},
		{"gate_site", "work.completed", "stages.review.gate.outcomes.approve.emit", canonicalrouting.LifecycleGateLocal},
		{"loop_id", "loop.escaped", "revision", canonicalrouting.LifecycleLoopConnected},
		{"loop_site", "loop.escaped", "loops.revision.escape.emit", canonicalrouting.LifecycleLoopConnected},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, field := range []string{"producer", "source", "consumer"} {
				t.Run(field, func(t *testing.T) {
					root := canonicalrouting.CopyLifecycleEmitterMetadata(t, tc.variant, field, strings.Contains(tc.name, "site"))
					repo := canonicalrouting.RepoRoot(t)
					bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
					if err != nil {
						t.Fatal(err)
					}
					report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
					if !reportContains(report.HardInvalidities(), "event_metadata_authority", tc.role) {
						t.Fatalf("internal %s role not rejected: %#v", field, report.Findings)
					}
				})
			}
		})
	}
}

func loadLifecycleEmitterBundle(t *testing.T, variant canonicalrouting.LifecycleEmitterVariant) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, canonicalrouting.CopyLifecycleEmitter(t, variant), runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	return bundle
}
