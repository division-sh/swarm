package bootverify

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/providerconnectors"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestCompiledLifecycleEmitterGeneratedOutcomeDiagnostics(t *testing.T) {
	for _, nested := range []bool{false, true} {
		for _, rule := range []bool{false, true} {
			t.Run(fmt.Sprintf("nested_%v/rule_%v", nested, rule), func(t *testing.T) {
				root := canonicalrouting.CopyLifecycleEmitterActivityOutcomes(t, nested)
				if rule {
					root = canonicalrouting.CopyLifecycleEmitterRuleActivityOutcomes(t, nested)
				}
				repo := canonicalrouting.RepoRoot(t)
				bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
				if err != nil {
					t.Fatal(err)
				}
				source := semanticview.Wrap(bundle)
				report := Run(context.Background(), source, Options{})
				for _, check := range []string{"event_chain_integrity", "event_consumer_exists", "event_producer_exists", "transition_reference_validation", "condition_payload_alignment", "semantic_drift_dead_event_schema", "event_metadata_authority"} {
					if reportContains(report.Findings, check, "send") {
						t.Errorf("generated outcomes changed %s: %#v", check, report.Findings)
					}
				}
				prefix := ""
				if nested {
					prefix = "child/"
				}
				got := generatedActivityResultEventNamesLocal(source)
				want := map[string]struct{}{}
				for _, outcome := range []string{"succeeded", "failed", "revision_requested", "rejected"} {
					want[prefix+"send."+outcome] = struct{}{}
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("generated names = %v, want %v", got, want)
				}
			})
		}
	}
}

func TestCompiledLifecycleEmitterImportedApprovalReferences(t *testing.T) {
	repo := canonicalrouting.RepoRoot(t)
	root := canonicalrouting.CopyLifecycleEmitterImportedApprovalReferences(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOptions(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo), runtimecontracts.WorkflowContractLoadOptions{AdmitPackInventory: packadmission.AdmitInventory})
	if err != nil {
		t.Fatal(err)
	}
	projection, err := packadmission.FromBundle(bundle)
	if err != nil {
		t.Fatal(err)
	}
	source, err := providerconnectors.SourceWithConnectorPackImports(semanticview.Wrap(bundle), projection.ProviderConnectors)
	if err != nil {
		t.Fatal(err)
	}
	generated := source.ResolvedEventCatalog()
	for _, local := range []string{"telegram_send_message.succeeded", "telegram_send_message.failed", "telegram_send_message.revision_requested", "telegram_send_message.rejected"} {
		canonical := "telegram-chat/" + local
		if _, ok := generated[canonical]; !ok {
			t.Fatalf("actual connector has no generated owner for %s", canonical)
		}
		if _, ok := source.AuthoredResolvedEventCatalog()[canonical]; ok {
			t.Fatalf("approval result %s has a dummy authored schema", canonical)
		}
		entry, key, ok := source.ResolveFlowEventCatalogEntry("telegram-chat", local)
		if !ok {
			t.Errorf("local generated schema missing: %s resolves to %s; canonical lookup=%v", local, source.ResolveFlowEventReference("telegram-chat", local), semanticview.ResolveFlowEventProof(source, "telegram-chat", canonical).HasSchema)
		} else if key != canonical || !reflect.DeepEqual(entry, generated[canonical]) {
			t.Errorf("wrong generated schema owner for %s: key=%s", local, key)
		}
		if !flowEventExists(source, "telegram-chat", local) {
			t.Errorf("generated approval reference rejected: %s", local)
		}
		if flowEventExists(source, "telegram-ingress", local) || flowEventExists(source, ".", local) {
			t.Errorf("another flow acquired local generated reference %s", local)
		}
	}
	for _, unowned := range []string{"not_an_activity.revision_requested", "not_an_activity.rejected", "telegram_send_message.unknown_outcome"} {
		if flowEventExists(source, "telegram-chat", unowned) {
			t.Errorf("generated-looking spelling acquired schema authority: %s", unowned)
		}
	}
	c := checkerContext{source: source}
	if findings := c.transitionReferences(); reportContains(findings, "transition_reference_validation", "telegram_send_message") {
		t.Fatalf("generated approval references rejected: %#v", findings)
	}
}

func TestCompiledLifecycleEmitterDiagnostics(t *testing.T) {
	for _, tc := range []struct {
		name, event string
		variant     canonicalrouting.LifecycleEmitterStaticVariant
		want        map[string]int
	}{
		{"loop_local", "loop.escaped", canonicalrouting.LifecycleStaticLoopLocal, map[string]int{}},
		{"loop_dangling_live_schema", "loop.escaped", canonicalrouting.LifecycleStaticLoopDangling, map[string]int{"event_consumer_exists": 1}},
		{"absent_escape_emit", "loop.escaped", canonicalrouting.LifecycleStaticLoopNoEmit, map[string]int{"semantic_drift_dead_event_schema": 1}},
		{"shared_gate_dangling_live_schema", "work.completed", canonicalrouting.LifecycleStaticGateSharedDangling, map[string]int{"event_consumer_exists": 1}},
		{"mixed_connected_output", "work.completed", canonicalrouting.LifecycleStaticMixedOutput, map[string]int{}},
		{"mixed_disconnected_output", "work.completed", canonicalrouting.LifecycleStaticMixedDisconnected, map[string]int{"event_consumer_exists": 1}},
		{"foreign_subscribers_do_not_satisfy_root", "work.completed", canonicalrouting.LifecycleStaticForeignOnly, map[string]int{"event_consumer_exists": 1}},
		{"external_proof", "work.completed", canonicalrouting.LifecycleStaticGateExternalProof, map[string]int{}},
		{"two_loops_shared", "loop.escaped", canonicalrouting.LifecycleStaticTwoLoopsShared, map[string]int{}},
		{"gate_loop_shared", "loop.escaped", canonicalrouting.LifecycleStaticGateLoopShared, map[string]int{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := loadLifecycleStaticDiagnosticSource(t, tc.variant)
			report := Run(context.Background(), source, Options{})
			got := map[string]int{}
			for _, finding := range report.Findings {
				if finding.CheckID == "stage_gate_validation" || finding.CheckID == "loop_validation" || finding.CheckID == "event_metadata_authority" {
					t.Errorf("unexpected lifecycle/authority invalidity: %#v", finding)
				}
				if !strings.Contains(finding.Location, tc.event) {
					continue
				}
				switch finding.CheckID {
				case "event_producer_exists", "event_consumer_exists", "semantic_drift_dead_event_schema":
					got[finding.CheckID]++
					if finding.Severity != SeveritySemanticDriftWarn {
						t.Errorf("changed warning severity: %#v", finding)
					}
				}
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("event diagnostic counts = %#v, want %#v; report = %#v", got, tc.want, report.Findings)
			}
			if tc.variant == canonicalrouting.LifecycleStaticMixedDisconnected {
				if !reportContains(report.Findings, "pin_target_resolution", "work.completed") {
					t.Fatalf("disconnected mixed output accepted: %#v", report.Findings)
				}
			} else if tc.variant == canonicalrouting.LifecycleStaticMixedOutput && reportContains(report.Findings, "pin_target_resolution", "work.completed") {
				t.Fatalf("connected mixed output rejected: %#v", report.Findings)
			}
		})
	}
}

func TestCompiledLifecycleEmitterInvalidSourcePlans(t *testing.T) {
	for _, tc := range []struct {
		name, check, detail string
		variant             canonicalrouting.LifecycleEmitterStaticVariant
	}{
		{"gate_unknown", "stage_gate_validation", "absent.event", canonicalrouting.LifecycleStaticGateUnknownEvent},
		{"gate_bad_field", "stage_gate_validation", "unknown", canonicalrouting.LifecycleStaticGateBadField},
		{"gate_wrong_flow", "stage_gate_validation", "work.completed", canonicalrouting.LifecycleStaticGateWrongFlow},
		{"loop_unknown", "loop_validation", "absent.event", canonicalrouting.LifecycleStaticLoopUnknownEvent},
		{"loop_bad_field", "loop_validation", "unknown", canonicalrouting.LifecycleStaticLoopBadField},
		{"loop_wrong_flow", "loop_validation", "loop.escaped", canonicalrouting.LifecycleStaticLoopWrongFlow},
		{"gate_missing_field", "stage_gate_validation", "result", canonicalrouting.LifecycleStaticGateMissingField},
		{"loop_missing_field", "loop_validation", "revision_id", canonicalrouting.LifecycleStaticLoopMissingField},
		{"loop_local_node_without_operation", "loop_validation", "omits loop operation", canonicalrouting.LifecycleStaticLoopLocalNodeInvalid},
		{"reserved_platform_gate", "platform_namespace_violation", "platform.stage_timer", canonicalrouting.LifecycleStaticGateReserved},
		{"reserved_platform_escape", "platform_namespace_violation", "platform.stage_timer", canonicalrouting.LifecycleStaticLoopReserved},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := loadLifecycleStaticDiagnosticSource(t, tc.variant)
			report := Run(context.Background(), source, Options{})
			if !reportContains(report.HardInvalidities(), tc.check, tc.detail) {
				t.Fatalf("missing %s/%s rejection: %#v", tc.check, tc.detail, report.Findings)
			}
		})
	}
}

func TestCompiledLifecycleEmitterMalformedSourceRejected(t *testing.T) {
	for _, tc := range []struct {
		name, detail string
		variant      canonicalrouting.LifecycleEmitterStaticVariant
	}{
		{"loop_cap", "max_attempts must be a positive integer", canonicalrouting.LifecycleStaticLoopBadCap},
		{"gate_field", "unknown_gate_field", canonicalrouting.LifecycleStaticGateMalformed},
		{"loop_field", "unknown_loop_field", canonicalrouting.LifecycleStaticLoopMalformed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := canonicalrouting.RepoRoot(t)
			_, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, canonicalrouting.CopyLifecycleEmitterStatic(t, tc.variant), runtimecontracts.DefaultPlatformSpecFile(repo))
			if err == nil || !strings.Contains(err.Error(), tc.detail) {
				t.Fatalf("malformed source load = %v, want %q", err, tc.detail)
			}
		})
	}
}

func TestCompiledLifecycleEmitterHandlerCycleBoundary(t *testing.T) {
	for _, tc := range []struct {
		name        string
		variant     canonicalrouting.LifecycleEmitterStaticVariant
		cycle, self bool
	}{
		{"gate_loop_are_not_handlers", canonicalrouting.LifecycleStaticGateLoopShared, false, false},
		{"mixed_handler_cycle", canonicalrouting.LifecycleStaticHandlerCycle, true, false},
		{"mixed_handler_self_cycle", canonicalrouting.LifecycleStaticHandlerSelfCycle, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := loadLifecycleStaticDiagnosticSource(t, tc.variant)
			err := detectEventCyclesSemanticModel(source)
			if (err != nil) != tc.cycle {
				t.Fatalf("handler chain cycle = %v, want cycle=%v", err, tc.cycle)
			}
			if tc.cycle && (!strings.Contains(err.Error(), "work.requested") || !strings.Contains(err.Error(), "work.completed")) {
				t.Fatalf("wrong cycle: %v", err)
			}
			c := checkerContext{source: source}
			findings := c.eventCycleDetection()
			if reportContains(findings, "event_cycle_detection", "emits its own trigger event") != tc.self {
				t.Fatalf("self-cycle findings = %#v", findings)
			}
			if !tc.cycle && !tc.self && len(findings) != 0 {
				t.Fatalf("lifecycle invented handler cycle: %#v", findings)
			}
		})
	}
}

func TestCompiledLifecycleEmitterMetadataCoordinates(t *testing.T) {
	for _, tc := range []struct {
		name, role string
		coordinate canonicalrouting.LifecycleEmitterMetadataCoordinate
	}{
		{"stage", "review", canonicalrouting.LifecycleMetadataStage},
		{"verdict", "approve", canonicalrouting.LifecycleMetadataVerdict},
	} {
		for _, field := range []string{"source", "producer", "consumer"} {
			t.Run(tc.name+"/"+field, func(t *testing.T) {
				repo := canonicalrouting.RepoRoot(t)
				root := canonicalrouting.CopyLifecycleEmitterMetadataCoordinate(t, tc.coordinate, field)
				bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
				if err != nil {
					t.Fatal(err)
				}
				report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
				if !reportContains(report.HardInvalidities(), "event_metadata_authority", "swarm."+field) || !reportContains(report.HardInvalidities(), "event_metadata_authority", tc.role) {
					t.Fatalf("internal coordinate accepted: %#v", report.Findings)
				}
			})
		}
	}
}

func TestCompiledLifecycleEmitterCannotSatisfyNodeAssertion(t *testing.T) {
	source := loadLifecycleStaticDiagnosticSource(t, canonicalrouting.LifecycleStaticHandlerAssertion)
	report := Run(context.Background(), source, Options{})
	if !reportContains(report.Findings, "phantom_produces", "work.completed") {
		t.Fatalf("gate satisfied unrelated node produces assertion: %#v", report.Findings)
	}
}

func TestCompiledLifecycleEmitterTimerOtherProducer(t *testing.T) {
	self := loadLifecycleStaticDiagnosticSource(t, canonicalrouting.LifecycleStaticTimerSelfOnly)
	selfTimers := self.WorkflowTimers()
	if len(selfTimers) != 1 {
		t.Fatalf("self timers = %#v", selfTimers)
	}
	selfChecker := checkerContext{source: self}
	if selfChecker.timerTriggerEventProduced(selfTimers[0], semanticview.ResolveFlowEventProof(self, ".", "work.completed")) {
		t.Fatal("timer counted itself as another producer")
	}
	source := loadLifecycleStaticDiagnosticSource(t, canonicalrouting.LifecycleStaticTimerCollision)
	timers := source.WorkflowTimers()
	if len(timers) != 1 {
		t.Fatalf("timers = %#v", timers)
	}
	c := checkerContext{source: source}
	proof := semanticview.ResolveFlowEventProof(source, ".", "work.completed")
	if !c.timerTriggerEventProduced(timers[0], proof) {
		t.Fatal("gate omitted as non-self timer producer")
	}
	counts := map[semanticview.EventEndpointKind]int{}
	for _, endpoint := range semanticview.BuildAuthoredEventEndpointCensus(source).MatchingProducers(".", "work.completed") {
		counts[endpoint.Kind]++
	}
	if !reflect.DeepEqual(counts, map[semanticview.EventEndpointKind]int{semanticview.EventEndpointTimer: 1, semanticview.EventEndpointGateOutcome: 1}) {
		t.Fatalf("timer/gate sites = %#v", counts)
	}
}

func loadLifecycleStaticDiagnosticSource(t *testing.T, variant canonicalrouting.LifecycleEmitterStaticVariant) semanticview.Source {
	t.Helper()
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, canonicalrouting.CopyLifecycleEmitterStatic(t, variant), runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	return semanticview.Wrap(bundle)
}
