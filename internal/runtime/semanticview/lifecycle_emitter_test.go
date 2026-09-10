package semanticview

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/yamlsource"
)

func TestCompiledLifecycleEmitterCensus(t *testing.T) {
	for _, tc := range []struct {
		name    string
		variant canonicalrouting.LifecycleEmitterStaticVariant
		want    []string
	}{
		{"loop_escape", canonicalrouting.LifecycleStaticLoopLocal, []string{".|loop_escape|loops.revision.escape.emit|loop.escaped"}},
		{"absent_escape_emit", canonicalrouting.LifecycleStaticLoopNoEmit, nil},
		{"loop_scope_matrix", canonicalrouting.LifecycleStaticLoopScopeMatrix, []string{
			".|loop_escape|loops.revision.escape.emit|loop.escaped",
			"left|loop_escape|loops.revision.escape.emit|left/loop.escaped",
			"left/deep|loop_escape|loops.revision.escape.emit|left/deep/loop.escaped",
			"right|loop_escape|loops.revision.escape.emit|right/loop.escaped",
		}},
		{"gate_loop_shared", canonicalrouting.LifecycleStaticGateLoopShared, []string{
			".|loop_escape|loops.revision.escape.emit|loop.escaped",
			".|gate_outcome|stages.waiting.gate.outcomes.approve.emit|loop.escaped",
		}},
		{"two_loops_shared", canonicalrouting.LifecycleStaticTwoLoopsShared, []string{
			".|loop_escape|loops.revision.escape.emit|loop.escaped",
			".|loop_escape|loops.second.escape.emit|loop.escaped",
		}},
		{"shared_event", canonicalrouting.LifecycleStaticGateTwoGates, []string{
			".|gate_outcome|stages.review.gate.outcomes.approve.emit|work.completed",
			".|gate_outcome|stages.review.gate.outcomes.reject.emit|work.completed",
			".|gate_outcome|stages.second_review.gate.outcomes.approve.emit|work.completed",
			".|gate_outcome|stages.second_review.gate.outcomes.reject.emit|work.completed",
		}},
		{"scope_matrix", canonicalrouting.LifecycleStaticScopeMatrix, []string{
			".|gate_outcome|stages.review.gate.outcomes.approve.emit|work.completed",
			"left|gate_outcome|stages.review.gate.outcomes.approve.emit|left/work.completed",
			"left/deep|gate_outcome|stages.review.gate.outcomes.approve.emit|left/deep/work.completed",
			"right|gate_outcome|stages.review.gate.outcomes.approve.emit|right/work.completed",
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := loadLifecycleStaticSource(t, canonicalrouting.CopyLifecycleEmitterStatic(t, tc.variant))
			census := BuildAuthoredEventEndpointCensus(source)
			var got []string
			ids := map[string]bool{}
			for _, endpoint := range census.Producers() {
				if endpoint.Kind != EventEndpointGateOutcome && endpoint.Kind != EventEndpointLoopEscape {
					continue
				}
				got = append(got, strings.Join([]string{endpoint.FlowID, string(endpoint.Kind), endpoint.Site, endpoint.Event.Canonical}, "|"))
				if ids[endpoint.ID] {
					t.Fatalf("duplicate site identity: %#v", endpoint)
				}
				ids[endpoint.ID] = true
				if endpoint.Node.Valid() || endpoint.NodeID != "" || endpoint.AgentID != "" || endpoint.Role != "" || endpoint.TimerID != "" || endpoint.PinName != "" || endpoint.HandlerEvent != "" {
					t.Fatalf("lifecycle site impersonates actor/input/handler: %#v", endpoint)
				}
				if !endpoint.Event.HasSchema || endpoint.Event.Authored != endpoint.Event.Local || endpoint.SourceLocation != endpoint.Site || endpoint.SourceLine <= 0 {
					t.Fatalf("missing declaration proof: %#v", endpoint)
				}
				wantFile := "schema.yaml"
				if endpoint.FlowID != "." {
					wantFile = endpoint.FlowID + "/schema.yaml"
				}
				if endpoint.SourceFile != wantFile {
					t.Fatalf("source = %q, want %q", endpoint.SourceFile, wantFile)
				}
				if endpoint.Kind == EventEndpointGateOutcome && (endpoint.StageID == "" || endpoint.DecisionID == "" || endpoint.Verdict == "" || endpoint.LoopID != "") {
					t.Fatalf("gate coordinates = %#v", endpoint)
				}
				if endpoint.Kind == EventEndpointLoopEscape && (endpoint.LoopID == "" || endpoint.StageID != "" || endpoint.DecisionID != "" || endpoint.Verdict != "") {
					t.Fatalf("loop coordinates = %#v", endpoint)
				}
				indexed, ok := census.Endpoint(endpoint.ID)
				if !ok || !reflect.DeepEqual(endpoint, indexed) {
					t.Fatalf("index lost site: %#v", indexed)
				}
			}
			sort.Strings(got)
			sort.Strings(tc.want)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("sites = %q, want %q", got, tc.want)
			}
			if !reflect.DeepEqual(census.Producers(), BuildAuthoredEventEndpointCensus(source).Producers()) {
				t.Fatal("repeated build changed ordering/identity")
			}
		})
	}
}

func TestCompiledLifecycleEmitterProvenanceAndIsolation(t *testing.T) {
	root := canonicalrouting.CopyLifecycleEmitter(t, canonicalrouting.LifecycleGateSharedEvent)
	source := loadLifecycleStaticSource(t, root)
	census := BuildAuthoredEventEndpointCensus(source)
	want := census.Producers()
	parses := yamlsource.DefaultStats().ParseCount
	canonicalrouting.RewriteLifecycleEmitterAmbientSource(t, root)
	for _, endpoint := range want {
		if endpoint.Kind != EventEndpointGateOutcome {
			continue
		}
		if endpoint.Verdict == "approve" && endpoint.SourceLine != 11 {
			t.Fatalf("approve coordinate = %#v", endpoint)
		}
		if endpoint.Verdict == "reject" && endpoint.SourceLine != 16 {
			t.Fatalf("reject coordinate = %#v", endpoint)
		}
		returned, _ := census.Endpoint(endpoint.ID)
		returned.Verdict, returned.SourceFile, returned.Event.Canonical = "hostile", "ambient.yaml", "foreign/event"
		matched := census.MatchingProducers(".", "work.completed")
		matched[0].StageID = "hostile"
		matches := census.ResolveTypedPubSubConsumerMatches(endpoint)
		if len(matches) != 1 {
			t.Fatalf("matches = %#v", matches)
		}
		matches[0].Producer.DecisionID = "hostile"
		matches[0].Consumer.NodeID = "hostile"
	}
	returned := census.Producers()
	returned[0].FlowID = "hostile"
	if !reflect.DeepEqual(census.Producers(), want) || !reflect.DeepEqual(BuildAuthoredEventEndpointCensus(source).Producers(), want) {
		t.Fatal("returned value or ambient YAML mutated admitted census")
	}
	if got := yamlsource.DefaultStats().ParseCount; got != parses {
		t.Fatalf("census reparsed YAML: %d -> %d", parses, got)
	}
	// Reinsert the admitted outcome map in reverse order; presentation ordering
	// and site identity must not depend on Go map iteration.
	bundle, _ := Bundle(source)
	for key, gate := range bundle.Semantics.Gates {
		outcomes := make(map[string]runtimecontracts.WorkflowGateOutcomePlan, len(gate.Outcomes))
		keys := sortedMapKeys(gate.Outcomes)
		for i := len(keys) - 1; i >= 0; i-- {
			outcomes[keys[i]] = gate.Outcomes[keys[i]]
		}
		gate.Outcomes = outcomes
		bundle.Semantics.Gates[key] = gate
	}
	if !reflect.DeepEqual(BuildAuthoredEventEndpointCensus(source).Producers(), want) {
		t.Fatal("reordered outcome map changed census")
	}
}

func TestCompiledLifecycleEmitterSchemaReadbackIsolation(t *testing.T) {
	source := loadLifecycleStaticSource(t, canonicalrouting.CopyLifecycleEmitter(t, canonicalrouting.LifecycleGateLocal))
	census := BuildAuthoredEventEndpointCensus(source)
	returned := census.MatchingProducers(".", "work.completed")
	if len(returned) != 1 {
		t.Fatalf("gate sites = %#v", returned)
	}
	id := returned[0].ID
	field, exists := returned[0].Event.Entry.Payload.Properties["result"]
	if !exists {
		t.Fatal("gate result payload missing")
	}
	returned[0].Event.Entry.Payload.Properties["result"] = runtimecontracts.EventFieldSpec{Type: "boolean"}
	indexed, ok := census.Endpoint(id)
	if !ok || !reflect.DeepEqual(indexed.Event.Entry.Payload.Properties["result"], field) {
		t.Fatal("mutating returned lifecycle schema changed the census index")
	}
	if fresh := BuildAuthoredEventEndpointCensus(source).MatchingProducers(".", "work.completed"); len(fresh) != 1 || !reflect.DeepEqual(fresh[0].Event.Entry.Payload.Properties["result"], field) {
		t.Fatal("mutating returned lifecycle schema changed admitted source")
	}
}

func TestCompiledLifecycleEmitterAllCensusReadbacksIsolateSchemas(t *testing.T) {
	source := loadLifecycleStaticSource(t, canonicalrouting.CopyLifecycleEmitter(t, canonicalrouting.LifecycleLoopConnected))
	census := BuildAuthoredEventEndpointCensus(source)
	snapshot := func(c AuthoredEventEndpointCensus) []AuthoredEventEndpoint {
		out := append(c.Producers(), c.Consumers()...)
		out = append(out, c.InputPins()...)
		return append(out, c.OutputPins()...)
	}
	want := snapshot(census)
	wantRelations := census.ResolveTypedPubSubRelations()
	for _, getter := range []string{"slices", "index", "matching", "relations", "matches"} {
		t.Run(getter, func(t *testing.T) {
			var proofs []FlowEventProof
			add := func(endpoints []AuthoredEventEndpoint) {
				for _, endpoint := range endpoints {
					proofs = append(proofs, endpoint.Event)
				}
			}
			addMatches := func(matches []TypedPubSubConsumerMatch) {
				for _, match := range matches {
					proofs = append(proofs, match.Producer.Event, match.Consumer.Event, match.Event)
				}
			}
			switch getter {
			case "slices":
				add(snapshot(census))
			case "index":
				for _, endpoint := range want {
					indexed, ok := census.Endpoint(endpoint.ID)
					if !ok {
						t.Fatalf("missing endpoint %s", endpoint.ID)
					}
					add([]AuthoredEventEndpoint{indexed})
				}
			case "matching":
				for _, endpoint := range want {
					add(census.MatchingProducers(endpoint.FlowID, endpoint.Event.Canonical))
					add(census.MatchingConsumers(endpoint.FlowID, endpoint.Event.Canonical))
					add(census.MatchingOutputPins(endpoint.FlowID, endpoint.Event.Canonical))
				}
			case "relations":
				addMatches(census.ResolveTypedPubSubRelations().Matches)
			case "matches":
				for _, producer := range append(census.Producers(), census.InputPins()...) {
					addMatches(census.ResolveTypedPubSubConsumerMatches(producer))
				}
			}
			if len(proofs) == 0 {
				t.Fatal("empty readback did not exercise schema isolation")
			}
			for _, proof := range proofs {
				for key := range proof.Entry.Payload.Properties {
					delete(proof.Entry.Payload.Properties, key)
				}
				if len(proof.Entry.Payload.Required) > 0 {
					proof.Entry.Payload.Required[0] = "mutated"
				}
				if len(proof.Entry.Swarm.Producer) > 0 {
					proof.Entry.Swarm.Producer[0] = "mutated"
				}
			}
			if !reflect.DeepEqual(snapshot(census), want) || !reflect.DeepEqual(census.ResolveTypedPubSubRelations(), wantRelations) {
				t.Fatal("returned schema mutation changed census or relations")
			}
			if !reflect.DeepEqual(snapshot(BuildAuthoredEventEndpointCensus(source)), want) {
				t.Fatal("returned schema mutation changed admitted source")
			}
		})
	}
}

func TestLifecycleProducerDoesNotGrantIngressAuthority(t *testing.T) {
	for _, variant := range []canonicalrouting.LifecycleEmitterVariant{canonicalrouting.LifecycleGateLocal, canonicalrouting.LifecycleLoopConnected} {
		t.Run(fmt.Sprint(variant), func(t *testing.T) {
			source := loadLifecycleStaticSource(t, canonicalrouting.CopyLifecycleEmitter(t, variant))
			census := BuildAuthoredEventEndpointCensus(source)
			for _, endpoint := range census.Producers() {
				if endpoint.Kind != EventEndpointGateOutcome && endpoint.Kind != EventEndpointLoopEscape {
					continue
				}
				if result := census.ResolveDeclaredInputEndpoint(endpoint.FlowID, endpoint.Event.Authored); result.Status != EndpointAssociationNotFound {
					t.Fatalf("producer acquired input authority: %#v", result)
				}
				resolution := ResolveNonConnectFlowInputProducer(source, endpoint.FlowID, endpoint.Event.Authored)
				if len(resolution.Evidence) != 1 || resolution.Evidence[0].Kind != runtimecontracts.FlowInputProducerInvalidContext {
					t.Fatalf("non-input accepted: %#v", resolution)
				}
				resolution = ResolveNonConnectFlowInputProducerWithOptions(source, endpoint.FlowID, endpoint.Event.Authored, runtimecontracts.FlowInputProducerResolutionOptions{AllowNonInputEvent: true})
				if len(resolution.Evidence) != 1 || resolution.Evidence[0].Kind != runtimecontracts.FlowInputProducerInternalTopology || resolution.Evidence[0].FlowID != endpoint.FlowID {
					t.Fatalf("internal producer became ingress evidence: %#v", resolution)
				}
				want := "flow . stage review gate review_decision verdict approve"
				if endpoint.Kind == EventEndpointLoopEscape {
					want = "flow . loop revision escape"
				}
				if resolution.Evidence[0].Detail != want {
					t.Fatalf("detail = %q, want %q", resolution.Evidence[0].Detail, want)
				}
			}
		})
	}
}

func loadLifecycleStaticSource(t *testing.T, root string) Source {
	t.Helper()
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	return Wrap(bundle)
}

func TestCompiledLifecycleEmitterExistingFamilies(t *testing.T) {
	for _, tc := range []struct {
		name string
		root func(*testing.T) string
		want []string
	}{
		{"handler_families", func(t *testing.T) string { return canonicalrouting.CopyLifecycleEmitterHandlerFamilies(t) }, []string{
			".|gate_outcome||stages.review.gate.outcomes.approve.emit|work.completed",
			"families|node_handler|worker|handler.emit|families/direct",
			"families|node_handler|worker|handler.guard.on_fail.escalate|families/escalated",
			"families|node_handler|router|handler.rules[0].emit|families/routed",
			"families|node_handler|router|handler.on_success.emit|families/audit",
			"families|node_handler|template|handler.rules[0].emit_template|families/specialized",
			"families|node_handler|template|handler.rules[1].emit_template|families/specialized",
			"families|node_handler|dispatcher|handler.fan_out.emit|families/item",
			"families|node_handler|rule-dispatcher|handler.rules[0].fan_out.emit|families/item",
			"families|node_handler|completion|handler.on_complete[0].emit|families/direct",
			"families|node_handler|completion-dispatcher|handler.on_complete[0].fan_out.emit|families/item",
			"families|node_handler|committer|handler.action.success|families/commit.ok",
			"families|node_handler|committer|handler.action.failure|families/commit.failed",
		}},
		{"generated_outcomes_root", func(t *testing.T) string { return canonicalrouting.CopyLifecycleEmitterActivityOutcomes(t, false) }, []string{
			".|node_generated|activity-node||send.succeeded",
			".|node_generated|activity-node||send.failed",
			".|node_generated|activity-node||send.revision_requested",
			".|node_generated|activity-node||send.rejected",
		}},
		{"generated_outcomes_nested", func(t *testing.T) string { return canonicalrouting.CopyLifecycleEmitterActivityOutcomes(t, true) }, []string{
			"child|node_generated|activity-node||child/send.succeeded",
			"child|node_generated|activity-node||child/send.failed",
			"child|node_generated|activity-node||child/send.revision_requested",
			"child|node_generated|activity-node||child/send.rejected",
		}},
		{"timers_join_auto_emit", func(t *testing.T) string { return canonicalrouting.CopyLifecycleEmitterExistingLifecycleFamilies(t) }, []string{
			".|node_handler|join-node|handler.join.on_complete.emit|join.completed",
			".|node_handler|join-node|handler.join.timeout.emit|join.expired",
			".|timer|||reminder",
			".|timer|||expired",
			".|platform|||platform.stage_timer",
			".|auto_emit_on_create|||created",
			"child|auto_emit_on_create|||child/created",
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := loadLifecycleStaticSource(t, tc.root(t))
			var got []string
			for _, endpoint := range BuildAuthoredEventEndpointCensus(source).Producers() {
				if endpoint.Kind == EventEndpointExternal {
					continue
				}
				got = append(got, strings.Join([]string{endpoint.FlowID, string(endpoint.Kind), endpoint.NodeID, endpoint.Site, endpoint.Event.Canonical}, "|"))
				if !endpoint.Event.HasSchema && endpoint.Kind != EventEndpointPlatform {
					t.Fatalf("missing schema: %#v", endpoint)
				}
				if endpoint.Kind == EventEndpointNodeGenerated && (!endpoint.Node.Valid() || endpoint.FlowID != endpoint.Node.FlowPath()) {
					t.Fatalf("generated outcome lost exact node: %#v", endpoint)
				}
			}
			sort.Strings(got)
			sort.Strings(tc.want)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("family census = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestCompiledLifecycleEmitterGeneratedOutcomeOwnerMatrix(t *testing.T) {
	for _, nested := range []bool{false, true} {
		for _, rule := range []bool{false, true} {
			t.Run(fmt.Sprintf("nested_%v/rule_%v", nested, rule), func(t *testing.T) {
				root := canonicalrouting.CopyLifecycleEmitterActivityOutcomes(t, nested)
				if rule {
					root = canonicalrouting.CopyLifecycleEmitterRuleActivityOutcomes(t, nested)
				}
				source := loadLifecycleStaticSource(t, root)
				flow, prefix := ".", ""
				if nested {
					flow, prefix = "child", "child/"
				}
				bundle, _ := Bundle(source)
				sites := bundle.ActivitySites()
				if len(sites) != 1 || sites[0].Node.FlowPath() != flow || sites[0].Spec.ID != "send" || sites[0].Spec.Approval == nil {
					t.Fatalf("compiled activity sites = %#v", sites)
				}
				if rule && (sites[0].RuleID != "send_rule" || sites[0].Source != "handler.rules[0].activity") {
					t.Fatalf("lost selected rule site: %#v", sites[0])
				}
				if !rule && sites[0].Source != "handler.activity" {
					t.Fatalf("invented rule site: %#v", sites[0])
				}
				census := BuildAuthoredEventEndpointCensus(source)
				var names []string
				for _, endpoint := range census.Producers() {
					if endpoint.Kind == EventEndpointExternal {
						continue
					}
					if endpoint.Kind != EventEndpointNodeGenerated || endpoint.FlowID != flow || endpoint.Node != sites[0].Node || !endpoint.Event.HasSchema {
						t.Fatalf("generated producer ownership = %#v", endpoint)
					}
					names = append(names, endpoint.Event.Canonical)
				}
				want := []string{prefix + "send.succeeded", prefix + "send.failed", prefix + "send.revision_requested", prefix + "send.rejected"}
				sort.Strings(names)
				sort.Strings(want)
				if !reflect.DeepEqual(names, want) {
					t.Fatalf("generated family = %q, want %q", names, want)
				}
				for suffix, field := range map[string]string{"succeeded": "result", "failed": "failure", "revision_requested": "feedback", "rejected": "reason"} {
					proof := ResolveFlowEventProof(source, flow, "send."+suffix)
					if _, found := proof.Entry.Payload.Properties[field]; !found {
						t.Fatalf("generated %s schema missing %s: %#v", suffix, field, proof)
					}
					if matches := census.MatchingConsumers(flow, proof.EventKey()); len(matches) != 1 || matches[0].Kind != EventEndpointNodeHandler {
						t.Fatalf("generated %s has no exact consumer: %#v", suffix, matches)
					}
				}
			})
		}
	}
}

func TestCompiledLifecycleEmitterHumanTaskPlatformBoundary(t *testing.T) {
	for _, nested := range []bool{false, true} {
		t.Run(fmt.Sprintf("nested_%v", nested), func(t *testing.T) {
			source := loadLifecycleStaticSource(t, canonicalrouting.CopyLifecycleEmitterHumanTaskObserver(t, nested))
			flow := "."
			if nested {
				flow = "child"
			}
			bundle, _ := Bundle(source)
			if len(bundle.ActivitySites()) != 0 {
				t.Fatal("human task became an authored HTTP activity")
			}
			census := BuildAuthoredEventEndpointCensus(source)
			for _, event := range []string{"human_task.approved", "human_task.rejected", "human_task.deferred", "human_task.expired"} {
				proof := ResolveFlowEventProof(source, flow, event)
				if !proof.HasSchema || proof.Canonical != event || !runtimecontracts.PlatformEventCatalogContains(source.PlatformSpec(), event) {
					t.Fatalf("human-task platform schema lost: %#v", proof)
				}
				if producers := census.MatchingProducers(flow, event); len(producers) != 0 {
					t.Fatalf("invented authored human-task producer: %#v", producers)
				}
				consumers := census.MatchingConsumers(flow, event)
				kinds := map[EventEndpointKind]int{}
				for _, consumer := range consumers {
					if consumer.FlowID != flow {
						t.Fatalf("human-task receiver crossed flow: %s", consumer.FlowID)
					}
					kinds[consumer.Kind]++
				}
				if !reflect.DeepEqual(kinds, map[EventEndpointKind]int{EventEndpointAgent: 1, EventEndpointRequiredAgentRole: 1}) {
					t.Fatalf("human-task receiver kinds = %v", kinds)
				}
				resolution := ResolveNonConnectFlowInputProducerWithOptions(source, flow, event, runtimecontracts.FlowInputProducerResolutionOptions{AllowNonInputEvent: true})
				if len(resolution.Evidence) != 1 || resolution.Evidence[0].Kind != runtimecontracts.FlowInputProducerPlatformSource {
					t.Fatalf("human-task authority = %#v", resolution)
				}
			}
		})
	}
}
