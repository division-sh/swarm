package bootverify_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/authoringview"
	"github.com/division-sh/swarm/internal/runtime/bootverify"
	contracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"gopkg.in/yaml.v3"
)

func TestCompiledTransitionSourceAuthoredExactRelation(t *testing.T) {
	for _, family := range []string{"ordinary", "loop", "gate"} {
		for _, reordered := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/reordered_%t", family, reordered), func(t *testing.T) {
				bundle := loadTransitionRelationFixture(t, family, reordered)
				source := semanticview.Wrap(bundle)
				view, err := authoringview.Build(context.Background(), source, authoringview.BuildOptions{IncludeStageGraph: true})
				if err != nil {
					t.Fatal(err)
				}
				if len(view.StageGraphs) != 3 || len(bundle.Semantics.StageTopologies) != 3 {
					t.Fatalf("unexpected flow inventory: runtime=%d authoring=%d", len(bundle.Semantics.StageTopologies), len(view.StageGraphs))
				}
				for _, flow := range []string{"right", ".", "left"} {
					t.Run(flow, func(t *testing.T) {
						want := authoredTransitionRelation(t, family, flow)
						graph, ok := semanticview.WorkflowStageTopology(source, flow)
						if !ok || graph.FlowID != flow {
							t.Fatalf("missing exact flow %q: %+v", flow, graph)
						}
						wantStages := authoredTransitionStages(family)
						var stages, terminal []string
						initial := ""
						for _, stage := range wantStages {
							stages = append(stages, stage.ID)
							if stage.Initial {
								initial = stage.ID
							}
							if stage.Terminal {
								terminal = append(terminal, stage.ID)
							}
						}
						requireTransitionRelation(t, "declared stages", graph.Stages, stages)
						requireTransitionRelation(t, "terminal stages", graph.TerminalStages, terminal)
						if graph.InitialStage != initial {
							t.Fatalf("initial = %q, want %q", graph.InitialStage, initial)
						}
						requireTransitionRelation(t, "runtime", graph.Edges, want)
						var projected *authoringview.StageGraphView
						for i := range view.StageGraphs {
							if view.StageGraphs[i].FlowPath == flow {
								projected = &view.StageGraphs[i]
							}
						}
						if projected == nil {
							t.Fatalf("authoring graph omitted flow %q", flow)
						}
						label := flow
						if flow == "." {
							label = "root"
						}
						if projected.FlowID != label {
							t.Fatalf("authoring flow label = %q, want %q", projected.FlowID, label)
						}
						requireTransitionRelation(t, "authoring stages", projected.Nodes, wantStages)
						wantView := authoredTransitionView(want)
						requireTransitionRelation(t, "authoring", projected.Edges, wantView)
						// Equal endpoint pairs remain a multiset: two rules/verdicts
						// cannot collapse merely because their public rows coincide.
						if len(graph.Edges) > 1 && reflect.DeepEqual(transitionRelationBag(graph.Edges[1:]), transitionRelationBag(want)) {
							t.Fatal("oracle ignored a missing carrier")
						}
						for _, coordinate := range []string{"from", "to", "carrier", "owner", "event", "rule"} {
							mutated := slices.Clone(graph.Edges)
							switch coordinate {
							case "from":
								mutated[0].From = "foreign"
							case "to":
								mutated[0].To = "foreign"
							case "carrier":
								mutated[0].Source = "handler.rules"
								mutated[0].AdvanceCarrier = "foreign"
							case "owner":
								mutated[0].Node = identitytest.FlowNode(t, "foreign", "worker")
							case "event":
								mutated[0].HandlerEvent = "foreign"
							case "rule":
								mutated[0].RuleRef = transitionRelationRule(t, flow, "foreign", "rules", 0)
							}
							if reflect.DeepEqual(transitionRelationBag(mutated), transitionRelationBag(want)) {
								t.Fatalf("oracle accepted mutated %s", coordinate)
							}
						}
						graph.Edges[0].To = "foreign"
						projected.Edges[0].From[0] = "foreign"
						if reflect.DeepEqual(transitionRelationBag(projected.Edges), transitionRelationBag(wantView)) {
							t.Fatal("public relation oracle accepted mutated source")
						}
						after, _ := semanticview.WorkflowStageTopology(source, flow)
						requireTransitionRelation(t, "projection mutation isolation", after.Edges, want)
					})
				}
				rebuilt, err := authoringview.Build(context.Background(), source, authoringview.BuildOptions{IncludeStageGraph: true})
				if err != nil {
					t.Fatal(err)
				}
				for _, graph := range rebuilt.StageGraphs {
					requireTransitionRelation(t, "authoring mutation isolation", graph.Edges, authoredTransitionView(authoredTransitionRelation(t, family, graph.FlowPath)))
				}
				// Consumer diagnostics supplement, rather than define, the oracle.
				for _, finding := range bootverify.Run(context.Background(), source, bootverify.Options{}).Findings {
					switch finding.CheckID {
					case "transition_reference_validation", "transition_ownership_validation", "handler_field_compliance", "semantic_drift_unreachable_state", "join_validation", "timer_validation", "loop_validation", "stage_gate_validation":
						if strings.EqualFold(finding.Severity, "error") {
							t.Errorf("source-loaded consumer rejected authored relation: %+v", finding)
						}
					}
				}
			})
		}
	}
}

func authoredTransitionStages(family string) []authoringview.StageGraphNodeView {
	switch family {
	case "ordinary":
		return []authoringview.StageGraphNodeView{{ID: "ready", Initial: true}, {ID: "working"}, {ID: "awaiting"}, {ID: "done", Terminal: true}}
	case "loop":
		return []authoringview.StageGraphNodeView{{ID: "waiting", Initial: true}, {ID: "drafting"}, {ID: "review"}, {ID: "done", Terminal: true}, {ID: "escaped", Terminal: true}}
	case "gate":
		return []authoringview.StageGraphNodeView{{ID: "ready", Initial: true}, {ID: "waiting"}, {ID: "done", Terminal: true}}
	default:
		panic("unknown relation fixture " + family)
	}
}

// This table is authored alongside the YAML below. It does not call carrier
// enumeration, graph builders, admission, or validation to compute expectations.
func authoredTransitionRelation(t *testing.T, family, flow string) []contracts.WorkflowStageTopologyEdge {
	t.Helper()
	node := identitytest.FlowNode(t, flow, "worker")
	var rows []contracts.WorkflowStageTopologyEdge
	handler := func(event, carrier, context, target string, sources []string, index int) {
		for _, from := range sources {
			e := contracts.WorkflowStageTopologyEdge{From: from, To: target, Source: carrier, Node: node, HandlerEvent: event, EventType: event, AdvanceCarrier: contracts.HandlerAdvanceCarrierKind(carrier)}
			if context != "" {
				e.RuleRef = transitionRelationRule(t, flow, event, context, index)
			}
			rows = append(rows, e)
		}
	}
	switch family {
	case "ordinary":
		active := []string{"ready", "working", "awaiting"}
		handler("direct", "handler.advances_to", "", "awaiting", active, 0)
		handler("inherited", "handler.advances_to", "", "done", active, 0)
		handler("created", "handler.advances_to", "", "working", []string{"ready"}, 0)
		for i := 0; i < 2; i++ {
			handler("selected", "handler.rules", "rules", "done", active, i)
			handler("completed", "handler.on_complete", "on_complete", "done", active, i)
		}
		handler("arrived", "handler.join.on_complete", "join.on_complete", "done", []string{"awaiting"}, 0)
		handler("arrived", "handler.join.timeout", "join.timeout", "done", []string{"awaiting"}, 0)
		timeout := &rows[len(rows)-1]
		timeout.EventType, timeout.TimerID, timeout.After, timeout.Timed = "platform.join_timeout", "awaiting", "3h", true
		timer := map[string]string{".": "advance", "left": "left.advance", "right": "right.advance"}[flow]
		rows = append(rows, contracts.WorkflowStageTopologyEdge{From: "ready", To: "working", Source: "timer", InternalOwner: "runtime", EventType: "timer:" + timer, TimerID: timer, After: "1h", Timed: true})
	case "loop":
		handler("created", "handler.advances_to", "", "waiting", []string{"waiting"}, 0)
		for _, op := range []struct{ event, operation, from, to string }{
			{"start", "start", "waiting", "drafting"},
			{"admit", "admit", "drafting", "review"},
			{"repeat", "repeat", "review", "drafting"},
			{"close", "close", "review", "done"},
		} {
			rows = append(rows, contracts.WorkflowStageTopologyEdge{From: op.from, To: op.to, Source: "loop." + op.operation, Node: node, HandlerEvent: op.event, EventType: op.event, AdvanceCarrier: "handler.advances_to", LoopID: "revision", LoopOperation: contracts.LoopOperationKind(op.operation)})
		}
		rows = append(rows, contracts.WorkflowStageTopologyEdge{From: "review", To: "escaped", Source: "loop.escape", Node: node, HandlerEvent: "repeat", EventType: "repeat", LoopID: "revision", LoopOperation: "repeat"})
	case "gate":
		handler("created", "handler.advances_to", "", "waiting", []string{"ready"}, 0)
		for _, verdict := range []string{"approve", "waive"} {
			rows = append(rows, contracts.WorkflowStageTopologyEdge{From: "waiting", To: "done", Source: "gate", InternalOwner: "runtime", EventType: "mailbox.card_decided", DecisionID: "review", Verdict: verdict})
		}
	default:
		t.Fatalf("unknown authored family %q", family)
	}
	return rows
}

func transitionRelationRule(t *testing.T, flow, event, context string, index int) identity.DeclarationIdentity {
	t.Helper()
	ref, err := identity.AdmitDeclarationIdentity(flow, "handler_rule", fmt.Sprintf("nodes[\"worker\"].handlers[%q].%s[%d]", event, context, index))
	if err != nil {
		t.Fatal(err)
	}
	return ref
}

func authoredTransitionView(rows []contracts.WorkflowStageTopologyEdge) []authoringview.StageGraphEdgeView {
	var out []authoringview.StageGraphEdgeView
	for _, row := range rows {
		owner := row.Node.Key()
		if row.InternalOwner != "" {
			owner = row.InternalOwner
		}
		limit := ""
		if row.LoopID != "" {
			limit = "2"
		}
		out = append(out, authoringview.StageGraphEdgeView{From: []string{row.From}, To: row.To, Source: row.Source, NodeID: owner, HandlerEvent: row.HandlerEvent, EventType: row.EventType, TimerID: row.TimerID, After: row.After, Timed: row.Timed, LoopID: row.LoopID, LoopOperation: string(row.LoopOperation), MaxAttempts: limit, LoopEscape: row.Source == "loop.escape", DecisionID: row.DecisionID, Verdict: row.Verdict})
	}
	return out
}

func transitionRelationBag[T any](rows []T) map[string]int {
	bag := map[string]int{}
	for _, row := range rows {
		bag[fmt.Sprintf("%#v", row)]++
	}
	return bag
}

func requireTransitionRelation[T any](t *testing.T, label string, got, want []T) {
	t.Helper()
	g, w := transitionRelationBag(got), transitionRelationBag(want)
	if !reflect.DeepEqual(g, w) {
		for row, count := range w {
			if g[row] != count {
				t.Errorf("%s expected multiplicity %d, got %d: %s", label, count, g[row], row)
			}
		}
		for row, count := range g {
			if w[row] != count {
				t.Errorf("%s unexpected multiplicity %d, want %d: %s", label, count, w[row], row)
			}
		}
		t.FailNow()
	}
}

func loadTransitionRelationFixture(t *testing.T, family string, reordered bool) *contracts.WorkflowContractBundle {
	t.Helper()
	root := t.TempDir()
	schema, handlers, events := transitionRelationDeclarations(family)
	flows := []string{".", "left", "right"}
	if reordered {
		slices.Reverse(flows)
	}
	for _, flow := range flows {
		files := map[string]string{
			"schema.yaml":   schema,
			"entities.yaml": "item:\n  expected: {type: '[text]', initial: []}\n  window: {type: text, initial: window}\n",
			"events.yaml":   events,
			"nodes.yaml":    "worker:\n  id: worker\n  execution_type: system_node\n  event_handlers:\n" + handlers,
		}
		for name, content := range files {
			if reordered {
				var doc yaml.Node
				if err := yaml.Unmarshal([]byte(content), &doc); err != nil {
					t.Fatal(err)
				}
				var reverseMappings func(*yaml.Node)
				reverseMappings = func(node *yaml.Node) {
					for i, child := range node.Content {
						// Rule order is executable policy, unlike sibling/map
						// declaration presentation; keep that policy unchanged.
						if node.Kind == yaml.MappingNode && i%2 == 1 && node.Content[i-1].Value == "rules" {
							continue
						}
						reverseMappings(child)
					}
					if node.Kind == yaml.MappingNode {
						for i, j := 0, len(node.Content)-2; i < j; i, j = i+2, j-2 {
							node.Content[i], node.Content[j] = node.Content[j], node.Content[i]
							node.Content[i+1], node.Content[j+1] = node.Content[j+1], node.Content[i+1]
						}
					}
				}
				reverseMappings(&doc)
				data, err := yaml.Marshal(&doc)
				if err != nil {
					t.Fatal(err)
				}
				content = string(data)
			}
			path := filepath.Join(root, flow, name)
			if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
		}
	}
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, root, contracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	return bundle
}

func transitionRelationDeclarations(family string) (schema, handlers, events string) {
	switch family {
	case "ordinary":
		return `stages:
  ready:
    initial: true
    timers:
      - {id: advance, after: 1h, advances_to: working}
      - {id: notify, after: 2h, emit: tick}
  working: {}
  awaiting: {}
  done: {terminal: true}
`, `    direct: {advances_to: awaiting}
    created: {create_entity: true, advances_to: working}
    selected:
      rules:
        first: {condition: "true", advances_to: done}
        second: {condition: else, advances_to: done}
    inherited:
      advances_to: done
      rules:
        first: {condition: "true"}
        second: {condition: else}
    completed:
      on_complete:
        - {condition: "true", advances_to: done}
        - {condition: else, advances_to: done}
    arrived:
      join:
        stage: awaiting
        members: {from: entity.expected, by: payload.member}
        window: {from: entity.window, by: payload.window}
        output: payload.result
        on_complete: {advances_to: done}
        timeout: {after: 3h, advances_to: done}
    tick: {}
`, "direct: {}\ncreated: {}\nselected: {}\ninherited: {}\ncompleted: {}\ntick: {}\narrived:\n  member: text\n  window: text\n  result: text\n"
	case "loop":
		return `stages:
  waiting: {initial: true}
  drafting: {}
  review: {}
  done: {terminal: true}
  escaped: {terminal: true}
loops:
  revision:
    revision_field: revision_id
    max_attempts: 2
    escape: {advances_to: escaped}
`, `    created: {create_entity: true, advances_to: waiting}
    start:
      loop: {start: revision, from: waiting}
      advances_to: drafting
    admit:
      loop: {admit: revision, from: drafting}
      advances_to: review
    repeat:
      loop: {repeat: revision, from: review}
      advances_to: drafting
    close:
      loop: {close: revision, from: review}
      advances_to: done
`, "created: {}\nstart: {}\nadmit: {revision_id: text}\nrepeat: {revision_id: text}\nclose: {revision_id: text}\n"
	case "gate":
		return `stages:
  ready: {initial: true}
  waiting:
    gate:
      decision: review
      outcomes:
        approve: {advances_to: done}
        waive: {advances_to: done}
  done: {terminal: true}
`, "    created: {create_entity: true, advances_to: waiting}\n", "created: {}\n"
	default:
		panic("unknown relation fixture " + family)
	}
}
