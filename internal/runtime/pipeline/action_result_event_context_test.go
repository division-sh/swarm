package pipeline

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	stdruntime "runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestArtifactRepoResultEventPreservesScopedProducerSourceRoute(t *testing.T) {
	cases := []struct {
		name            string
		eventType       string
		stateFlowPath   string
		inboundFlowPath string
		producerRoute   events.RouteIdentity
		targetRoute     events.RouteIdentity
		wantFlowPath    string
	}{
		{
			name:          "success uses state flow path",
			eventType:     "repo_scaffold.repo_commit_succeeded",
			stateFlowPath: "repo-scaffold/inst-1",
			wantFlowPath:  "repo-scaffold/inst-1",
		},
		{
			name:          "failure uses state flow path",
			eventType:     "repo_scaffold.repo_commit_failed",
			stateFlowPath: "repo-scaffold/inst-1",
			wantFlowPath:  "repo-scaffold/inst-1",
		},
		{
			name:            "success uses admitted producer route over stale inbound flow instance",
			eventType:       "repo_scaffold.repo_commit_succeeded",
			inboundFlowPath: "component-scaffold/component-a",
			producerRoute: events.RouteIdentity{
				FlowID:       "repo-scaffold",
				FlowInstance: "repo-scaffold",
				EntityID:     "ent-repo",
			},
			wantFlowPath: "repo-scaffold",
		},
		{
			name:            "failure ignores delivery target in favor of exact producer route",
			eventType:       "repo_scaffold.repo_commit_failed",
			inboundFlowPath: "component-scaffold/component-a",
			producerRoute: events.RouteIdentity{
				FlowID:       "repo-scaffold",
				FlowInstance: "repo-scaffold",
				EntityID:     "ent-repo",
			},
			targetRoute: events.RouteIdentity{
				FlowID:       "repo-scaffold",
				FlowInstance: "repo-scaffold",
				EntityID:     "target-ent",
			},
			wantFlowPath: "repo-scaffold",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			entityID := "ent-repo"
			parentEnvelope := events.EventEnvelope{EntityID: "upstream-ent", FlowInstance: tc.inboundFlowPath}
			if tc.stateFlowPath != "" || !tc.producerRoute.Empty() || !tc.targetRoute.Empty() {
				parentEnvelope = events.EnvelopeForSourceRoute(parentEnvelope, events.RouteIdentity{
					FlowID:       "upstream",
					FlowInstance: "upstream/inst-0",
					EntityID:     "upstream-ent",
				})

			}
			if !tc.targetRoute.Empty() {
				parentEnvelope = events.EnvelopeForTargetRoute(parentEnvelope, tc.targetRoute)
			} else if tc.inboundFlowPath != "" {
				parentEnvelope = events.EnvelopeForFlowInstance(parentEnvelope, tc.inboundFlowPath)
			}
			parent := eventtest.RunCreatingRootIngress(
				"evt-parent",
				"repo_scaffold.repo_commit_requested",
				"workflow-runtime",
				"",
				json.RawMessage(`{"request_id":"req-1"}`),
				4,
				"run-1",
				"",
				parentEnvelope,
				time.Unix(1_700_000_000, 0).UTC())
			stateMetadata := map[string]any{}
			if tc.stateFlowPath != "" {
				stateMetadata["flow_path"] = tc.stateFlowPath
			}
			execCtx := runtimeengine.ExecutionContext{
				Request: runtimeengine.ExecutionRequest{
					EntityID:   identity.NormalizeEntityID(entityID),
					Node:       pipelineNode(t, "repo-scaffold", "repo-scaffold-node"),
					Event:      parent,
					ChainDepth: 4,
					State: runtimeengine.StateSnapshot{
						EntityID:     identity.NormalizeEntityID(entityID),
						StateCarrier: runtimeengine.NewStateCarrier(stateMetadata, nil, nil),
					},
				},
			}

			mode := "static"
			if tc.wantFlowPath != "repo-scaffold" {
				mode = "template"
			}
			producerRoute := tc.producerRoute.Normalized()
			if producerRoute.Empty() {
				producerRoute = events.RouteIdentity{FlowID: "repo-scaffold", FlowInstance: tc.wantFlowPath, EntityID: entityID}
			}
			execCtx.Request.ProducerSource = mustActionResultRoutingSource(t, mode, producerRoute)
			pc := &PipelineCoordinator{module: staticSemanticWorkflowModule{source: actionResultRouteSource(t, mode)}}
			intent, err := pc.artifactRepoResultEvent(execCtx, tc.eventType, map[string]any{"ok": true})
			if err != nil {
				t.Fatalf("artifactRepoResultEvent: %v", err)
			}
			emitted := intent.Event
			wantEventType := "repo-scaffold/" + tc.eventType
			if got := string(emitted.Type()); got != wantEventType {
				t.Fatalf("event type = %q, want %q", got, wantEventType)
			}
			if got := emitted.EntityID(); got != entityID {
				t.Fatalf("entity_id = %q, want %q", got, entityID)
			}
			if got := emitted.FlowInstance(); got != tc.wantFlowPath {
				t.Fatalf("flow_instance = %q, want %q", got, tc.wantFlowPath)
			}
			wantSource := events.RouteIdentity{
				FlowID:       "repo-scaffold",
				FlowInstance: tc.wantFlowPath,
				EntityID:     entityID,
			}.Normalized()
			if got := emitted.SourceRoute(); got != wantSource {
				t.Fatalf("source route = %#v, want %#v", got, wantSource)
			}
			if got := emitted.TargetRoute(); !got.Empty() {
				t.Fatalf("target route = %#v, want empty result-event target", got)
			}
			if got := emitted.ParentEventID(); got != parent.ID() {
				t.Fatalf("parent_event_id = %q, want %q", got, parent.ID())
			}
			if got := emitted.RunID(); got != parent.RunID() {
				t.Fatalf("run_id = %q, want %q", got, parent.RunID())
			}
			if got := emitted.ChainDepth(); got != 5 {
				t.Fatalf("chain_depth = %d, want 5", got)
			}
			if got := intent.ParentEventID; got != parent.ID() {
				t.Fatalf("intent parent_event_id = %q, want %q", got, parent.ID())
			}
		})
	}
}

func TestActionResultEventTypeResolvesAgainstProducerRoute(t *testing.T) {
	templateSource := actionResultRouteSource(t, "template")
	staticSource := actionResultRouteSource(t, "static")
	cases := []struct {
		name          string
		source        semanticview.Source
		eventType     string
		producerRoute events.RouteIdentity
		want          string
	}{
		{
			source:    templateSource,
			name:      "template instance local success event",
			eventType: "repo_scaffold.repo_commit_succeeded",
			producerRoute: events.RouteIdentity{
				FlowID:       "repo-scaffold",
				FlowInstance: "repo-scaffold/inst-1",
				EntityID:     "ent-repo",
			},
			want: "repo-scaffold/repo_scaffold.repo_commit_succeeded",
		},
		{
			source:    staticSource,
			name:      "static service local failure event",
			eventType: "repo_scaffold.repo_commit_failed",
			producerRoute: events.RouteIdentity{
				FlowID:       "repo-scaffold",
				FlowInstance: "repo-scaffold",
				EntityID:     "ent-repo",
			},
			want: "repo-scaffold/repo_scaffold.repo_commit_failed",
		},
		{
			source:    templateSource,
			name:      "declaration-scoped event is preserved",
			eventType: "repo-scaffold/repo_scaffold.repo_commit_succeeded",
			producerRoute: events.RouteIdentity{
				FlowID:       "repo-scaffold",
				FlowInstance: "repo-scaffold/inst-1",
				EntityID:     "ent-repo",
			},
			want: "repo-scaffold/repo_scaffold.repo_commit_succeeded",
		},
		{
			source:    staticSource,
			name:      "static manual prefix stays service scoped",
			eventType: "repo-scaffold/repo_scaffold.repo_commit_failed",
			producerRoute: events.RouteIdentity{
				FlowID:       "repo-scaffold",
				FlowInstance: "repo-scaffold",
				EntityID:     "ent-repo",
			},
			want: "repo-scaffold/repo_scaffold.repo_commit_failed",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := actionResultEventType(tc.source, "repo-scaffold", tc.eventType, tc.producerRoute)
			if got != tc.want {
				t.Fatalf("actionResultEventType() = %q, want %q", got, tc.want)
			}
		})
	}
}

func actionResultRouteSource(t *testing.T, mode string) semanticview.Source {
	t.Helper()
	return loadWorkflowTempSource(t, map[string]string{

		"repo-scaffold/schema.yaml": `name: repo-scaffold
initial_state: ready
terminal_states: [done]
states: [ready, done]
`,
		"repo-scaffold/events.yaml": `repo_scaffold.repo_commit_requested: {}
repo_scaffold.repo_commit_succeeded: {}
repo_scaffold.repo_commit_failed: {}
`,
	})
}

func mustActionResultRoutingSource(t testing.TB, mode string, route events.RouteIdentity) events.RoutingSource {
	t.Helper()
	var (
		source events.RoutingSource
		err    error
	)
	if mode == "template" {
		source, err = events.NewConcreteTemplateInstanceRoutingSource(route)
	} else {
		source, err = events.NewStaticFlowRoutingSource(route)
	}
	if err != nil {
		t.Fatalf("construct %s action-result routing source: %v", mode, err)
	}
	return source
}

func TestRuntimeActionResultEventProducerInventoryHasNoMutableEmitCollector(t *testing.T) {
	_, filename, _, ok := stdruntime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(filename), "..", "..", ".."))
	runtimeRoot := filepath.Join(repoRoot, "internal", "runtime")

	fset := token.NewFileSet()
	var calls []string
	err := filepath.WalkDir(runtimeRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			if !callsQueueActionEmitIntent(call) {
				return true
			}
			position := fset.Position(call.Pos())
			rel, relErr := filepath.Rel(repoRoot, position.Filename)
			if relErr != nil {
				rel = position.Filename
			}
			calls = append(calls, rel+":"+strconv.Itoa(position.Line))
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walk runtime source: %v", err)
	}

	if len(calls) != 0 {
		t.Fatalf("QueueActionEmitIntent production calls = %#v, want none", calls)
	}
}

func callsQueueActionEmitIntent(call *ast.CallExpr) bool {
	switch fn := call.Fun.(type) {
	case *ast.SelectorExpr:
		return fn.Sel.Name == "QueueActionEmitIntent"
	case *ast.Ident:
		return fn.Name == "QueueActionEmitIntent"
	default:
		return false
	}
}
