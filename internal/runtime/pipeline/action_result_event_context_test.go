package pipeline

import (
	"context"
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
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
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
				EntityID:     "stale-ent",
			},
			wantFlowPath: "repo-scaffold",
		},
		{
			name:            "failure uses admitted delivery target route over stale inbound flow instance",
			eventType:       "repo_scaffold.repo_commit_failed",
			inboundFlowPath: "component-scaffold/component-a",
			targetRoute: events.RouteIdentity{
				FlowID:       "repo-scaffold",
				FlowInstance: "repo-scaffold",
				EntityID:     "stale-ent",
			},
			wantFlowPath: "repo-scaffold",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			entityID := "ent-repo"
			parentEnvelope := events.EventEnvelope{EntityID: "upstream-ent", FlowInstance: tc.inboundFlowPath}
			parent := events.NewProjectionEvent(
				"evt-parent",
				"repo_scaffold.repo_commit_requested",
				"workflow-runtime",
				"",
				json.RawMessage(`{"request_id":"req-1"}`),
				4,
				"run-1",
				"",
				parentEnvelope,
				time.Unix(1_700_000_000, 0).UTC(),
			)
			if tc.stateFlowPath != "" || !tc.producerRoute.Empty() || !tc.targetRoute.Empty() {
				parent = parent.WithSourceRoute(events.RouteIdentity{
					FlowID:       "upstream",
					FlowInstance: "upstream/inst-0",
					EntityID:     "upstream-ent",
				})
			}
			if !tc.targetRoute.Empty() {
				parent = parent.WithTargetRoute(tc.targetRoute)
			} else if tc.inboundFlowPath != "" {
				parent = parent.WithFlowInstance(tc.inboundFlowPath)
			}
			stateMetadata := map[string]any{}
			if tc.stateFlowPath != "" {
				stateMetadata["flow_path"] = tc.stateFlowPath
			}
			execCtx := runtimeengine.ExecutionContext{
				Request: runtimeengine.ExecutionRequest{
					EntityID:      identity.NormalizeEntityID(entityID),
					FlowID:        identity.NormalizeFlowID("repo-scaffold"),
					NodeID:        identity.NormalizeNodeID("repo-scaffold-node"),
					Event:         parent,
					ChainDepth:    4,
					ProducerRoute: tc.producerRoute,
					State: runtimeengine.StateSnapshot{
						EntityID:     identity.NormalizeEntityID(entityID),
						StateCarrier: runtimeengine.NewStateCarrier(stateMetadata, nil, nil),
					},
				},
			}

			pc := &PipelineCoordinator{}
			var intents []runtimeengine.EmitIntent
			ctx := runtimeengine.WithActionEmitIntentCollector(context.Background(), &intents)
			queued, err := pc.queueArtifactRepoResultEvent(ctx, execCtx, tc.eventType, map[string]any{"ok": true})
			if err != nil {
				t.Fatalf("queueArtifactRepoResultEvent: %v", err)
			}
			if !queued {
				t.Fatal("queueArtifactRepoResultEvent queued=false, want true")
			}
			if len(intents) != 1 {
				t.Fatalf("queued intents = %d, want 1", len(intents))
			}
			emitted := intents[0].Event
			if got := string(emitted.Type()); got != tc.eventType {
				t.Fatalf("event type = %q, want %q", got, tc.eventType)
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
			if got := intents[0].ParentEventID; got != parent.ID() {
				t.Fatalf("intent parent_event_id = %q, want %q", got, parent.ID())
			}
		})
	}
}

func TestActionResultEventTypeResolvesAgainstProducerRoute(t *testing.T) {
	source := loadWorkflowTempSource(t, map[string]string{
		"package.yaml": `name: action-result-event-type
version: 1.0.0
flows:
  - id: repo-scaffold
    flow: repo-scaffold
    mode: template
`,
		"flows/repo-scaffold/schema.yaml": `name: repo-scaffold
initial_state: ready
terminal_states: [done]
states: [ready, done]
`,
		"flows/repo-scaffold/events.yaml": `repo_scaffold.repo_commit_succeeded: {}
repo_scaffold.repo_commit_failed: {}
`,
	})
	cases := []struct {
		name          string
		eventType     string
		producerRoute events.RouteIdentity
		want          string
	}{
		{
			name:      "template instance local success event",
			eventType: "repo_scaffold.repo_commit_succeeded",
			producerRoute: events.RouteIdentity{
				FlowID:       "repo-scaffold",
				FlowInstance: "repo-scaffold/inst-1",
				EntityID:     "ent-repo",
			},
			want: "repo-scaffold/inst-1/repo_scaffold.repo_commit_succeeded",
		},
		{
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
			name:      "already scoped event is preserved",
			eventType: "repo-scaffold/inst-1/repo_scaffold.repo_commit_succeeded",
			producerRoute: events.RouteIdentity{
				FlowID:       "repo-scaffold",
				FlowInstance: "repo-scaffold/inst-1",
				EntityID:     "ent-repo",
			},
			want: "repo-scaffold/inst-1/repo_scaffold.repo_commit_succeeded",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := actionResultEventType(source, "repo-scaffold", tc.eventType, tc.producerRoute)
			if got != tc.want {
				t.Fatalf("actionResultEventType() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestRuntimeActionResultEventProducerInventoryOnlyArtifactRepoQueuesResultEvents(t *testing.T) {
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

	if len(calls) != 1 || !strings.HasPrefix(calls[0], filepath.ToSlash("internal/runtime/pipeline/artifact_repo.go:")) {
		t.Fatalf("QueueActionEmitIntent production calls = %#v, want only artifact_repo.go action result-event producer", calls)
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
