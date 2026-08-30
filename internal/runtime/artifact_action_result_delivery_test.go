package runtime_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
)

func TestArtifactRepoCommitResultEventsFlowThroughDurableCallbackDelivery(t *testing.T) {
	tests := []struct {
		name            string
		requestEventID  string
		requestID       string
		mvpYAML         string
		resultEventName string
		resultKind      string
	}{
		{
			name:            "success",
			requestEventID:  "99999999-9999-4999-8999-999999999941",
			requestID:       "99999999-9999-4999-8999-999999999951",
			mvpYAML:         "name: Demo\n",
			resultEventName: "repo_scaffold.repo_commit_succeeded",
			resultKind:      "ready",
		},
		{
			name:            "failure",
			requestEventID:  "99999999-9999-4999-8999-999999999942",
			requestID:       "99999999-9999-4999-8999-999999999952",
			mvpYAML:         "title: Demo\n",
			resultEventName: "repo_scaffold.repo_commit_failed",
			resultKind:      "failed",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			repoNodeID := artifactActionResultNodeID(t)
			resultEventType := "repo-scaffold/inst-1/" + tc.resultEventName
			bundle := loadRuntimeTempBundle(t, artifactActionResultDeliveryFixtureFiles())
			source := semanticview.Wrap(bundle)
			_, db, cleanup := testutil.StartPostgres(t)
			t.Cleanup(cleanup)
			ctx := seedRuntimeTestRun(t, db)
			pg := storetest.AdmitPostgresRuntimeStore(t, db)
			var pc *runtimepipeline.PipelineCoordinator
			bus, err := newScopedTestEventBus(t, pg, runtimebus.EventBusOptions{
				ContractBundle: source,
				InterceptorProvider: func() []runtimebus.EventInterceptor {
					if pc == nil {
						return nil
					}
					return []runtimebus.EventInterceptor{pc}
				},
			})
			if err != nil {
				t.Fatalf("NewEventBusWithOptions: %v", err)
			}
			module := newRuntimeTestWorkflowModule(t, source)
			resultHandlerStarted := make(chan string, 4)
			pc = newExternalRuntimeTestPipelineCoordinator(t, bus, db, pg, runtimepipeline.PipelineCoordinatorOptions{
				WorkOwner:           runtimeTestEventBusWorkOwner(t, bus),
				Module:              module,
				Persistence:         runtimepipeline.NewWorkflowPersistence(pg),
				RunLifecycle:        pg,
				PipelineObligations: pg.PipelineObligations(),
				DeliveryStore:       pg,
				FlowRoutes:          bus,
				ArtifactRoot:        t.TempDir(),
				TestWorkflowNodeHandlerStartHook: func(_ context.Context, nodeID string, evt events.Event) error {
					if strings.TrimSpace(nodeID) == repoNodeID && strings.TrimSpace(string(evt.Type())) == resultEventType {
						select {
						case resultHandlerStarted <- evt.ID():
						default:
						}
					}
					return nil
				},
			})

			if _, err := pc.MaterializeInitialEntry(testLiveExecutionContext(ctx), artifactActionResultWorkflowInstance(), time.Now().UTC()); err != nil {
				t.Fatalf("seed workflow instance: %v", err)
			}
			if err := bus.AddFlowInstanceRouteContext(ctx, runtimebus.FlowInstanceRouteMaterializationRequest{Identity: runtimeflowidentity.DeriveRoute("repo-scaffold", "inst-1")}); err != nil {
				t.Fatalf("AddFlowInstanceRoute: %v", err)
			}

			requestPayload, err := json.Marshal(map[string]any{
				"request_id": tc.requestID,
				"mvp_yaml":   tc.mvpYAML,
			})
			if err != nil {
				t.Fatalf("marshal request payload: %v", err)
			}
			requestEvent := eventtest.ExistingRunRootIngress(
				tc.requestEventID,
				events.EventType("repo-scaffold/inst-1/repo_scaffold.repo_commit_requested"),
				"test",
				"",
				requestPayload,
				0,
				templateInstanceDeliveryRunID,
				events.EnvelopeForSourceRoute(
					events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, artifactActionResultEntityID), "repo-scaffold/inst-1"),
					events.RouteIdentity{FlowID: "repo-scaffold", FlowInstance: "repo-scaffold/inst-1", EntityID: artifactActionResultEntityID},
				),
				time.Now().UTC(),
			)

			if err := bus.Publish(ctx, requestEvent); err != nil {
				t.Fatalf("Publish request event: %v", err)
			}
			resultEventID := waitRuntimeEventID(t, ctx, db, `
				SELECT event_id::text
				FROM events
				WHERE event_name = $1 AND source_event_id = $2::uuid
			`, []any{resultEventType, tc.requestEventID})
			assertArtifactActionResultEventContext(t, ctx, db, resultEventID, tc.resultKind, "repo-scaffold/inst-1")
			assertArtifactActionResultNodeRoute(t, ctx, db, resultEventID, "repo-scaffold/inst-1")
			waitArtifactActionResultHandlerStarted(t, ctx, db, resultHandlerStarted, resultEventID)
			waitArtifactActionResultDBCount(t, ctx, db, `
				SELECT COUNT(*)
				FROM event_deliveries
				WHERE event_id = $1::uuid
				  AND subscriber_type = 'node'
				  AND subscriber_id = $3
				  AND status = 'delivered'
				  AND settled_at IS NOT NULL
				  AND delivery_target_route @> $2::jsonb
			`, 1, resultEventID, artifactActionResultDeliveryTargetRouteJSON("repo-scaffold/inst-1"), repoNodeID)
			waitRuntimeNodeDeliveryOutcome(t, ctx, db, resultEventID, repoNodeID)
		})
	}
}

func TestArtifactRepoCommitResultEventsFlowThroughStaticServiceCallbackDelivery(t *testing.T) {
	tests := []struct {
		name            string
		requestEventID  string
		requestID       string
		mvpYAML         string
		resultEventName string
		resultKind      string
		requestFlowPath string
		wantFlowPath    string
	}{
		{
			name:            "success",
			requestEventID:  "99999999-9999-4999-8999-999999999961",
			requestID:       "99999999-9999-4999-8999-999999999971",
			mvpYAML:         "name: Demo\n",
			resultEventName: "repo_scaffold.repo_commit_succeeded",
			resultKind:      "ready",
			requestFlowPath: "repo-scaffold",
			wantFlowPath:    "repo-scaffold",
		},
		{
			name:            "failure",
			requestEventID:  "99999999-9999-4999-8999-999999999962",
			requestID:       "99999999-9999-4999-8999-999999999972",
			mvpYAML:         "title: Demo\n",
			resultEventName: "repo_scaffold.repo_commit_failed",
			resultKind:      "failed",
			requestFlowPath: "repo-scaffold",
			wantFlowPath:    "repo-scaffold",
		},
		{
			name:            "wildcard_child_inbound_success",
			requestEventID:  "99999999-9999-4999-8999-999999999963",
			requestID:       "99999999-9999-4999-8999-999999999973",
			mvpYAML:         "name: Demo\n",
			resultEventName: "repo_scaffold.repo_commit_succeeded",
			resultKind:      "ready",
			requestFlowPath: "repo-scaffold/child-1",
			wantFlowPath:    "repo-scaffold",
		},
		{
			name:            "wildcard_child_inbound_failure",
			requestEventID:  "99999999-9999-4999-8999-999999999964",
			requestID:       "99999999-9999-4999-8999-999999999974",
			mvpYAML:         "title: Demo\n",
			resultEventName: "repo_scaffold.repo_commit_failed",
			resultKind:      "failed",
			requestFlowPath: "repo-scaffold/child-1",
			wantFlowPath:    "repo-scaffold",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			repoNodeID := artifactActionResultNodeID(t)
			resultEventType := "repo-scaffold/" + tc.resultEventName
			bundle := loadRuntimeTempBundle(t, artifactActionResultStaticDeliveryFixtureFiles())
			source := semanticview.Wrap(bundle)
			_, db, cleanup := testutil.StartPostgres(t)
			t.Cleanup(cleanup)
			ctx := seedRuntimeTestRun(t, db)
			pg := storetest.AdmitPostgresRuntimeStore(t, db)
			var pc *runtimepipeline.PipelineCoordinator
			bus, err := newScopedTestEventBus(t, pg, runtimebus.EventBusOptions{
				ContractBundle: source,
				InterceptorProvider: func() []runtimebus.EventInterceptor {
					if pc == nil {
						return nil
					}
					return []runtimebus.EventInterceptor{pc}
				},
			})
			if err != nil {
				t.Fatalf("NewEventBusWithOptions: %v", err)
			}
			module := newRuntimeTestWorkflowModule(t, source)
			resultHandlerStarted := make(chan string, 4)
			pc = newExternalRuntimeTestPipelineCoordinator(t, bus, db, pg, runtimepipeline.PipelineCoordinatorOptions{
				WorkOwner:           runtimeTestEventBusWorkOwner(t, bus),
				Module:              module,
				Persistence:         runtimepipeline.NewWorkflowPersistence(pg),
				RunLifecycle:        pg,
				PipelineObligations: pg.PipelineObligations(),
				DeliveryStore:       pg,
				FlowRoutes:          bus,
				ArtifactRoot:        t.TempDir(),
				TestWorkflowNodeHandlerStartHook: func(_ context.Context, nodeID string, evt events.Event) error {
					if strings.TrimSpace(nodeID) == repoNodeID && strings.TrimSpace(string(evt.Type())) == resultEventType {
						select {
						case resultHandlerStarted <- evt.ID():
						default:
						}
					}
					return nil
				},
			})

			if _, err := pc.MaterializeInitialEntry(testLiveExecutionContext(ctx), artifactActionResultStaticWorkflowInstance(), time.Now().UTC()); err != nil {
				t.Fatalf("seed workflow instance: %v", err)
			}

			requestPayload, err := json.Marshal(map[string]any{
				"request_id": tc.requestID,
				"mvp_yaml":   tc.mvpYAML,
			})
			if err != nil {
				t.Fatalf("marshal request payload: %v", err)
			}
			requestEvent := eventtest.ExistingRunRootIngress(
				tc.requestEventID,
				events.EventType("repo-scaffold/repo_scaffold.repo_commit_requested"),
				"test",
				"",
				requestPayload,
				0,
				templateInstanceDeliveryRunID,
				events.EnvelopeForSourceRoute(
					events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, artifactActionResultEntityID), tc.requestFlowPath),
					events.RouteIdentity{FlowID: "repo-scaffold", FlowInstance: tc.requestFlowPath, EntityID: artifactActionResultEntityID},
				),
				time.Now().UTC(),
			)

			if err := bus.Publish(ctx, requestEvent); err != nil {
				t.Fatalf("Publish request event: %v", err)
			}

			resultEventID := waitRuntimeEventID(t, ctx, db, `
				SELECT event_id::text
				FROM events
				WHERE event_name = $1 AND source_event_id = $2::uuid
			`, []any{resultEventType, tc.requestEventID})
			assertArtifactActionResultEventContext(t, ctx, db, resultEventID, tc.resultKind, tc.wantFlowPath)
			assertArtifactActionResultNodeRoute(t, ctx, db, resultEventID, tc.wantFlowPath)
			waitArtifactActionResultHandlerStarted(t, ctx, db, resultHandlerStarted, resultEventID)
			waitArtifactActionResultDBCount(t, ctx, db, `
				SELECT COUNT(*)
				FROM event_deliveries
				WHERE event_id = $1::uuid
				  AND subscriber_type = 'node'
				  AND subscriber_id = $3
				  AND status = 'delivered'
				  AND settled_at IS NOT NULL
				  AND delivery_target_route @> $2::jsonb
			`, 1, resultEventID, artifactActionResultDeliveryTargetRouteJSON(tc.wantFlowPath), repoNodeID)
			waitRuntimeNodeDeliveryOutcome(t, ctx, db, resultEventID, repoNodeID)
		})
	}
}

const artifactActionResultEntityID = "22222222-2222-4222-8222-222222222222"

func artifactActionResultNodeID(t testing.TB) string {
	t.Helper()
	return identitytest.FlowNode(t, "repo-scaffold", "repo-scaffold-node").Key()
}

func artifactActionResultWorkflowInstance() runtimepipeline.WorkflowInstance {
	enteredAt := time.Now().UTC()
	fields := map[string]any{
		"repo_id":          "11111111-1111-1111-1111-111111111111",
		"namespace":        "tenant-alpha",
		"partition_key":    "project-42",
		"display_slug":     "Demo Artifact",
		"source_record_id": "record-123",
	}
	return runtimepipeline.WorkflowInstance{
		InstanceID:      "inst-1",
		StorageRef:      "repo-scaffold/inst-1",
		EntityID:        artifactActionResultEntityID,
		WorkflowName:    "repo-scaffold",
		WorkflowVersion: "1.0.0",
		CurrentState:    "ready",
		EnteredStageAt:  enteredAt,
		CreatedAt:       enteredAt,
		Fields:          fields,
		EntityType:      "test_entity",
	}
}

func artifactActionResultStaticWorkflowInstance() runtimepipeline.WorkflowInstance {
	instance := artifactActionResultWorkflowInstance()
	instance.InstanceID = "repo-scaffold"
	instance.StorageRef = "repo-scaffold"
	return instance
}

func assertArtifactActionResultEventContext(t *testing.T, ctx context.Context, db *sql.DB, eventID, resultKind, wantFlowInstance string) {
	t.Helper()
	var entityID, flowInstance, sourceRouteJSON, payloadJSON string
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(entity_id::text, ''), COALESCE(flow_instance, ''), source_route::text, payload::text
		FROM events
		WHERE event_id = $1::uuid
	`, eventID).Scan(&entityID, &flowInstance, &sourceRouteJSON, &payloadJSON); err != nil {
		t.Fatalf("query result event context: %v", err)
	}
	if entityID != artifactActionResultEntityID {
		t.Fatalf("result event entity_id = %q, want %q", entityID, artifactActionResultEntityID)
	}
	if flowInstance != wantFlowInstance {
		t.Fatalf("result event flow_instance = %q, want %s", flowInstance, wantFlowInstance)
	}
	var sourceRoute map[string]any
	if err := json.Unmarshal([]byte(sourceRouteJSON), &sourceRoute); err != nil {
		t.Fatalf("decode source route %q: %v", sourceRouteJSON, err)
	}
	if got := strings.TrimSpace(asRuntimeTestString(sourceRoute["flow_id"])); got != "repo-scaffold" {
		t.Fatalf("source route flow_id = %q, want repo-scaffold: %#v", got, sourceRoute)
	}
	if got := strings.TrimSpace(asRuntimeTestString(sourceRoute["flow_instance"])); got != wantFlowInstance {
		t.Fatalf("source route flow_instance = %q, want %s: %#v", got, wantFlowInstance, sourceRoute)
	}
	if got := strings.TrimSpace(asRuntimeTestString(sourceRoute["entity_id"])); got != artifactActionResultEntityID {
		t.Fatalf("source route entity_id = %q, want %s: %#v", got, artifactActionResultEntityID, sourceRoute)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		t.Fatalf("decode result payload: %v", err)
	}
	if got := strings.TrimSpace(asRuntimeTestString(payload["result_kind"])); got != resultKind {
		t.Fatalf("result payload result_kind = %q, want %q: %#v", got, resultKind, payload)
	}
}

func assertArtifactActionResultNodeRoute(t *testing.T, ctx context.Context, db *sql.DB, eventID, wantFlowInstance string) {
	t.Helper()
	wantRoute := artifactActionResultDeliveryTargetRouteJSON(wantFlowInstance)
	deadline := time.Now().Add(5 * time.Second)
	for {
		var got int
		if err := db.QueryRowContext(ctx, `
			SELECT COUNT(*)
			FROM event_deliveries
			WHERE event_id = $1::uuid
			  AND subscriber_type = 'node'
			  AND subscriber_id = $3
			  AND delivery_target_route @> $2::jsonb
		`, eventID, wantRoute, artifactActionResultNodeID(t)).Scan(&got); err != nil {
			t.Fatalf("query result delivery route: %v", err)
		}
		if got == 1 {
			return
		}
		if time.Now().After(deadline) {
			var rows string
			if err := db.QueryRowContext(ctx, `
				SELECT COALESCE(jsonb_agg(jsonb_build_object(
					'subscriber_type', subscriber_type,
					'subscriber_id', subscriber_id,
					'status', status,
					'reason_code', reason_code,
					'delivery_target_route', delivery_target_route
				))::text, '[]')
				FROM event_deliveries
				WHERE event_id = $1::uuid
			`, eventID).Scan(&rows); err != nil {
				t.Fatalf("query result delivery route debug rows: %v", err)
			}
			t.Fatalf("delivery route for event %s missing route %s; rows=%s", eventID, wantRoute, rows)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func waitArtifactActionResultHandlerStarted(t *testing.T, ctx context.Context, db *sql.DB, started <-chan string, eventID string) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case got := <-started:
			if got == eventID {
				return
			}
		case <-deadline:
			var deliveries, receipts string
			if err := db.QueryRowContext(ctx, `
				SELECT COALESCE(jsonb_agg(to_jsonb(d))::text, '[]')
				FROM event_deliveries d WHERE event_id = $1::uuid
			`, eventID).Scan(&deliveries); err != nil {
				t.Fatalf("query callback delivery diagnostics: %v", err)
			}
			if err := db.QueryRowContext(ctx, `
				SELECT COALESCE(jsonb_agg(to_jsonb(r))::text, '[]')
				FROM event_receipts r WHERE event_id = $1::uuid
			`, eventID).Scan(&receipts); err != nil {
				t.Fatalf("query callback receipt diagnostics: %v", err)
			}
			t.Fatalf("callback handler did not start for result event %s; deliveries=%s receipts=%s", eventID, deliveries, receipts)
		}
	}
}

func waitArtifactActionResultDBCount(t *testing.T, ctx context.Context, db *sql.DB, query string, want int, args ...any) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var got int
		if err := db.QueryRowContext(ctx, query, args...).Scan(&got); err != nil {
			t.Fatalf("query count: %v", err)
		}
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			var diagnostics string
			if len(args) > 0 {
				_ = db.QueryRowContext(ctx, `
					SELECT jsonb_build_object(
						'deliveries', COALESCE(jsonb_agg(jsonb_build_object(
						'status', d.status,
						'target', d.delivery_target_route,
						'claim_version', d.claim_version,
						'outcomes', (SELECT COALESCE(jsonb_agg(to_jsonb(o)), '[]'::jsonb) FROM event_delivery_outcomes o WHERE o.delivery_id = d.delivery_id)
						)), '[]'::jsonb),
						'receipts', (SELECT COALESCE(jsonb_agg(to_jsonb(r)), '[]'::jsonb) FROM event_receipts r WHERE r.event_id = $1::uuid)
					)::text
					FROM event_deliveries d WHERE d.event_id = $1::uuid
				`, args[0]).Scan(&diagnostics)
			}
			t.Fatalf("count = %d, want %d for query %s; deliveries=%s", got, want, strings.TrimSpace(query), diagnostics)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func artifactActionResultDeliveryTargetRouteJSON(flowInstance string) string {
	return `{"kind":"existing_entity","route":{"flow_instance":"` + flowInstance + `","entity_id":"` + artifactActionResultEntityID + `"}}`
}

func asRuntimeTestString(value any) string {
	switch v := value.(type) {
	case string:
		return v
	default:
		return ""
	}
}

func artifactActionResultDeliveryFixtureFiles() map[string]string {
	return map[string]string{
		"schema.yaml": "name: artifact-action-result-delivery\n",
		"repo-scaffold/schema.yaml": `name: repo-scaffold
mode: template
instance: request_id
initial_state: ready
terminal_states: [done]
states: [ready, done]
`,
		"repo-scaffold/entities.yaml": "test_entity: {}\n",
		"repo-scaffold/types.yaml": `types:
  ArtifactProvenance:
    artifact_type: text
    source_record_id: text
  ArtifactManifestFile:
    path: text
    content_type: text
    sha256: text
    size_bytes: integer
  ArtifactManifest:
    provider: text
    repo_id: text
    namespace: text
    partition_key: text
    display_slug: text
    request_id: text
    source_event_id: text
    repo_url: text
    ref: text
    tree_hash: text
    files: [ArtifactManifestFile]
    provenance: ArtifactProvenance
`,
		"repo-scaffold/events.yaml": `repo_scaffold.repo_commit_requested:
  request_id: string
  mvp_yaml: string
repo_scaffold.repo_commit_succeeded:
  repo_id: string
  namespace: string
  partition_key: string?
  display_slug: string?
  request_id: string
  source_event_id: string
  repo_url: string
  current_ref: string
  file_manifest: ArtifactManifest
  provenance: ArtifactProvenance
  result_kind: string
repo_scaffold.repo_commit_failed:
  repo_id: string
  namespace: string
  partition_key: string?
  display_slug: string?
  request_id: string
  source_event_id: string
  failure: platform.failure/v1 envelope
  provenance: ArtifactProvenance
  result_kind: string
  request_copy: string?
`,
		"repo-scaffold/nodes.yaml": `repo-scaffold-node:
  id: repo-scaffold-node
  execution_type: system_node
  subscribes_to:
    - repo_scaffold.repo_commit_requested
    - repo_scaffold.repo_commit_succeeded
    - repo_scaffold.repo_commit_failed
  produces:
    - repo_scaffold.repo_commit_succeeded
    - repo_scaffold.repo_commit_failed
  event_handlers:
    repo_scaffold.repo_commit_requested:
      action:
        id: artifact_repo_commit
        artifact_repo:
          provider: local_git
          repo_id:
            ref: entity.repo_id
          namespace:
            ref: entity.namespace
          partition_key:
            ref: entity.partition_key
          display_slug:
            ref: entity.display_slug
          request_id:
            ref: payload.request_id
          author:
            literal: artifact-writer
          provenance:
            artifact_type:
              literal: fixture
            source_record_id:
              ref: entity.source_record_id
          allowed_paths:
            - specs/mvp.yaml
          files:
            - path:
                literal: specs/mvp.yaml
              content:
                ref: payload.mvp_yaml
              content_type: yaml
              schema:
                type: object
                required_fields:
                  - name
              max_bytes: 4096
          output:
            repo_url: repo_url
            current_ref: current_ref
            file_manifest: file_manifest
            status: status
            failure: failure
            last_request_id: last_request_id
            last_source_event_id: last_source_event_id
          limits:
            max_yaml_bytes: 4096
            max_repo_bytes: 1048576
          success_event: repo_scaffold.repo_commit_succeeded
          success_payload:
            result_kind:
              literal: ready
          failure_event: repo_scaffold.repo_commit_failed
          failure_payload:
            result_kind:
              literal: failed
            request_copy:
              ref: payload.request_id
    repo_scaffold.repo_commit_succeeded:
      sets_gate: result_callback_observed
    repo_scaffold.repo_commit_failed:
      sets_gate: result_callback_observed
`,
	}
}

func artifactActionResultStaticDeliveryFixtureFiles() map[string]string {
	files := artifactActionResultDeliveryFixtureFiles()
	files["repo-scaffold/schema.yaml"] = strings.Replace(files["repo-scaffold/schema.yaml"], "mode: template", "mode: static", 1)
	return files
}
