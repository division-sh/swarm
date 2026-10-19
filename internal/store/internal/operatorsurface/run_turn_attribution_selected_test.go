package operatorsurface

import (
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	privatedelivery "github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
	_ "modernc.org/sqlite"
)

// Invariant/reach: selected completion readback requires the exact retained
// execution, delivery and usage target, not a live lease or normal-agent token.
func TestCompletionAttributionRequiresExactSelectedOriginBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var db *sql.DB
			if backend == "postgres" {
				_, db, _ = testutil.StartEmptyPostgres(t)
			} else {
				var err error
				db, err = sql.Open("sqlite", filepath.Join(t.TempDir(), "selected-origin.db"))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = db.Close() })
			}
			idType := "TEXT"
			if backend == "postgres" {
				idType = "UUID"
			}
			for _, ddl := range []string{
				`CREATE TABLE runs (run_id TEXT PRIMARY KEY, bundle_hash TEXT)`,
				`CREATE TABLE run_fork_selected_contract_bindings (binding_id TEXT PRIMARY KEY, fork_run_id TEXT, mode TEXT, bundle_hash TEXT)`,
				`CREATE TABLE run_fork_selected_contract_runtime_executions (execution_id TEXT PRIMARY KEY, binding_id TEXT, fork_run_id TEXT, generation BIGINT, state TEXT, lease_expires_at TEXT)`,
				`CREATE TABLE events (event_id NATIVE_ID PRIMARY KEY, run_id TEXT, event_name TEXT, entity_id TEXT, flow_instance TEXT)`,
				`CREATE TABLE event_deliveries (delivery_id NATIVE_ID PRIMARY KEY, event_id NATIVE_ID, run_id TEXT, route_identity TEXT, subscriber_type TEXT, subscriber_id TEXT, agent_name_owner TEXT, agent_name_source TEXT, agent_route_presence TEXT, agent_flow_scope_key TEXT, agent_flow_instance_id TEXT, agent_flow_instance_path TEXT, delivery_target_route TEXT, delivery_context TEXT, delivery_payload_projection TEXT, connect_execution_claim TEXT, receiver_materialization_plan TEXT, execution_authority_kind TEXT, authority_bundle_hash TEXT, execution_authority_id TEXT, execution_authority_generation BIGINT, selected_execution_id TEXT, selected_fork_run_id TEXT, selected_execution_generation BIGINT, status TEXT, retry_count INTEGER, max_retries INTEGER, next_eligible_at TIMESTAMP, claim_version BIGINT, current_attempt_version BIGINT, reason_code TEXT, failure TEXT, started_at TIMESTAMP, settled_at TIMESTAMP, created_at TIMESTAMP, updated_at TIMESTAMP)`,
				`CREATE TABLE event_delivery_attempts (delivery_id NATIVE_ID, claim_version BIGINT, claim_token NATIVE_ID, open_marker BOOLEAN, lease_expires_at TIMESTAMP, active_session_id NATIVE_ID)`,
				`CREATE TABLE event_delivery_handler_rule_selections (delivery_id NATIVE_ID, selection_context TEXT, disposition TEXT, flow_path TEXT, declaration_family TEXT, semantic_path TEXT, display_label TEXT)`,
				`CREATE TABLE runtime_external_effect_operations (operation_id TEXT PRIMARY KEY, effect_kind TEXT, authority_kind TEXT, authority_id TEXT, agent_run_id TEXT, agent_id TEXT, agent_name_owner TEXT, agent_name_source TEXT, agent_route_presence TEXT, flow_scope_key TEXT, flow_instance_id TEXT, flow_instance TEXT, request_fingerprint TEXT, selected_execution_id TEXT, generation BIGINT, bundle_hash TEXT)`,
				`CREATE TABLE runtime_external_effect_attempts (attempt_id TEXT PRIMARY KEY, operation_id TEXT, adapter TEXT, state TEXT, completion_projection_phase TEXT, completion_continuation_active BOOLEAN, evidence TEXT, origin_kind TEXT, origin_delivery_id NATIVE_ID, origin_run_id TEXT, origin_route_identity TEXT, origin_subscriber_type TEXT, origin_subscriber_id TEXT, capability_surface_id TEXT, usage_target_kind TEXT, usage_target_id TEXT, target_ordinal INTEGER, authority_evidence TEXT, generation BIGINT, origin_claim_version BIGINT, origin_claim_token NATIVE_ID)`,
				`CREATE TABLE agent_turns (turn_id TEXT PRIMARY KEY, session_id TEXT, run_id TEXT, agent_id TEXT, agent_name_owner TEXT, agent_name_source TEXT, agent_route_presence TEXT, flow_scope_key TEXT, flow_instance_id TEXT, flow_instance TEXT, trigger_event_id TEXT, trigger_event_type TEXT, capability_surface_id TEXT, completion_attempt_id TEXT)`,
				`CREATE TABLE managed_agent_capability_surfaces (surface_id TEXT PRIMARY KEY, surface TEXT)`,
				`CREATE TABLE runtime_provider_attempt_drains (attempt_id TEXT)`,
			} {
				if _, err := db.Exec(strings.ReplaceAll(ddl, "NATIVE_ID", idType)); err != nil {
					t.Fatal(err)
				}
			}
			adapter, err := privatedelivery.NewAdapter(privatedelivery.Dialect(backend))
			if err != nil {
				t.Fatal(err)
			}
			runID, executionID, bindingID := uuid.NewString(), uuid.NewString(), uuid.NewString()
			turnID, sessionID, operationID, attemptID, eventID, deliveryID := uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()
			name, err := agentidentity.DeclaredName("worker", "swarm://proof/worker")
			if err != nil {
				t.Fatal(err)
			}
			route, err := agentidentity.PresentRoute("consumer", "one", "consumer/one")
			if err != nil {
				t.Fatal(err)
			}
			identity, err := agentidentity.New(runID, name, route)
			if err != nil {
				t.Fatal(err)
			}
			fields, err := identity.StorageFields()
			if err != nil {
				t.Fatal(err)
			}
			deliveryRoute := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(identity.AgentID()), AgentIdentity: identity}.Normalized()
			routeID, err := deliveryRoute.Identity()
			if err != nil {
				t.Fatal(err)
			}
			routeKey := events.EncodeDeliveryRouteIdentity(routeID)
			jsonText := func(value any) string {
				t.Helper()
				raw, err := json.Marshal(value)
				if err != nil {
					t.Fatal(err)
				}
				return string(raw)
			}
			materialization, err := events.EncodeReceiverMaterializationRecord(deliveryRoute)
			if err != nil {
				t.Fatal(err)
			}
			claimToken := uuid.NewString()
			surface, err := managedcapabilities.New(managedcapabilities.Plan{
				ActorIdentity: identity, RuntimeMode: "task", Provider: "test", Transport: "api", ProviderContract: "test.v1", CreatedAt: time.Unix(1, 0).UTC(),
				Authority: managedcapabilities.Authority{Kind: managedcapabilities.AuthorityProviderTurn, ID: turnID,
					ExecutionKind: managedcapabilities.ExecutionSelectedContractFork, ExecutionAuthorityID: executionID, RunID: runID, SessionID: sessionID, TurnOrdinal: 1},
			})
			if err != nil {
				t.Fatal(err)
			}
			surfaceJSON, err := json.Marshal(surface)
			if err != nil {
				t.Fatal(err)
			}
			authority := effects.Authority{Kind: effects.AuthoritySelectedContractFork, ID: executionID,
				SelectedFork: effects.SelectedContractForkAuthority{ExecutionID: executionID, ForkRunID: runID, Generation: 2},
				Target:       effects.UsageTarget{Kind: effects.UsageTargetAgentTurn, ID: turnID, RunID: runID, AgentID: identity.AgentID(), AgentIdentity: identity, SessionID: sessionID, FlowInstance: identity.FlowInstance()},
			}
			bundle := "bundle-v2:sha256:" + strings.Repeat("a", 64)
			for _, tc := range []struct {
				name         string
				update       string
				change       func(map[string]any)
				valid        bool
				ownerRefusal string
			}{
				{name: "closed_expired_execution", valid: true},
				{name: "pinned_bundle_binding", update: `UPDATE run_fork_selected_contract_bindings SET mode='bundle_hash',bundle_hash=(SELECT bundle_hash FROM runs)`, valid: true},
				{name: "operation_kind", update: `UPDATE runtime_external_effect_operations SET authority_kind='conversation_fork_chat'`},
				{name: "provider_kind", update: `UPDATE runtime_external_effect_operations SET effect_kind='http_request'`},
				{name: "operation_execution", update: `UPDATE runtime_external_effect_operations SET selected_execution_id='foreign'`},
				{name: "operation_authority", update: `UPDATE runtime_external_effect_operations SET authority_id='foreign'`},
				{name: "operation_generation", update: `UPDATE runtime_external_effect_operations SET generation=3`},
				{name: "attempt_generation", update: `UPDATE runtime_external_effect_attempts SET generation=3`},
				{name: "operation_bundle", update: `UPDATE runtime_external_effect_operations SET bundle_hash='foreign'`},
				{name: "delivery_kind", update: `UPDATE event_deliveries SET execution_authority_kind='normal_runtime'`},
				{name: "delivery_execution", update: `UPDATE event_deliveries SET execution_authority_id='foreign'`, ownerRefusal: "selected delivery authority projection mismatch"},
				{name: "delivery_generation", update: `UPDATE event_deliveries SET execution_authority_generation=3`, ownerRefusal: "selected delivery authority projection mismatch"},
				{name: "delivery_selected_execution", update: `UPDATE event_deliveries SET selected_execution_id='foreign'`, ownerRefusal: "selected delivery authority projection mismatch"},
				{name: "delivery_selected_generation", update: `UPDATE event_deliveries SET selected_execution_generation=3`, ownerRefusal: "selected delivery authority projection mismatch"},
				{name: "delivery_fork_run", update: `UPDATE event_deliveries SET selected_fork_run_id='foreign'`, ownerRefusal: "persisted delivery execution authority"},
				{name: "delivery_bundle", update: `UPDATE event_deliveries SET authority_bundle_hash='foreign'`, ownerRefusal: "persisted delivery execution authority"},
				{name: "stored_generation", update: `UPDATE run_fork_selected_contract_runtime_executions SET generation=3`},
				{name: "stored_fork_run", update: `UPDATE run_fork_selected_contract_runtime_executions SET fork_run_id='foreign'`},
				{name: "stored_binding", update: `UPDATE run_fork_selected_contract_runtime_executions SET binding_id='foreign'`},
				{name: "binding_fork_run", update: `UPDATE run_fork_selected_contract_bindings SET fork_run_id='foreign'`},
				{name: "binding_bundle", update: `UPDATE run_fork_selected_contract_bindings SET mode='bundle_hash',bundle_hash='foreign'`},
				{name: "run_bundle", update: `UPDATE runs SET bundle_hash='foreign'`},
				{name: "usage_kind", update: `UPDATE runtime_external_effect_attempts SET usage_target_kind='conversation_fork_turn_completion'`},
				{name: "usage_turn", update: `UPDATE runtime_external_effect_attempts SET usage_target_id='foreign'`},
				{name: "usage_ordinal", update: `UPDATE runtime_external_effect_attempts SET target_ordinal=1`},
				{name: "origin_run", update: `UPDATE runtime_external_effect_attempts SET origin_run_id='foreign'`},
				{name: "origin_route", update: `UPDATE runtime_external_effect_attempts SET origin_route_identity='foreign'`},
				{name: "origin_subscriber", update: `UPDATE runtime_external_effect_attempts SET origin_subscriber_id='foreign'`},
				{name: "origin_class", update: `UPDATE runtime_external_effect_attempts SET origin_subscriber_type='node'`},
				{name: "capability_pairing", update: `UPDATE runtime_external_effect_attempts SET capability_surface_id='foreign'`},
				{name: "trigger_event", update: `UPDATE agent_turns SET trigger_event_id='foreign'`},
				{name: "trigger_type", update: `UPDATE agent_turns SET trigger_event_type='foreign'`},
				{name: "recorded_kind", change: func(e map[string]any) { e["authority_kind"] = "normal_agent" }},
				{name: "recorded_authority", change: func(e map[string]any) { e["authority_id"] = uuid.NewString() }},
				{name: "recorded_execution", change: func(e map[string]any) { e["execution_id"] = uuid.NewString() }},
				{name: "recorded_fork_run", change: func(e map[string]any) { e["fork_run_id"] = uuid.NewString() }},
				{name: "recorded_generation", change: func(e map[string]any) { e["generation"] = uint64(3) }},
				{name: "target_kind", change: func(e map[string]any) {
					e["usage_target"].(map[string]any)["kind"] = "conversation_fork_turn_completion"
				}},
				{name: "target_turn", change: func(e map[string]any) { e["usage_target"].(map[string]any)["id"] = uuid.NewString() }},
				{name: "target_ordinal", change: func(e map[string]any) { e["usage_target"].(map[string]any)["ordinal"] = 1 }},
				{name: "target_run", change: func(e map[string]any) { e["usage_target"].(map[string]any)["run_id"] = uuid.NewString() }},
				{name: "target_session", change: func(e map[string]any) { e["usage_target"].(map[string]any)["session_id"] = uuid.NewString() }},
				{name: "target_agent", change: func(e map[string]any) { e["usage_target"].(map[string]any)["agent_id"] = "foreign" }},
				{name: "target_route", change: func(e map[string]any) { e["usage_target"].(map[string]any)["flow_instance"] = "foreign" }},
				{name: "target_full_identity", change: func(e map[string]any) {
					other := identity
					other.Name.Owner = "swarm://other/worker"
					e["usage_target"].(map[string]any)["agent_identity"] = other
				}},
				{name: "unknown_claim", update: `UPDATE event_delivery_attempts SET claim_version=2`},
				{name: "foreign_claim_token", update: `UPDATE event_delivery_attempts SET claim_token='00000000-0000-4000-8000-000000000001'`},
			} {
				t.Run(tc.name, func(t *testing.T) {
					tx, err := db.BeginTx(t.Context(), nil)
					if err != nil {
						t.Fatal(err)
					}
					defer tx.Rollback()
					exec := func(query string, args ...any) {
						t.Helper()
						if _, err := tx.ExecContext(t.Context(), query, args...); err != nil {
							t.Fatal(err)
						}
					}
					evidence := authority.Evidence()
					if tc.change != nil {
						tc.change(evidence)
					}
					raw, err := json.Marshal(evidence)
					if err != nil {
						t.Fatal(err)
					}
					exec(`INSERT INTO runs VALUES ($1,$2)`, runID, bundle)
					exec(`INSERT INTO run_fork_selected_contract_bindings VALUES ($1,$2,'selected_contracts','')`, bindingID, runID)
					exec(`INSERT INTO run_fork_selected_contract_runtime_executions VALUES ($1,$2,$3,2,'closed','2000-01-01T00:00:00Z')`, executionID, bindingID, runID)
					exec(`INSERT INTO events VALUES ($1,$2,'work.ready',NULL,NULL)`, eventID, runID)
					exec(`INSERT INTO event_deliveries (delivery_id,event_id,run_id,route_identity,subscriber_type,subscriber_id,agent_name_owner,agent_name_source,agent_route_presence,agent_flow_scope_key,agent_flow_instance_id,agent_flow_instance_path,delivery_target_route,delivery_context,delivery_payload_projection,connect_execution_claim,receiver_materialization_plan,execution_authority_kind,authority_bundle_hash,execution_authority_id,execution_authority_generation,selected_execution_id,selected_fork_run_id,selected_execution_generation,status,retry_count,max_retries,next_eligible_at,claim_version,current_attempt_version,created_at,updated_at)
						VALUES ($1,$2,$3,$4,'agent',$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,'selected_contract_fork',$17,$18,2,$18,$3,2,'pending',0,$19,$20,1,1,$20,$20)`,
						deliveryID, eventID, runID, routeKey, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath,
						jsonText(deliveryRoute.Target), jsonText(deliveryRoute.Context), jsonText(deliveryRoute.PayloadProjection), jsonText(deliveryRoute.ConnectClaim), string(materialization), bundle, executionID, deliverylifecycle.SubscriberAgent.MaxRetries(), time.Unix(1, 0).UTC())
					exec(`INSERT INTO event_delivery_attempts VALUES ($1,1,$2,FALSE,NULL,NULL)`, deliveryID, claimToken)
					exec(`INSERT INTO runtime_external_effect_operations (operation_id,effect_kind,authority_kind,authority_id,request_fingerprint,selected_execution_id,generation,bundle_hash) VALUES ($1,'provider_turn','selected_contract_fork',$2,'proof',$2,2,$3)`, operationID, executionID, bundle)
					exec(`INSERT INTO runtime_external_effect_attempts VALUES ($1,$2,'anthropic_api','response_observed','',TRUE,'{}','delivery',$3,$4,$5,'agent',$6,$7,'agent_turn',$8,0,$9,2,1,$10)`, attemptID, operationID, deliveryID, runID, routeKey, identity.AgentID(), surface.ID, turnID, string(raw), claimToken)
					exec(`INSERT INTO agent_turns VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'work.ready',$12,$13)`, turnID, sessionID, fields.RunID, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath, eventID, surface.ID, attemptID)
					exec(`INSERT INTO managed_agent_capability_surfaces VALUES ($1,$2)`, surface.ID, string(surfaceJSON))
					if tc.update != "" {
						exec(tc.update)
					}
					facts, err := adapter.RunOperationalEvidence(t.Context(), tx, runID)
					if tc.ownerRefusal != "" {
						if !errors.Is(err, deliverylifecycle.ErrConflict) || !strings.Contains(err.Error(), tc.ownerRefusal) || len(facts) != 0 {
							t.Fatalf("canonical delivery owner did not refuse contradictory evidence: %+v / %v", facts, err)
						}
						return
					}
					if err != nil || len(facts) != 1 {
						t.Fatalf("canonical delivery evidence: %+v / %v", facts, err)
					}
					out := operatorread.RunTurnAttribution{EventType: "work.ready"}
					_, _, _, err = loadDeliveryCompletionEvidence(t.Context(), tx, facts[0], &out)
					if tc.valid {
						if err != nil || len(out.ProviderTurns) != 1 || out.ProviderTurns[0] != turnID {
							t.Fatalf("exact historical selected origin lost: %+v / %v", out, err)
						}
					} else if err == nil || !strings.Contains(err.Error(), "completion evidence contradicts exact delivery origin") || len(out.ProviderTurns) != 0 {
						t.Fatalf("contradictory origin was not refused before attribution: %+v / %v", out, err)
					}
				})
			}
		})
	}
}
