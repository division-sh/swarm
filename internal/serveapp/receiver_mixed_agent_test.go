package serveapp

import (
	"database/sql"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/backendselection"
)

func TestReceiverCompositionMixedAgentBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			root := canonicalrouting.CopyReceiverMixedAgent(t)
			opts := cliapp.ServeOptions{SourceRoot: root, PlatformSpecPath: defaultPlatformSpecPath, APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0", SelfCheck: true, Verbose: true, TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig()}
			var db *sql.DB
			if backend == "sqlite" {
				unsetStoreSelectorEnv(t)
				stubServeRuntimeWorkspaceLifecycle(t)
				opts.ConfigPath = writeMockAgentRuntimeConfig(t, "sqlite", filepath.Join(t.TempDir(), "receiver.sqlite"))
				captureSelectedRuntimePersistence(t, func(p serveRuntimePersistence) { db, _, _ = selectedRuntimeStoreForTest(t, p) })
			} else {
				_, selected, _ := installServeRuntimeEmptyPostgresTestStores(t, func() cliapp.ServeWorkspaceLifecycle { return serveRuntimeWorkspaceStub{} })
				db = selected
				opts.ConfigPath = writeMockAgentRuntimeConfig(t, "postgres", "")
				opts.StoreMode, opts.StoreModeSet = backendselection.BackendPostgres.String(), true
			}
			endpoint, runtime := startServedEventPublishFollowUpRuntime(t, opts)
			bundle := servedEventPublishFixtureBundleHash(t, root)
			seed := requireServedEventPublishRPCResult(t, endpoint, map[string]any{"event_name": "work.seeded", "bundle_hash": bundle, "payload": map[string]any{"seed": true}, "idempotency_key": "mixed-agent-seed"})
			requireServedEventPublishEntityState(t, db, backend, seed.RunID, flowidentity.EntityID("sink"), "active")
			waitServedRunDeliveryQuiescence(t, db, backend, seed.RunID)
			var childID, parentID string
			if err := db.QueryRow(`SELECT entity_id FROM entity_state WHERE run_id=$1 AND flow_instance='sink'`, seed.RunID).Scan(&childID); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRow(`SELECT entity_id FROM entity_state WHERE run_id=$1 AND flow_instance<>'sink'`, seed.RunID).Scan(&parentID); err != nil {
				t.Fatal(err)
			}
			if childID == parentID || childID == "" {
				t.Fatal("seed did not establish distinct receiving ownership")
			}
			published := requireServedEventPublishRPCResult(t, endpoint, map[string]any{"event_name": "work.requested", "run_id": seed.RunID, "source_event_id": seed.EventID, "payload": map[string]any{"seed": true}, "idempotency_key": "mixed-agent"})
			waitServedRunDeliveryQuiescence(t, db, backend, published.RunID)
			var agents, nodes, turns int
			if err := db.QueryRow(`SELECT count(*) FROM event_deliveries WHERE run_id=$1 AND subscriber_type='agent' AND status='delivered'`, published.RunID).Scan(&agents); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRow(`SELECT count(*) FROM event_deliveries WHERE run_id=$1 AND subscriber_type='node' AND status='delivered'`, published.RunID).Scan(&nodes); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRow(`SELECT count(*) FROM agent_turns WHERE run_id=$1 AND execution_mode='mock'`, published.RunID).Scan(&turns); err != nil {
				t.Fatal(err)
			}
			if agents != 1 || nodes != 4 || turns != 1 {
				t.Fatalf("mixed real execution agents/nodes/turns=%d/%d/%d", agents, nodes, turns)
			}
			rows, err := db.Query(`SELECT CAST(d.delivery_target_route AS TEXT) FROM event_deliveries d JOIN events e ON e.event_id=d.event_id WHERE d.run_id=$1 AND e.event_name='work.completed'`, published.RunID)
			if err != nil {
				t.Fatal(err)
			}
			owners := 0
			for rows.Next() {
				var raw string
				var owner events.DeliveryTargetOwnership
				if err := rows.Scan(&raw); err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal([]byte(raw), &owner); err != nil {
					t.Fatal(err)
				}
				if owner.Route().FlowID != "sink" || owner.Route().EntityID != childID || owner.Code() != "existing_entity" {
					t.Fatalf("mixed recipient did not preserve actual child ownership: %s", raw)
				}
				owners++
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			rows.Close()
			if owners != 2 {
				t.Fatalf("mixed event has %d owners, want node and agent", owners)
			}
			requireReceiverPublicReadback(t, servedControlProofRuntime{Endpoint: endpoint, DB: db, Backend: backend, Runtime: runtime, BundleHash: bundle}, published.RunID)
		})
	}
}
