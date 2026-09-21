package runtimepersistence

import (
	"database/sql"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/division-sh/swarm/internal/testutil"
)

func TestAgentReceiverConfigNativeNamespacesBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var selected agentfixture.Store
			var db *sql.DB
			if backend == "sqlite" {
				store := newBootstrappedSQLiteRuntimeStoreForTest(t)
				selected, db = store, store.backend.ConstructionHandle()
			} else {
				_, db, _ = testutil.StartPostgres(t)
				selected = newTestPostgresStore(t, db)
			}
			ctx := testAuthorActivityContext()
			cfg := withRuntimePersistenceTestIntent(t, runtimeactors.AgentConfig{
				ID: "receiver-storage-proof", Identity: testAgentIdentity(t, "receiver-storage-proof", "review/item"),
				ExecutionMode: "live", Role: "worker", Type: "worker", Model: "regular", LLMBackend: "claude_cli",
				Memory: agentmemory.Authored(true), FlowPath: "review/item",
				Config:         json.RawMessage(`{"opaque":[7,7.0,null]}`),
				ReceiverConfig: json.RawMessage(`{"flow_path":"business-path","model":"business-model","mode":"business-mode","constraints":{"memory":"business-memory"},"nested":{"archived_record":{"system_prompt":"business"}},"list":[{"system_prompt":"business"},7,7.0,null]}`),
			})
			if err := agentfixture.UpsertStatic(t, ctx, selected, runtimemanager.PersistedAgent{Config: cfg, Status: "active"}); err != nil {
				t.Fatal(err)
			}
			assertJSON := func(got, want []byte) {
				t.Helper()
				var gotValue, wantValue any
				if err := canonicaljson.DecodePreservingNumberLexemes(got, &gotValue); err != nil {
					t.Fatal(err)
				}
				if err := canonicaljson.DecodePreservingNumberLexemes(want, &wantValue); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(gotValue, wantValue) {
					t.Fatalf("JSON identity changed: got=%s want=%s", got, want)
				}
			}
			for read := 0; read < 2; read++ {
				agents, err := selected.LoadAgents(ctx)
				if err != nil || len(agents) != 1 {
					t.Fatalf("read %d: agents=%d err=%v", read, len(agents), err)
				}
				got := agents[0].Config
				assertJSON(got.Config, cfg.Config)
				assertJSON(got.ReceiverConfig, cfg.ReceiverConfig)
				if got.Model != cfg.Model || got.Memory != cfg.Memory || got.FlowPath != cfg.FlowPath || !reflect.DeepEqual(got.Intent, cfg.Intent) || !got.Prompt.Empty() {
					t.Fatalf("read %d changed runtime authority: %+v", read, got)
				}
				got.Config[0], got.ReceiverConfig[0] = '[', '['
			}
			var stored []byte
			if err := db.QueryRowContext(ctx, `SELECT config FROM agents WHERE agent_id=$1`, cfg.ID).Scan(&stored); err != nil {
				t.Fatal(err)
			}
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(stored, &fields); err != nil {
				t.Fatal(err)
			}
			if len(fields) != 2 {
				t.Fatalf("config envelope keys=%v", fields)
			}
			assertJSON(fields["config"], cfg.Config)
			assertJSON(fields["receiver_config"], cfg.ReceiverConfig)
			for _, tc := range []struct{ name, raw, want string }{
				{"flattened", `{"flow_path":"business"}`, "requires exactly config and receiver_config"},
				{"unknown", `{"config":{},"receiver_config":null,"extra":true}`, "requires exactly config and receiver_config"},
				{"missing_receiver", `{"config":{}}`, "requires exactly config and receiver_config"},
				{"receiver_array", `{"config":{},"receiver_config":[]}`, "receiver_config must be a JSON object or null"},
				{"authority_model", `{"config":{"model":"override"},"receiver_config":{}}`, "config contains runtime-owned keys: model"},
				{"authority_prompt", `{"config":{"nested":[{"system_prompt":"override"}]},"receiver_config":{}}`, "RETIRED: authored config.nested[0].system_prompt"},
			} {
				t.Run(tc.name, func(t *testing.T) {
					if _, err := db.ExecContext(ctx, `UPDATE agents SET config=$1 WHERE agent_id=$2`, tc.raw, cfg.ID); err != nil {
						t.Fatal(err)
					}
					if _, err := selected.LoadAgents(ctx); err == nil || !strings.Contains(err.Error(), tc.want) {
						t.Fatalf("read corrupt envelope: err=%v want=%q", err, tc.want)
					}
				})
			}
		})
	}
}
