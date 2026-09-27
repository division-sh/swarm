package serveapp

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/google/uuid"
)

func TestR2NamedRecordEmitSurvivesServedRestartBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			root := semanticNumericIngressFixture(t)
			for file, replacement := range map[string]struct{ old, next string }{
				"types.yaml":  {"    fraction: numeric\n", "    fraction: numeric\n  Report:\n    name: text\n    count: integer\n"},
				"events.yaml": {"numeric.completed:\n", "numeric.completed:\n  report: Report\n"},
				"nodes.yaml":  {`          explicit_double: "${double(payload.value) + 1.0}"`, "          explicit_double: \"${double(payload.value) + 1.0}\"\n          report: {name: Ada, count: \"${payload.value}\"}"},
			} {
				path := filepath.Join(root, file)
				raw, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				updated := strings.Replace(string(raw), replacement.old, replacement.next, 1)
				if updated == string(raw) {
					t.Fatalf("%s fixture mutation did not match", file)
				}
				if err := os.WriteFile(path, []byte(updated), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			rt, restart := startSemanticNumericLiveRuntime(t, backend, root)
			published := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
				"bundle_hash": rt.BundleHash, "event_name": "numeric.requested", "idempotency_key": uuid.NewString(),
				"payload": map[string]any{"value": 7, "nested": map[string]any{"numbers": []any{7}, "fraction": 7.5}},
			})
			checkReport := func(runID string) {
				t.Helper()
				var raw string
				deadline := time.Now().Add(10 * time.Second)
				for time.Now().Before(deadline) {
					err := rt.DB.QueryRow(`SELECT CAST(payload AS TEXT) FROM events WHERE CAST(run_id AS TEXT)=$1 AND event_name='numeric.completed'`, runID).Scan(&raw)
					if err == nil {
						break
					}
					if err != sql.ErrNoRows {
						t.Fatal(err)
					}
					time.Sleep(10 * time.Millisecond)
				}
				if raw == "" {
					t.Fatal("numeric.completed was not persisted")
				}
				var payload map[string]any
				if err := canonicaljson.DecodePreservingNumberLexemes([]byte(raw), &payload); err != nil {
					t.Fatal(err)
				}
				projected, err := workflowexpr.ProjectCELValue(payload)
				if err != nil {
					t.Fatal(err)
				}
				report, ok := projected.(map[string]any)["report"].(map[string]any)
				if !ok || report["name"] != "Ada" || report["count"] != int64(7) || len(report) != 2 {
					t.Fatalf("persisted typed report = %#v", projected)
				}
			}
			checkReport(published.RunID)
			waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, published.RunID)
			rt = restart()
			checkReport(published.RunID)
			var completedEventID string
			if err := rt.DB.QueryRow(`SELECT event_id FROM events WHERE CAST(run_id AS TEXT)=$1 AND event_name='numeric.completed'`, published.RunID).Scan(&completedEventID); err != nil {
				t.Fatal(err)
			}
			fork := requireSelectedForkExecutionRPCResult(t, rt.Endpoint, map[string]any{
				"source_run_id": published.RunID, "fork_event_id": completedEventID,
				"bundle_hash":         rt.BundleHash,
				"allow_source_freeze": true, "idempotency_key": "r2-record-fork",
			})
			if fork.ForkRunID == "" || fork.ForkRunID == published.RunID {
				t.Fatalf("invalid typed-record fork: %+v", fork)
			}
			checkReport(fork.ForkRunID)
			waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, fork.ForkRunID)
			rt = restart()
			checkReport(fork.ForkRunID)
		})
	}
}
