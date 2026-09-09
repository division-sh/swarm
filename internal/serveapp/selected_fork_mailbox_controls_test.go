package serveapp

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
	"gopkg.in/yaml.v3"
)

func TestSelectedForkMailboxControlRefusalsBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			root := canonicalrouting.CopyForkReceiverBusinessMutationOwnership(t, false)
			path := filepath.Join(root, "consumer", "schema.yaml")
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var schema map[string]any
			if err := yaml.Unmarshal(raw, &schema); err != nil {
				t.Fatal(err)
			}
			stages := schema["stages"].(map[string]any)
			stages["active"] = map[string]any{"gate": map[string]any{
				"decision": "review_receiver", "outcomes": map[string]any{"approve": map[string]any{"advances_to": "done"}},
			}}
			raw, err = yaml.Marshal(schema)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, raw, 0600); err != nil {
				t.Fatal(err)
			}
			rt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, root)
			seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
				"event_name": "start.seeded", "bundle_hash": rt.BundleHash,
				"payload": map[string]any{"token": "receiver-proof"}, "idempotency_key": "selected-card-seed",
			})
			waitForkReceiverSourceCompletion(t, rt, seed.RunID)
			requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
				"event_name": "start.requested", "run_id": seed.RunID, "source_event_id": seed.EventID,
				"payload": map[string]any{"token": "receiver-proof"}, "idempotency_key": "selected-card-request",
			})
			waitForkReceiverSourceCompletion(t, rt, seed.RunID)
			var frontier, sourceCard string
			if err := rt.DB.QueryRow(`SELECT event_id FROM events WHERE run_id=$1 AND event_name='producer/work.ready'`, seed.RunID).Scan(&frontier); err != nil {
				t.Fatal(err)
			}
			if err := rt.DB.QueryRow(`SELECT card_id FROM decision_cards WHERE run_id=$1`, seed.RunID).Scan(&sourceCard); err != nil {
				t.Fatal(err)
			}
			var fork apiv1.RunForkExecutionResult
			requireServedJSONRPCResult(t, rt.Endpoint, "run.fork", map[string]any{
				"source_run_id": seed.RunID, "fork_event_id": frontier, "allow_source_freeze": true,
				"idempotency_key": "selected-card-fork",
			}, &fork)
			var cardID, contentHash, bindingID string
			if err := rt.DB.QueryRow(`SELECT card_id,card_content_hash FROM decision_cards WHERE run_id=$1`, fork.ForkRunID).Scan(&cardID, &contentHash); err != nil || cardID == sourceCard {
				t.Fatalf("fork did not own its distinct decision card: card=%q err=%v", cardID, err)
			}
			if err := rt.DB.QueryRow(`SELECT binding_id FROM run_fork_selected_contract_bindings WHERE fork_run_id=$1`, fork.ForkRunID).Scan(&bindingID); err != nil {
				t.Fatal(err)
			}
			for _, phase := range []string{"active", "retired"} {
				if phase == "retired" {
					// Public retirement joins execution and candidate maintenance. Only
					// then is a whole-database comparison attributable to this request.
					var stopped map[string]any
					requireServedJSONRPCResult(t, rt.Endpoint, "run.stop", map[string]any{"run_id": fork.ForkRunID}, &stopped)
				}
				for _, operation := range []struct {
					name   string
					params map[string]any
				}{
					{"mailbox.decide", map[string]any{"verdict": "approve", "observed_content_hash": contentHash, "fields": map[string]any{}}},
					{"mailbox.defer", map[string]any{"until": time.Now().UTC().Add(time.Hour).Format(time.RFC3339Nano)}},
					{"mailbox.begin_input", map[string]any{"verdict": "approve", "observed_content_hash": contentHash}},
					{"mailbox.cancel_input", map[string]any{"input_draft_id": "not-admitted"}},
				} {
					t.Run(phase+"/"+operation.name, func(t *testing.T) {
						operation.params["card_id"] = cardID
						operation.params["idempotency_key"] = phase + operation.name + "-selected-refusal"
						before := snapshotForkReceiverApplication(t, rt)
						refusal := requireServedJSONRPCError(t, rt.Endpoint, operation.name, operation.params)
						details, ok := refusal.Data["details"].(map[string]any)
						if refusal.Data["code"] != "SELECTED_FORK_CONTROL_UNSUPPORTED" || !ok || details["run_id"] != fork.ForkRunID || details["binding_id"] != bindingID || details["operation"] != operation.name {
							t.Fatalf("mailbox control did not reject the exact selected binding: %+v", refusal)
						}
						after := snapshotForkReceiverApplication(t, rt)
						if phase == "retired" && !reflect.DeepEqual(before, after) {
							for table, rows := range before {
								if !reflect.DeepEqual(rows, after[table]) {
									t.Errorf("refused %s changed %s: before=%v after=%v", operation.name, table, rows, after[table])
								}
							}
							t.Fatal("refused selected mailbox control changed persisted state")
						}
					})
				}
			}
		})
	}
}
