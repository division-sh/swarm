package serveapp

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestServedSelectionProcessCutsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, cut := range []string{"select", "selected"} {
			t.Run(backend+"/"+cut, func(t *testing.T) {
				start, ready, _ := mailboxCompletionProcessHarnessWithSelectionCut(t, backend, canonicalrouting.CopySelectionRetry(t), cut)
				first, rt := start(true)
				seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "seed", "bundle_hash": rt.BundleHash, "payload": map[string]any{}, "idempotency_key": "seed"})
				waitServedRunDeliveryQuiescence(t, rt.DB, backend, seed.RunID)
				params := map[string]any{"event_name": "select", "run_id": seed.RunID, "source_event_id": seed.EventID, "payload": map[string]any{}, "idempotency_key": "selection-crash"}
				raw, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": "selection", "method": "event.publish", "params": params})
				if err != nil {
					t.Fatal(err)
				}
				request, err := http.NewRequest(http.MethodPost, rt.Endpoint, bytes.NewReader(raw))
				if err != nil {
					t.Fatal(err)
				}
				request.Header.Set("Content-Type", "application/json")
				request.Header.Set("Authorization", "Bearer "+apiv1.DefaultLoopbackAPIToken)
				responseDone := make(chan struct{})
				go func() {
					defer close(responseDone)
					response, err := (&http.Client{Timeout: 30 * time.Second}).Do(request)
					if err == nil {
						_, _ = io.Copy(io.Discard, response.Body)
						_ = response.Body.Close()
					}
				}()
				reached := make(chan error, 1)
				go func() { var b [1]byte; _, err := io.ReadFull(ready, b[:]); reached <- err }()
				select {
				case err := <-reached:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(20 * time.Second):
					t.Fatalf("node boundary %s not reached\n%s", cut, first.output.String())
				}
				var eventID, deliveryID, status string
				var version int64
				if err := rt.DB.QueryRow(`SELECT e.event_id,d.delivery_id,d.status,d.claim_version FROM events e JOIN event_deliveries d ON d.event_id=e.event_id WHERE e.run_id=$1 AND e.event_name='select'`, seed.RunID).Scan(&eventID, &deliveryID, &status, &version); err != nil {
					t.Fatal(err)
				}
				var facts, publications int
				if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM event_delivery_handler_rule_selections WHERE delivery_id=$1`, deliveryID).Scan(&facts); err != nil {
					t.Fatal(err)
				}
				if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name='selected'`, seed.RunID).Scan(&publications); err != nil {
					t.Fatal(err)
				}
				committed := cut == "selected"
				if committed {
					if status != "delivered" || facts != 1 || publications != 1 {
						t.Fatalf("postcommit cut did not prove atomic selection/publication: status=%s facts=%d events=%d", status, facts, publications)
					}
				} else if status != "in_progress" || facts != 0 || publications != 0 {
					t.Fatalf("preselection cut already persisted final effects: status=%s facts=%d events=%d", status, facts, publications)
				}
				assertServedSelectionTrace(t, rt, eventID, seed.RunID, committed)
				if err := first.kill(); err != nil || first.waitError() == nil {
					t.Fatalf("expected actual process death: %v", err)
				}
				<-responseDone
				second, recovered := start(false)
				assertServedSelectionTrace(t, recovered, eventID, seed.RunID, true)
				requireServedEventPublishEntityState(t, recovered.DB, backend, seed.RunID, "", "done")
				waitServedRunDeliveryQuiescence(t, recovered.DB, backend, seed.RunID)
				duplicate := requireServedEventPublishRPCResult(t, recovered.Endpoint, params)
				if duplicate.EventID != eventID || duplicate.RunID != seed.RunID {
					t.Fatal("process recovery replaced the admitted publication")
				}
				var recoveredVersion int64
				if err := recovered.DB.QueryRow(`SELECT claim_version FROM event_deliveries WHERE delivery_id=$1 AND status='delivered'`, deliveryID).Scan(&recoveredVersion); err != nil {
					t.Fatal(err)
				}
				if (committed && recoveredVersion != version) || (!committed && recoveredVersion <= version) {
					t.Fatalf("wrong recovery fence: committed=%v before=%d after=%d", committed, version, recoveredVersion)
				}
				for _, name := range []string{"selected", "ack"} {
					var count int
					if err := recovered.DB.QueryRow(`SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name=$2`, seed.RunID, name).Scan(&count); err != nil || count != 1 {
						t.Fatalf("recovery duplicated/lost %s: count=%d err=%v", name, count, err)
					}
				}
				if err := second.stop(); err != nil {
					t.Fatalf("recovered lifecycle failed to join: %v\n%s", err, second.output.String())
				}
			})
		}
	}
}
