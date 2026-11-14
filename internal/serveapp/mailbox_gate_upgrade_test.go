package serveapp

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/tests/support/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

// The served mock lifecycle uses real authenticated API/CLI and native stores;
// it is not a standalone binary, provider or physical-channel proof.
func TestMailboxUnchangedGateUpgradeAnswerBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		for _, transport := range []string{"api", "cli"} {
			t.Run(string(backend)+"/"+transport, func(t *testing.T) {
				root := canonicalrouting.CopyMailboxNoticeCompletion(t)
				rt, owner, restart := newRetainedMailboxCompletionRuntime(t, backend, root)
				f := mailboxCompletionFixtureInRuntime(t, rt, owner)
				until := time.Now().UTC().Add(time.Hour).Truncate(time.Second)
				var deferred, draft map[string]any
				requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.defer", map[string]any{"card_id": f.base.CardID, "until": until.Format(time.RFC3339), "idempotency_key": uuid.NewString()}, &deferred)
				requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.begin_input", map[string]any{"card_id": f.base.CardID, "verdict": "reject", "observed_content_hash": f.base.CardContentHash, "idempotency_key": uuid.NewString()}, &draft)
				before, err := owner.GetDecisionCard(f.ctx, f.base.CardID)
				if err != nil || !before.DeferredUntil.Equal(until) || draft["input_draft_id"] == nil {
					t.Fatalf("pending card setup: %+v draft=%v err=%v", before, draft, err)
				}
				schema := filepath.Join(root, "schema.yaml")
				body, err := os.ReadFile(schema)
				if err != nil {
					t.Fatal(err)
				}
				upgrade := func() {
					t.Helper()
					body = append(body, []byte("\n# immutable source changes; offered contract does not\n")...)
					if err := os.WriteFile(schema, body, 0o600); err != nil {
						t.Fatal(err)
					}
					previous := rt.BundleHash
					rt, owner = restart()
					if rt.BundleHash == previous {
						t.Fatal("restart did not admit the new source")
					}
					var header struct {
						Run struct {
							BundleHash string `json:"bundle_hash"`
						} `json:"run"`
					}
					requireServedJSONRPCResult(t, rt.Endpoint, "run.get", map[string]any{"run_id": f.base.RunID}, &header)
					if header.Run.BundleHash != rt.BundleHash {
						t.Fatal("retained run did not switch to current execution source")
					}
				}
				upgrade()
				ctx := servedControlProofAuthorActivityContext(t, rt)
				after, err := owner.GetDecisionCard(ctx, f.base.CardID)
				if err != nil || !reflect.DeepEqual(before, after) {
					t.Fatalf("equal upgrade changed issuance or defer: %v", err)
				}
				var canceled map[string]any
				requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.cancel_input", map[string]any{"card_id": f.base.CardID, "input_draft_id": draft["input_draft_id"], "idempotency_key": uuid.NewString()}, &canceled)
				params := map[string]any{"card_id": f.base.CardID, "verdict": "approve", "observed_content_hash": before.CardContentHash, "idempotency_key": uuid.NewString()}
				config := writeMockAgentRuntimeConfig(t, "sqlite", filepath.Join(t.TempDir(), "unused.sqlite"))
				tokenFile := filepath.Join(t.TempDir(), "api-token")
				if err := os.WriteFile(tokenFile, []byte(apiv1.DefaultLoopbackAPIToken), 0o600); err != nil {
					t.Fatal(err)
				}
				answer := func() {
					t.Helper()
					if transport == "api" {
						var result map[string]any
						requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.decide", params, &result)
						return
					}
					out, errOut := &lockedBuffer{}, &lockedBuffer{}
					code := executeCLI(t.Context(), []string{"--config", config, "mailbox", "decide", before.CardID, "approve", "--idempotency-key", params["idempotency_key"].(string), "--api-server", rt.Endpoint, "--api-token-file", tokenFile}, out, errOut, nil)
					if code != 0 {
						t.Fatalf("current-run CLI answer exit=%d out=%s err=%s", code, out.String(), errOut.String())
					}
				}
				answer()
				requireServedEventPublishEntityState(t, rt.DB, rt.Backend, before.RunID, "", "done")
				waitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, before.RunID)
				decided, err := owner.GetDecisionCard(ctx, before.CardID)
				if err != nil || decided.Status != decisioncard.StatusDecided || decided.DecisionEventID == "" || decided.BundleHash != before.BundleHash || decided.CardContentHash != before.CardContentHash || !reflect.DeepEqual(decided.Snapshot, before.Snapshot) {
					t.Fatalf("current execution rewrote issuance or failed continuation: %+v %v", decided, err)
				}
				upgrade()
				answer()
				var count int
				if err := rt.DB.QueryRow(`SELECT count(*) FROM events WHERE run_id=$1 AND event_name='mailbox.card_decided'`, before.RunID).Scan(&count); err != nil || count != 1 {
					t.Fatalf("exact replay published another answer: count=%d err=%v", count, err)
				}
				params["idempotency_key"] = uuid.NewString()
				if result := requestServedJSONRPC(t, rt.Endpoint, "mailbox.decide", params); result.Error == nil {
					t.Fatal("fresh request answered the completed wait")
				}
				rt.joinAndAssertExit()
			})
		}
	}
}
