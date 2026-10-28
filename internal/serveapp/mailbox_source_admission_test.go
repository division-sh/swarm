package serveapp

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func TestMailboxSourceAdmissionBeforeReplayAndAtCommitBothStores(t *testing.T) {
	const unavailable = "bundle-v2:sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			rt, owner := newMailboxCompletionRuntime(t, backend)
			for _, kind := range []decisioncard.AnchorKind{decisioncard.AnchorKindStageGate, decisioncard.AnchorKindHumanTask, decisioncard.AnchorKindProposedEffect, "notice"} {
				methods := []string{"mailbox.decide", "mailbox.defer", "mailbox.begin_input", "mailbox.cancel_input"}
				if kind == "notice" {
					methods = []string{"mailbox.acknowledge"}
				}
				for _, method := range methods {
					t.Run(string(kind)+"/"+method, func(t *testing.T) {
						f := mailboxCompletionFixtureInRuntime(t, rt, owner)
						params := mailboxPrincipalMutationParams(t, f, kind, method)
						req := mailboxPrincipalRequest(t, rt, method, params)
						if kind != "notice" {
							before := mailboxCompletionRunEffects(t, rt, f.base.RunID)
							installed := false
							restore := func() {
								if installed {
									if err := storetest.SwapMailboxRunSource(context.Background(), selectedMailboxFixtureStore(rt), f.base.RunID, unavailable, rt.BundleHash); err != nil {
										t.Error(err)
										return
									}
									installed = false
								}
							}
							t.Cleanup(restore)
							cut := &mailboxPostCommitFault{beforeCommit: func(ctx context.Context, _ runtimepipeline.DecisionCardMutationCommand) error {
								err := storetest.SwapMailboxRunSource(ctx, selectedMailboxFixtureStore(rt), f.base.RunID, rt.BundleHash, unavailable)
								if err != nil {
									return err
								}
								installed = true
								return nil
							}}
							mutation, err := mailboxCardMutation(req, params)
							if err != nil {
								t.Fatal(err)
							}
							_, replayed, err := mailboxFaultCoordinator(t, rt, cut).CommitDecisionCardMutation(f.ctx, req, mutation)
							if err == nil || err.Error() != "rooted execution context does not match the exact run source root" || replayed {
								t.Fatalf("source changed after acquisition was not rejected at commit: replay=%t err=%v", replayed, err)
							}
							restore()
							if after := mailboxCompletionRunEffects(t, rt, f.base.RunID); !reflect.DeepEqual(before, after) {
								t.Fatal("source race partially committed domain state")
							}
							var completions int
							if err := rt.DB.QueryRow(`SELECT count(*) FROM api_idempotency WHERE idempotency_key=$1`, req.IdempotencyKey).Scan(&completions); err != nil || completions != 0 {
								t.Fatalf("source race committed completion: count=%d err=%v", completions, err)
							}
						}
						var result map[string]any
						requireServedJSONRPCResult(t, rt.Endpoint, method, params, &result)
						waitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, f.base.RunID)
						before := mailboxCompletionRunEffects(t, rt, f.base.RunID)
						foreignCause := "rooted context does not match its content fact"
						if kind == "notice" {
							foreignCause = "source"
						}
						for _, rejected := range []struct {
							ctx   context.Context
							cause string
						}{
							{context.Background(), "source"},
							{correlation.WithSourceArtifactFact(f.ctx, mustServeTestPersistedSourceArtifactFact(unavailable)), foreignCause},
						} {
							_, replayed, err := mailboxTokenlessMutation(rejected.ctx, rt, owner, req, params)
							if err == nil || replayed || !strings.Contains(strings.ToLower(err.Error()), rejected.cause) {
								t.Fatalf("completion bypassed missing/foreign source admission: replay=%t err=%v", replayed, err)
							}
						}
						_, replayed, err := mailboxTokenlessMutation(f.ctx, rt, owner, req, params)
						if err != nil || !replayed {
							t.Fatalf("source-positive control lost completion: replay=%t err=%v", replayed, err)
						}
						if after := mailboxCompletionRunEffects(t, rt, f.base.RunID); !reflect.DeepEqual(before, after) {
							t.Fatal("source refusal/replay changed committed domain state")
						}
					})
				}
			}
		})
	}
}
