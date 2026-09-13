package serveapp

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/servedparity"
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
							restore := func() {
								if _, err := rt.DB.Exec(`UPDATE runs SET bundle_hash=$1 WHERE run_id=$2`, rt.BundleHash, f.base.RunID); err != nil {
									t.Fatal(err)
								}
							}
							t.Cleanup(restore)
							cut := &mailboxPostCommitFault{beforeCommit: func(ctx context.Context, _ runtimepipeline.DecisionCardMutationCommand) error {
								result, err := rt.DB.ExecContext(ctx, `UPDATE runs SET bundle_hash=$1 WHERE run_id=$2 AND bundle_hash=$3`, unavailable, f.base.RunID, rt.BundleHash)
								if err != nil {
									return err
								}
								if n, err := result.RowsAffected(); err != nil || n != 1 {
									t.Fatalf("source race missed exact run: rows=%d err=%v", n, err)
								}
								return nil
							}}
							mutation, err := mailboxCardMutation(req, params)
							if err != nil {
								t.Fatal(err)
							}
							_, replayed, err := mailboxFaultCoordinator(t, rt, cut).CommitDecisionCardMutation(f.ctx, req, mutation)
							var missing *runlifecycle.SourceArtifactUnavailableError
							if !errors.As(err, &missing) || missing.BundleHash != unavailable || replayed {
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
						waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, f.base.RunID)
						before := mailboxCompletionRunEffects(t, rt, f.base.RunID)
						for _, ctx := range []context.Context{context.Background(), correlation.WithSourceArtifactFact(f.ctx, mustServeTestPersistedSourceArtifactFact(unavailable))} {
							_, replayed, err := mailboxTokenlessMutation(ctx, rt, owner, req, params)
							if err == nil || replayed || !strings.Contains(strings.ToLower(err.Error()), "source") {
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
