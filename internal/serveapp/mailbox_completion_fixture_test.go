package serveapp

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/store"
)

func newMailboxCompletionFixture(t *testing.T, backend servedparity.Backend) cursorMailboxFixture {
	t.Helper()
	root := canonicalrouting.CopyMailboxCompletionMatrix(t)
	opts := cliapp.ServeOptions{SourceRoot: root, PlatformSpecPath: defaultPlatformSpecPath, APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0", SelfCheck: true, Verbose: true, TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig()}
	name := "sqlite"
	if backend == servedparity.BackendDefaultSQLite {
		unsetStoreSelectorEnv(t)
		stubServeRuntimeWorkspaceLifecycle(t)
		opts.ConfigPath = writeMockAgentRuntimeConfig(t, name, filepath.Join(t.TempDir(), "completion.sqlite"))
	} else {
		name = "postgres"
		installServeRuntimeEmptyPostgresTestStores(t, func() cliapp.ServeWorkspaceLifecycle { return serveRuntimeWorkspaceStub{} })
		opts.ConfigPath = writeMockAgentRuntimeConfig(t, name, "")
		opts.StoreMode, opts.StoreModeSet = name, true
	}
	var db *sql.DB
	var pg *store.PostgresStore
	var sq *store.SQLiteRuntimeStore
	captureSelectedRuntimePersistence(t, func(p serveRuntimePersistence) { db, pg, sq = selectedRuntimeStoreForTest(t, p) })
	endpoint, runtime := startOwnedMockLifecycleFollowUpRuntime(t, opts)
	rt := servedControlProofRuntime{Endpoint: endpoint, DB: db, Backend: name, Runtime: runtime, Postgres: pg, SQLite: sq, BundleHash: runtime.Options.SourceArtifactFact.BundleHash()}
	seed := requireServedEventPublishRPCResult(t, endpoint, map[string]any{"event_name": "work.requested", "bundle_hash": rt.BundleHash, "payload": map[string]any{"seed": true}, "idempotency_key": "matrix-seed"})
	requireServedEventPublishEntityState(t, db, name, seed.RunID, "", "review")
	waitServedRunDeliveryQuiescence(t, db, name, seed.RunID)
	var owner cursorMailboxStore = sq
	if pg != nil {
		owner = pg
	}
	var id string
	if err := db.QueryRow(`SELECT card_id FROM decision_cards WHERE run_id=$1 AND status='pending'`, seed.RunID).Scan(&id); err != nil {
		t.Fatal(err)
	}
	ctx := servedControlProofAuthorActivityContext(t, rt)
	base, err := owner.GetDecisionCard(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	return cursorMailboxFixture{rt: rt, store: owner, ctx: ctx, base: base, eventID: seed.EventID}
}

func mailboxCompletionAnchorCard(t *testing.T, f cursorMailboxFixture, kind decisioncard.AnchorKind) decisioncard.Card {
	t.Helper()
	if kind == decisioncard.AnchorKindStageGate {
		return f.base
	}
	if kind == decisioncard.AnchorKindProposedEffect {
		requireServedEventPublishRPCResult(t, f.rt.Endpoint, map[string]any{"event_name": "effect.requested", "run_id": f.base.RunID, "source_event_id": f.eventID, "payload": map[string]any{"seed": true}, "idempotency_key": "effect-seed"})
		var id string
		deadline := time.Now().Add(10 * time.Second)
		for {
			err := f.rt.DB.QueryRow(`SELECT card_id FROM decision_cards WHERE run_id=$1 AND anchor_kind='proposed_effect'`, f.base.RunID).Scan(&id)
			if err == nil {
				break
			}
			if err != sql.ErrNoRows || time.Now().After(deadline) {
				t.Fatal(err)
			}
			time.Sleep(10 * time.Millisecond)
		}
		waitServedRunDeliveryQuiescence(t, f.rt.DB, f.rt.Backend, f.base.RunID)
		card, err := f.store.GetDecisionCard(f.ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		return card
	}
	if kind != decisioncard.AnchorKindHumanTask {
		t.Fatalf("unknown anchor %s", kind)
	}
	requireServedEventPublishRPCResult(t, f.rt.Endpoint, map[string]any{"event_name": "observers/observer.requested", "run_id": f.base.RunID, "source_event_id": f.eventID, "payload": map[string]any{"seed": true}, "idempotency_key": "observer-seed"})
	waitServedRunDeliveryQuiescence(t, f.rt.DB, f.rt.Backend, f.base.RunID)
	var id string
	if err := f.rt.DB.QueryRow(`SELECT card_id FROM decision_cards WHERE run_id=$1 AND anchor_kind='human_task'`, f.base.RunID).Scan(&id); err != nil {
		t.Fatalf("ask_human did not create a card: %v\n%s", err, servedEventPublishDebugSummary(t, f.rt.DB, f.rt.Backend, f.base.RunID))
	}
	card, err := f.store.GetDecisionCard(f.ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	return card
}
