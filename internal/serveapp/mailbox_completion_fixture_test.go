package serveapp

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/store"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func newMailboxCompletionFixture(t *testing.T, backend servedparity.Backend) cursorMailboxFixture {
	t.Helper()
	rt, owner := newMailboxCompletionRuntime(t, backend)
	return mailboxCompletionFixtureInRuntime(t, rt, owner)
}

func newMailboxCompletionRuntime(t *testing.T, backend servedparity.Backend) (servedControlProofRuntime, cursorMailboxStore) {
	t.Helper()
	root := canonicalrouting.CopyMailboxNoticeCompletion(t)
	return newMailboxCompletionRuntimeSource(t, backend, root)
}

func newMailboxCompletionRuntimeSource(t *testing.T, backend servedparity.Backend, root string) (servedControlProofRuntime, cursorMailboxStore) {
	rt, owner, _ := newRetainedMailboxCompletionRuntime(t, backend, root)
	return rt, owner
}

func newRetainedMailboxCompletionRuntime(t *testing.T, backend servedparity.Backend, root string, tokens ...string) (servedControlProofRuntime, cursorMailboxStore, func() (servedControlProofRuntime, cursorMailboxStore)) {
	return newRetainedMailboxCompletionRuntimeConfigured(t, backend, root, "", tokens...)
}

func newRetainedMailboxCompletionRuntimeConfigured(t *testing.T, backend servedparity.Backend, root, configSuffix string, tokens ...string) (servedControlProofRuntime, cursorMailboxStore, func() (servedControlProofRuntime, cursorMailboxStore)) {
	t.Helper()
	if len(tokens) == 0 {
		tokens = []string{apiv1.DefaultLoopbackAPIToken}
	}
	opts := cliapp.ServeOptions{SourceRoot: root, PlatformSpecPath: defaultPlatformSpecPath, APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0", SelfCheck: true, Verbose: true, TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig()}
	name := "sqlite"
	if backend == servedparity.BackendDefaultSQLite {
		unsetStoreSelectorEnv(t)
		stubServeRuntimeWorkspaceLifecycle(t)
		opts.ConfigPath = writeMockAgentRuntimeConfig(t, name, filepath.Join(t.TempDir(), "completion.sqlite"))
	} else {
		name = "postgres"
		stubServeRuntimeWorkspaceLifecycle(t)
		dsn, _, cleanup := testutil.StartPostgres(t)
		t.Cleanup(cleanup)
		original := buildStoresForServe
		buildStoresForServe = func(_ context.Context, _ storebackend.Selection, cfg *config.Config) (*selectedStoreOwner, error) {
			pg, err := store.NewPostgresStore(dsn)
			if err != nil {
				return nil, err
			}
			storetest.BootstrapPostgresRuntimeStore(t, pg)
			return openSelectedPostgresOwner(t, dsn, storetest.DatabaseForTest(pg), cfg), nil
		}
		t.Cleanup(func() { buildStoresForServe = original })
		opts.ConfigPath = writeMockAgentRuntimeConfig(t, name, "")
		opts.StoreMode, opts.StoreModeSet = name, true
	}
	if configSuffix != "" {
		raw, err := os.ReadFile(opts.ConfigPath)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(opts.ConfigPath, append(raw, []byte(configSuffix)...), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	var db *sql.DB
	var pg *store.PostgresStore
	var sq *store.SQLiteRuntimeStore
	captureSelectedRuntimePersistence(t, func(p serveRuntimePersistence) { db, pg, sq = selectedRuntimeStoreForTest(t, p) })
	retained := t.TempDir()
	var process *serveRuntimeTestProcess
	start := func() (servedControlProofRuntime, cursorMailboxStore) {
		if process != nil {
			if code := process.stop(); code != 0 {
				t.Fatalf("stop retained mailbox runtime: %d\n%s", code, process.outputString())
			}
		}
		t.Log("proof_surface=H in-process retained mock lifecycle with real authenticated HTTP/WS; not public serve/test")
		process = startRuntimeTestProcessWithRunner(t, repoRootForTest(), opts, func(ctx context.Context, repo string, opts cliapp.ServeOptions) int {
			code, err := runOwnedMockLifecycle(ctx, repo, retained, opts, apiv1.AuthTokenResolution{Tokens: tokens, Explicit: true, Source: "internal-lifecycle-parent"})
			if err != nil {
				fmt.Fprintf(opts.ErrorOutput, "internal mailbox lifecycle setup: %v\n", err)
			}
			return code
		})
		process.waitForReadyLine()
		runtime := servedTestProcessRuntime(t, process)
		endpoint := "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString()) + "/v1/rpc"
		rt := servedControlProofRuntime{Endpoint: endpoint, DB: db, Backend: name, Runtime: runtime, Postgres: pg, SQLite: sq, BundleHash: runtime.Options.SourceArtifactFact.BundleHash()}
		var owner cursorMailboxStore = sq
		if pg != nil {
			owner = pg
		}
		return rt, owner
	}
	rt, owner := start()
	return rt, owner, start
}

func mailboxCompletionFixtureInRuntime(t *testing.T, rt servedControlProofRuntime, owner cursorMailboxStore) cursorMailboxFixture {
	t.Helper()
	seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "work.requested", "bundle_hash": rt.BundleHash, "payload": map[string]any{"seed": true}, "idempotency_key": "matrix-seed-" + uuid.NewString()})
	requireServedEventPublishEntityState(t, rt.DB, rt.Backend, seed.RunID, "", "review")
	waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, seed.RunID)
	var id string
	if err := rt.DB.QueryRow(`SELECT card_id FROM decision_cards WHERE run_id=$1 AND status='pending'`, seed.RunID).Scan(&id); err != nil {
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
		requireServedEventPublishRPCResult(t, f.rt.Endpoint, map[string]any{"event_name": "effect.requested", "run_id": f.base.RunID, "source_event_id": f.eventID, "payload": map[string]any{"seed": true}, "idempotency_key": "effect-seed-" + f.base.RunID})
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
	requireServedEventPublishRPCResult(t, f.rt.Endpoint, map[string]any{"event_name": "observers/observer.requested", "run_id": f.base.RunID, "source_event_id": f.eventID, "payload": map[string]any{"seed": true}, "idempotency_key": "observer-seed-" + f.base.RunID})
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

func mailboxCompletionNotice(t *testing.T, f cursorMailboxFixture) string {
	t.Helper()
	seed := requireServedEventPublishRPCResult(t, f.rt.Endpoint, map[string]any{"event_name": "observers/notice.requested", "run_id": f.base.RunID, "source_event_id": f.eventID, "payload": map[string]any{"seed": true}, "idempotency_key": "notice-seed-" + uuid.NewString()})
	waitServedRunDeliveryQuiescence(t, f.rt.DB, f.rt.Backend, f.base.RunID)
	waitPublicationSiteCompletion(t, f.rt, f.base.RunID)
	var id, summary, from string
	if err := f.rt.DB.QueryRow(`SELECT item_id,summary,from_agent FROM mailbox WHERE source_event_id=$1 AND item_type=$2`, seed.EventID, runtimetools.NotifyHumanMailboxItemType).Scan(&id, &summary, &from); err != nil {
		t.Fatalf("real notify_human did not persist the notice: %v", err)
	}
	if summary != "Observed notice" || from != "observer" {
		t.Fatalf("wrong real notice producer: summary=%q from=%q", summary, from)
	}
	return id
}
