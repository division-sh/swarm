package serveapp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

// Native selected-owner projection; this is not channel/provider execution.
func TestMailboxGateReplayProjectionBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			captured := captureSelectedResetSupervisor(t)
			rt, owner := newMailboxCompletionRuntime(t, backend)
			supervisor := <-captured
			f := mailboxCompletionFixtureInRuntime(t, rt, owner)
			other := mailboxCompletionFixtureInRuntime(t, rt, owner)
			params := map[string]any{"card_id": f.base.CardID, "verdict": "approve", "observed_content_hash": f.base.CardContentHash, "idempotency_key": uuid.NewString()}
			req := mailboxPrincipalRequest(t, rt, "mailbox.decide", params)
			var result map[string]any
			requireServedJSONRPCResult(t, rt.Endpoint, req.Method, params, &result)
			waitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, f.base.RunID)
			persistence := pipeline.NewWorkflowPersistence(owner.(pipeline.WorkflowPersistenceOwner))
			process := rt.Runtime.Options.ProcessWorkOwner
			lease, err := process.Begin(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = lease.Done() })
			ctx, err := startupownership.WithProcessMutationAuthority(worklifetime.WithProcess(lease.Context(), process), supervisor.processCapability)
			if err != nil {
				t.Fatal(err)
			}
			probe := func(ctx context.Context, request apiidempotency.Request) (json.RawMessage, bool, error) {
				mutation, err := mailboxCardMutation(request, params)
				if err != nil {
					return nil, false, err
				}
				return persistence.ReplayDecisionCardMutation(ctx, request, mutation)
			}
			domain := gateCompletionDomain{Method: req.Method, Actor: req.Actor.ID, Key: req.IdempotencyKey, Hash: req.RequestHash}
			before := gateCompletionRead(t, rt, "projection", f.base.RunID, f.base.CardID, domain)
			var original []byte
			for _, row := range before.API {
				if row.Key == req.IdempotencyKey && row.Method == req.Method && row.Actor == req.Actor.ID && row.ResourceID == req.ResourceID {
					if original != nil {
						t.Fatal("completion domain is not singular")
					}
					original = []byte(row.Response)
				}
			}
			got, replayed, err := probe(ctx, req)
			if err != nil || !replayed || len(original) == 0 || !bytes.Equal(got, original) {
				t.Fatalf("selected-owner exact replay: replay=%t response=%s error=%v", replayed, got, err)
			}
			for _, tc := range []struct {
				name     string
				change   func(*apiidempotency.Request)
				conflict bool
			}{
				{"hash", func(r *apiidempotency.Request) { r.RequestHash = "different-semantic-body" }, true},
				{"resource", func(r *apiidempotency.Request) { r.ResourceID = other.base.CardID }, true},
				{"actor", func(r *apiidempotency.Request) { r.Actor = apiidempotency.PrincipalActor(uuid.NewString()) }, false},
				{"bearer", func(r *apiidempotency.Request) { r.Actor = apiidempotency.BearerActor(req.Actor.ID) }, false},
			} {
				t.Run(tc.name, func(t *testing.T) {
					bad := req
					tc.change(&bad)
					got, replayed, err := probe(ctx, bad)
					if err == nil || replayed || len(got) != 0 || tc.conflict && !errors.Is(err, apiidempotency.ErrConflict) {
						t.Fatalf("domain refusal: response=%s replay=%t error=%v", got, replayed, err)
					}
				})
			}
			mutation, err := mailboxCardMutation(req, params)
			if err != nil {
				t.Fatal(err)
			}
			badMethod := req
			badMethod.Method = "mailbox.defer"
			if got, replayed, err := persistence.ReplayDecisionCardMutation(ctx, badMethod, mutation); err == nil || replayed || len(got) != 0 {
				t.Fatal("method was not bound to its actual mutation")
			}
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			foreign := mustServeTestPersistedSourceArtifactFact("bundle-v2:sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")
			for _, badCtx := range []context.Context{context.Background(), canceled, correlation.WithSourceArtifactFact(ctx, foreign), correlation.WithRootAdmission(ctx, sourceartifact.RootAdmission{})} {
				if got, replayed, err := probe(badCtx, req); err == nil || replayed || len(got) != 0 {
					t.Fatal("missing, foreign or canceled authority returned completion bytes")
				}
			}
			fresh := req
			fresh.IdempotencyKey = uuid.NewString()
			if got, replayed, err := probe(ctx, fresh); err != nil || replayed || len(got) != 0 {
				t.Fatalf("missing key was not a pure miss: replay=%t response=%s error=%v", replayed, got, err)
			}
			const missing = "bundle-v2:sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
			if err := storetest.SwapMailboxRunSource(f.ctx, selectedMailboxFixtureStore(rt), f.base.RunID, rt.BundleHash, missing); err != nil {
				t.Fatal(err)
			}
			got, replayed, readErr := probe(ctx, req)
			restoreErr := storetest.SwapMailboxRunSource(f.ctx, selectedMailboxFixtureStore(rt), f.base.RunID, missing, rt.BundleHash)
			if restoreErr != nil || readErr == nil || replayed || len(got) != 0 {
				t.Fatalf("missing exact source: read=%v restore=%v replay=%t", readErr, restoreErr, replayed)
			}
			sourceReader := owner.(interface {
				GetSourceArtifact(context.Context, string) (sourceartifact.Persisted, error)
			})
			retained, err := sourceReader.GetSourceArtifact(ctx, rt.BundleHash)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := rt.DB.ExecContext(ctx, `UPDATE source_artifacts SET source_blob=$1 WHERE bundle_hash=$2`, []byte("not an artifact"), rt.BundleHash); err != nil {
				t.Fatal(err)
			}
			got, replayed, corruptErr := probe(ctx, req)
			_, restoreErr = rt.DB.ExecContext(ctx, `UPDATE source_artifacts SET source_blob=$1 WHERE bundle_hash=$2`, retained.SourceBlob, rt.BundleHash)
			if restoreErr != nil || corruptErr == nil || replayed || len(got) != 0 {
				t.Fatalf("corrupt exact source: read=%v restore=%v replay=%t", corruptErr, restoreErr, replayed)
			}
			if after := gateCompletionRead(t, rt, "projection", f.base.RunID, f.base.CardID, domain); !reflect.DeepEqual(before, after) {
				t.Fatal("probe/refusal changed original card/event/change/completion/dispatch")
			}
			beforeEffects := mailboxCompletionRunEffects(t, rt, f.base.RunID)
			expired := req
			expired.Now = req.Now.Add(25 * time.Hour)
			if got, replayed, err := probe(ctx, expired); err != nil || replayed || len(got) != 0 {
				t.Fatalf("expired key was not a miss: replay=%t response=%s error=%v", replayed, got, err)
			}
			if afterEffects := mailboxCompletionRunEffects(t, rt, f.base.RunID); !reflect.DeepEqual(beforeEffects, afterEffects) {
				t.Fatal("expiration cleanup executed a domain mutation")
			}
			var completions int
			if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM api_idempotency WHERE method=$1 AND actor_kind=$2 AND actor_id=$3 AND idempotency_key=$4`, req.Method, req.Actor.Kind, req.Actor.ID, req.IdempotencyKey).Scan(&completions); err != nil || completions != 0 {
				t.Fatalf("expired completion retained or recreated: count=%d error=%v", completions, err)
			}
			if err := lease.Done(); err != nil {
				t.Fatal(err)
			}
			rt.joinAndAssertExit()
			if got, replayed, err := probe(ctx, req); err == nil || replayed || len(got) != 0 {
				t.Fatal("retired selected process returned completion bytes")
			}
		})
	}
}
