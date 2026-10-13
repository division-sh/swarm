package runforkexecution

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/storetest"
)

type selectedHandlerFanOutHeldClaim struct {
	claim fanoutobligation.Claim
	chunk pipeline.FanOutChunkCommand
}

type selectedHandlerFanOutClaimHold struct {
	key      fanoutobligation.IntentKey
	acquired chan selectedHandlerFanOutHeldClaim
	release  chan struct{}
	once     sync.Once
}

func newSelectedHandlerFanOutClaimHold(key fanoutobligation.IntentKey) *selectedHandlerFanOutClaimHold {
	return &selectedHandlerFanOutClaimHold{key: key, acquired: make(chan selectedHandlerFanOutHeldClaim, 1), release: make(chan struct{})}
}

func (h *selectedHandlerFanOutClaimHold) observe(ctx context.Context, chunk pipeline.FanOutChunkCommand) {
	claim := chunk.Claim
	if claim.Key != h.key {
		return
	}
	select {
	case h.acquired <- selectedHandlerFanOutHeldClaim{claim: claim, chunk: chunk}:
	case <-ctx.Done():
		return
	}
	select {
	case <-h.release:
	case <-ctx.Done():
	}
}

func (h *selectedHandlerFanOutClaimHold) resume() { h.once.Do(func() { close(h.release) }) }

func requireSelectedHandlerFanOutLiveClaimFenced(t *testing.T, ctx context.Context, selected any, owner SelectedContractExecutionOwner, checkpoint selectedForkCrashCheckpoint, held selectedHandlerFanOutHeldClaim) {
	t.Helper()
	old := *checkpoint.FanOut.Claim
	if held.claim.Validate() != nil || held.claim.Key != old.Key || held.claim.Owner == old.Owner || held.claim.Generation != old.Generation+1 {
		t.Fatalf("hold missed the actual native successor claim: claim=%+v predecessor=%+v", held.claim, old)
	}
	if !held.claim.LeaseUntil.After(time.Now()) {
		t.Fatal("successor lease expired before the LIVE fencing probes")
	}
	binder := selected.(selectedHandlerFanOutBinder)
	grant := selectedHandlerFanOutGrant(t, owner, checkpoint.ForkRun)
	current, err := binder.BindSelectedFanOutGrant(grant)
	if err != nil || current == nil {
		t.Fatalf("current native owner: %T error=%v", current, err)
	}
	input, err := current.LoadFanOutEvaluation(ctx, held.claim)
	if err != nil || input.StartOrdinal != 0 || input.Trigger.RunID() != checkpoint.ForkRun || !reflect.DeepEqual(input.Items, []any{"one", "two"}) {
		t.Fatalf("LIVE current-claim evaluation positive control: %+v error=%v", input, err)
	}
	group, err := current.BeginFanOutPublicationGroup(ctx, held.claim)
	if err != nil || group == nil {
		t.Fatalf("LIVE current-claim publication-group positive control: %T error=%v", group, err)
	}
	if err := group.Close(ctx); err != nil {
		t.Fatal(err)
	}
	read := func() fanoutobligation.IntentReadback {
		page, err := binder.ListFanOutIntents(ctx, fanoutobligation.ListQuery{RunID: checkpoint.ForkRun, Limit: 10})
		if err != nil || len(page.Intents) != 1 || page.Intents[0].Key != held.claim.Key {
			t.Fatalf("exact live successor observation: %+v error=%v", page, err)
		}
		fact := page.Intents[0]
		// Process sampling is not persisted claim/ordinal evidence.
		fact.Runtime = fanoutobligation.UnavailableRuntimeReadback()
		return fact
	}
	before := read()
	if before.Status != fanoutobligation.StatusOpen || before.ClaimOwner != held.claim.Owner || before.ClaimGeneration != held.claim.Generation || before.LeaseExpiresAt == nil || !before.LeaseExpiresAt.Equal(held.claim.LeaseUntil) {
		t.Fatalf("native live readback does not match the held claim: %+v", before)
	}
	progressBefore, err := storetest.ReadFanOutIntentCompletionStorage(ctx, selected, held.claim.Key)
	if err != nil || len(progressBefore.Outcomes) != 0 || progressBefore.Cursor != 0 {
		t.Fatalf("hold occurred after publication/settlement: %+v error=%v", progressBefore, err)
	}
	publicationsBefore, err := storetest.ReadFanOutPublishedOutcomeStorage(ctx, selected, checkpoint.ForkRun)
	if err != nil || len(publicationsBefore) != 0 {
		t.Fatalf("hold has already published an ordinal: %+v error=%v", publicationsBefore, err)
	}
	if stale, err := binder.BindSelectedFanOutGrant(*checkpoint.FanOut.Grant); err == nil || stale != nil {
		t.Fatalf("LIVE successor accepted its predecessor grant: %T error=%v", stale, err)
	}
	if _, err := current.LoadFanOutEvaluation(ctx, old); !errors.Is(err, fanoutobligation.ErrStaleClaim) {
		t.Fatalf("LIVE successor accepted stale evaluation: %v", err)
	}
	if stale, err := current.BeginFanOutPublicationGroup(ctx, old); !errors.Is(err, fanoutobligation.ErrStaleClaim) || stale != nil {
		if stale != nil {
			_ = stale.Close(ctx)
		}
		t.Fatalf("LIVE successor accepted stale group admission: %T error=%v", stale, err)
	}
	failure, found := failures.EnvelopeFromError(failures.New(failures.ClassConnectorFailure, "provider_rate_limited", "proof", "stale_claim", map[string]any{"status": 429}))
	if !found || failures.ValidateEnvelope(failure) != nil {
		t.Fatal("stale-call counterexample lacks a valid typed retry/block failure")
	}
	retry := pipeline.FanOutRetryableRelease{Claim: old, Now: time.Now().UTC(), Failure: failure}
	block := pipeline.FanOutBlockRequest{Claim: old, Now: time.Now().UTC(), Failure: failure}
	if retry.Validate() != nil || block.Validate() != nil {
		t.Fatal("stale retry/block requests are malformed")
	}
	for _, request := range []struct {
		name string
		call func() (pipeline.FanOutClaimSettlement, error)
	}{
		{"release", func() (pipeline.FanOutClaimSettlement, error) { return current.ReleaseFanOutClaim(ctx, old) }},
		{"retryable_release", func() (pipeline.FanOutClaimSettlement, error) { return current.ReleaseFanOutRetryable(ctx, retry) }},
		{"block", func() (pipeline.FanOutClaimSettlement, error) { return current.BlockFanOutClaim(ctx, block) }},
	} {
		settlement, err := request.call()
		if settlement.Acknowledged || !errors.Is(err, fanoutobligation.ErrStaleClaim) {
			t.Fatalf("LIVE successor accepted stale %s: %+v error=%v", request.name, settlement, err)
		}
		if after := read(); !reflect.DeepEqual(before, after) {
			t.Fatalf("stale %s changed live owner/generation/lease/retry/block/progress: before=%+v after=%+v", request.name, before, after)
		}
	}
	// Publication groups cannot be borrowed across claims. Probe stale chunk
	// settlement with valid semantic outcomes, which require no group, while the
	// real successor retains its original sealed publications for completion.
	rejection, found := failures.EnvelopeFromError(failures.New(failures.ClassSchemaInvalid, "emit_payload_contract_violation", "runtime.engine", "fan_out.emit",
		map[string]any{"event": "items.child", "kind": "schema_mismatch", "path": "$.value", "constraint": "type", "expected": "text", "actual": "integer", "detail": "$.value must be text"}))
	if !found {
		t.Fatal("stale chunk lacks a typed semantic rejection")
	}
	raw, err := failures.MarshalEnvelope(rejection)
	if err != nil || fanoutobligation.ValidateSemanticRejection(raw) != nil {
		t.Fatalf("stale chunk semantic rejection is invalid: %s error=%v", raw, err)
	}
	chunk := pipeline.FanOutChunkCommand{Claim: old, Now: time.Now().UTC(), Outcomes: make([]pipeline.FanOutChunkOutcome, len(held.chunk.Outcomes))}
	for i, outcome := range held.chunk.Outcomes {
		chunk.Outcomes[i] = pipeline.FanOutChunkOutcome{Ordinal: outcome.Ordinal, Failure: raw}
	}
	if err := chunk.Validate(); err != nil {
		t.Fatal(err)
	}
	if committed, err := current.CommitFanOutChunk(ctx, chunk); !errors.Is(err, fanoutobligation.ErrStaleClaim) || len(committed.Publications) != 0 {
		t.Fatalf("LIVE successor accepted a valid stale chunk: %+v error=%v", committed, err)
	}
	if after := read(); !reflect.DeepEqual(before, after) {
		t.Fatalf("stale chunk changed the LIVE successor: before=%+v after=%+v", before, after)
	}
	progressAfter, err := storetest.ReadFanOutIntentCompletionStorage(ctx, selected, held.claim.Key)
	if err != nil || !reflect.DeepEqual(progressBefore, progressAfter) {
		t.Fatalf("stale operations changed exact ordinal progress/outcomes: before=%+v after=%+v error=%v", progressBefore, progressAfter, err)
	}
	publicationsAfter, err := storetest.ReadFanOutPublishedOutcomeStorage(ctx, selected, checkpoint.ForkRun)
	if err != nil || !reflect.DeepEqual(publicationsBefore, publicationsAfter) {
		t.Fatalf("stale operations published ordinal events: before=%+v after=%+v error=%v", publicationsBefore, publicationsAfter, err)
	}
	if after, err := current.LoadFanOutEvaluation(ctx, held.claim); err != nil || !reflect.DeepEqual(input, after) {
		t.Fatalf("stale calls damaged CURRENT evaluation: before=%+v after=%+v error=%v", input, after, err)
	}
	if !held.claim.LeaseUntil.After(time.Now()) {
		t.Fatal("successor lease expired during the LIVE fencing probes")
	}
}
