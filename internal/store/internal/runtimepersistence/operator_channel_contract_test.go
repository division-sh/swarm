package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type operatorChannelContractStore interface {
	operatorchannel.Store
}

type operatorChannelContractFixture struct {
	store  operatorChannelContractStore
	settle func(context.Context, operatorchannel.InboundClaim, time.Time) (operatorchannel.ClaimSettlement, error)
}

func TestOperatorChannelSelectedStoreContractParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := openOperatorChannelContractFixture(t, backend)
			runOperatorChannelContract(t, fixture)
		})
	}
}

func runOperatorChannelContract(t *testing.T, fixture operatorChannelContractFixture) {
	t.Helper()
	ctx := context.Background()
	now := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)

	const concurrentReaders = 8
	principals := make(chan operatorchannel.Principal, concurrentReaders)
	errorsSeen := make(chan error, concurrentReaders)
	var wg sync.WaitGroup
	for range concurrentReaders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			principal, err := fixture.store.EnsureOperatorPrincipal(ctx, now)
			principals <- principal
			errorsSeen <- err
		}()
	}
	wg.Wait()
	close(principals)
	close(errorsSeen)
	for err := range errorsSeen {
		if err != nil {
			t.Fatalf("EnsureOperatorPrincipal: %v", err)
		}
	}
	var principal operatorchannel.Principal
	for candidate := range principals {
		if principal.ID == "" {
			principal = candidate
		}
		if candidate.ID != principal.ID {
			t.Fatalf("concurrent principals differ: %s != %s", candidate.ID, principal.ID)
		}
	}

	rejectedIdentity := operatorChannelContractIdentity("generation-rejected")
	rejectedBegin := operatorchannel.BeginRequest{
		OperationID: uuid.NewString(), Kind: operatorchannel.OperationConnect, PrincipalID: principal.ID,
		Interface: rejectedIdentity, ExpectedRevision: 0, RequestKeyHash: "rejected-key", RequestHash: "rejected-body",
		RequestedAt: now, ExpiresAt: now.Add(operatorchannel.DefaultChallengeTTL),
	}
	rejectedOp, err := fixture.store.BeginChannelBinding(ctx, rejectedBegin)
	if err != nil {
		t.Fatal(err)
	}
	rejectedSettlement, err := fixture.settle(ctx, operatorChannelContractClaim(rejectedOp, operatorchannel.ConversationScopeDirect, "account-rejected", "conversation-rejected", uuid.NewString()), now.Add(time.Second))
	if err != nil || rejectedSettlement.Operation.State != operatorchannel.StateAwaitingConfirmation {
		t.Fatalf("rejected claim settlement = %#v, %v", rejectedSettlement, err)
	}
	rejectedOp, rejectedBinding, err := fixture.store.ConfirmChannelBinding(ctx, operatorchannel.ConfirmRequest{
		OperationID: rejectedOp.OperationID, PrincipalID: principal.ID, ExpectedRevision: rejectedSettlement.Operation.Revision,
		Approve: false, ConfirmedAt: now.Add(2 * time.Second),
	})
	if err != nil || rejectedOp.State != operatorchannel.StateRejected || rejectedOp.Revision != 3 || rejectedBinding != (operatorchannel.Binding{}) {
		t.Fatalf("rejected confirmation = op:%#v binding:%#v err:%v", rejectedOp, rejectedBinding, err)
	}
	bindings, err := fixture.store.ListOperatorChannelBindings(ctx, principal.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, candidate := range bindings {
		if candidate.Interface.Key() == rejectedIdentity.Key() {
			t.Fatalf("rejected operation created binding %#v", candidate)
		}
	}

	concurrentIdentity := operatorChannelContractIdentity("generation-concurrent-claim")
	concurrentOp, err := fixture.store.BeginChannelBinding(ctx, operatorchannel.BeginRequest{
		OperationID: uuid.NewString(), Kind: operatorchannel.OperationConnect, PrincipalID: principal.ID,
		Interface: concurrentIdentity, ExpectedRevision: 0, RequestKeyHash: "concurrent-claim-key", RequestHash: "concurrent-claim-body",
		RequestedAt: now, ExpiresAt: now.Add(operatorchannel.DefaultChallengeTTL),
	})
	if err != nil {
		t.Fatal(err)
	}
	claimants := []operatorchannel.InboundClaim{
		operatorChannelContractClaim(concurrentOp, operatorchannel.ConversationScopeDirect, "account-concurrent-a", "conversation-concurrent-a", uuid.NewString()),
		operatorChannelContractClaim(concurrentOp, operatorchannel.ConversationScopeShared, "account-concurrent-b", "conversation-concurrent-b", uuid.NewString()),
	}
	startClaims := make(chan struct{})
	claimResults := make(chan operatorchannel.ClaimSettlement, len(claimants))
	claimErrors := make(chan error, len(claimants))
	for _, claimant := range claimants {
		claimant := claimant
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-startClaims
			settlement, err := fixture.settle(ctx, claimant, now.Add(3*time.Second))
			claimResults <- settlement
			claimErrors <- err
		}()
	}
	close(startClaims)
	wg.Wait()
	close(claimResults)
	close(claimErrors)
	for err := range claimErrors {
		if err != nil {
			t.Fatalf("concurrent claimant settlement: %v", err)
		}
	}
	consumed, rejected := 0, 0
	var winningOperation operatorchannel.Operation
	for result := range claimResults {
		switch result.Disposition {
		case operatorchannel.DispositionConsumedBinding:
			consumed++
			winningOperation = result.Operation
		case operatorchannel.DispositionRejectedClaim:
			rejected++
		default:
			t.Fatalf("concurrent claimant disposition = %q", result.Disposition)
		}
	}
	if consumed != 1 || rejected != 1 || winningOperation.State != operatorchannel.StateAwaitingConfirmation || winningOperation.Revision != 2 {
		t.Fatalf("concurrent claimant results consumed=%d rejected=%d winner=%#v", consumed, rejected, winningOperation)
	}

	identity := operatorChannelContractIdentity("generation-a")
	begin := operatorchannel.BeginRequest{
		OperationID: uuid.NewString(), Kind: operatorchannel.OperationConnect, PrincipalID: principal.ID,
		Interface: identity, ExpectedRevision: 0, RequestKeyHash: "connect-key", RequestHash: "connect-body",
		RequestedAt: now, ExpiresAt: now.Add(operatorchannel.DefaultChallengeTTL),
	}
	op, err := fixture.store.BeginChannelBinding(ctx, begin)
	if err != nil {
		t.Fatal(err)
	}
	replay := begin
	replay.OperationID = uuid.NewString()
	replayed, err := fixture.store.BeginChannelBinding(ctx, replay)
	if err != nil || replayed.OperationID != op.OperationID || replayed.Challenge != op.Challenge {
		t.Fatalf("begin replay = %#v, %v", replayed, err)
	}
	changed := replay
	changed.RequestHash = "changed-body"
	if _, err := fixture.store.BeginChannelBinding(ctx, changed); !errors.Is(err, operatorchannel.ErrConflict) {
		t.Fatalf("changed begin replay error = %v", err)
	}

	claim := operatorChannelContractClaim(op, operatorchannel.ConversationScopeDirect, `{"principal":"account-a"}`, `{"room":"conversation-a"}`, uuid.NewString())
	settlement, err := fixture.settle(ctx, claim, now.Add(time.Second))
	if err != nil || settlement.Disposition != operatorchannel.DispositionConsumedBinding || settlement.Operation.State != operatorchannel.StateAwaitingConfirmation {
		t.Fatalf("claim settlement = %#v, %v", settlement, err)
	}
	duplicate := claim
	duplicate.PublicationID = uuid.NewString()
	duplicate.ProviderEventID = "event-duplicate"
	settlement, err = fixture.settle(ctx, duplicate, now.Add(2*time.Second))
	if err != nil || settlement.Operation.OperationID != op.OperationID || settlement.Operation.Revision != 2 {
		t.Fatalf("duplicate settlement = %#v, %v", settlement, err)
	}
	changedClaim := claim
	changedClaim.PublicationID = uuid.NewString()
	changedClaim.ProviderEventID = "event-changed"
	changedClaim.ConversationScope = operatorchannel.ConversationScopeShared
	settlement, err = fixture.settle(ctx, changedClaim, now.Add(3*time.Second))
	if err != nil || settlement.Disposition != operatorchannel.DispositionRejectedClaim {
		t.Fatalf("changed claim settlement = %#v, %v", settlement, err)
	}

	confirmed, binding, err := fixture.store.ConfirmChannelBinding(ctx, operatorchannel.ConfirmRequest{
		OperationID: op.OperationID, PrincipalID: principal.ID, ExpectedRevision: 2, Approve: true, ConfirmedAt: now.Add(4 * time.Second),
	})
	if err != nil || confirmed.State != operatorchannel.StateBound || confirmed.ProofStatus != operatorchannel.ProofSkipped || binding.Revision != 1 || binding.ConversationScope != operatorchannel.ConversationScopeDirect ||
		binding.ProofID != "" || binding.ProofRevision != 0 ||
		binding.ExternalAccountRef != `{"principal":"account-a"}` || binding.ConversationRef != `{"room":"conversation-a"}` {
		t.Fatalf("confirm = op:%#v binding:%#v err:%v", confirmed, binding, err)
	}
	responsibilities, err := fixture.store.ListPendingProofResponsibilities(ctx)
	if err != nil || len(responsibilities) != 0 {
		t.Fatalf("no-save proof responsibilities = %#v, %v", responsibilities, err)
	}
	replayedConfirm, replayedBinding, err := fixture.store.ConfirmChannelBinding(ctx, operatorchannel.ConfirmRequest{
		OperationID: op.OperationID, PrincipalID: principal.ID, ExpectedRevision: 2, Approve: true, ConfirmedAt: now.Add(5 * time.Second),
	})
	if err != nil || replayedConfirm.Revision != confirmed.Revision || replayedBinding.Revision != binding.Revision {
		t.Fatalf("confirmation replay = op:%#v binding:%#v err:%v", replayedConfirm, replayedBinding, err)
	}
	if _, err := fixture.store.BeginChannelBinding(ctx, operatorchannel.BeginRequest{
		OperationID: uuid.NewString(), Kind: operatorchannel.OperationConnect, PrincipalID: principal.ID,
		Interface: identity, ExpectedRevision: 1, RequestKeyHash: "connect-current-key", RequestHash: "connect-current-body",
		RequestedAt: now.Add(5 * time.Second), ExpiresAt: now.Add(operatorchannel.DefaultChallengeTTL),
	}); !errors.Is(err, operatorchannel.ErrConflict) {
		t.Fatalf("connect replaced current binding: %v", err)
	}
	if _, err := fixture.store.BeginChannelBinding(ctx, operatorchannel.BeginRequest{
		OperationID: uuid.NewString(), Kind: operatorchannel.OperationRebind, PrincipalID: principal.ID,
		Interface: operatorChannelContractIdentity("generation-unbound-rebind"), ExpectedRevision: 0,
		RequestKeyHash: "rebind-unbound-key", RequestHash: "rebind-unbound-body",
		RequestedAt: now.Add(5 * time.Second), ExpiresAt: now.Add(operatorchannel.DefaultChallengeTTL),
	}); !errors.Is(err, operatorchannel.ErrConflict) {
		t.Fatalf("rebind admitted unbound interface: %v", err)
	}

	reconnectOp, err := fixture.store.BeginChannelBinding(ctx, operatorchannel.BeginRequest{
		OperationID: uuid.NewString(), Kind: operatorchannel.OperationReconnect, PrincipalID: principal.ID,
		Interface: identity, ExpectedRevision: 1, RequestKeyHash: "reconnect-changed-scope-key", RequestHash: "reconnect-changed-scope-body",
		RequestedAt: now.Add(5 * time.Second), ExpiresAt: now.Add(operatorchannel.DefaultChallengeTTL),
	})
	if err != nil {
		t.Fatal(err)
	}
	reconnectSettlement, err := fixture.settle(ctx, operatorChannelContractClaim(reconnectOp, operatorchannel.ConversationScopeShared, `{"principal":"account-a"}`, `{"room":"conversation-a"}`, uuid.NewString()), now.Add(6*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := fixture.store.ConfirmChannelBinding(ctx, operatorchannel.ConfirmRequest{
		OperationID: reconnectOp.OperationID, PrincipalID: principal.ID, ExpectedRevision: reconnectSettlement.Operation.Revision,
		Approve: true, ConfirmedAt: now.Add(7 * time.Second),
	}); !errors.Is(err, operatorchannel.ErrConflict) {
		t.Fatalf("reconnect changed conversation scope: %v", err)
	}

	sameClaimantRebind, err := fixture.store.BeginChannelBinding(ctx, operatorchannel.BeginRequest{
		OperationID: uuid.NewString(), Kind: operatorchannel.OperationRebind, PrincipalID: principal.ID,
		Interface: identity, ExpectedRevision: 1, RequestKeyHash: "rebind-same-key", RequestHash: "rebind-same-body",
		RequestedAt: now.Add(5 * time.Second), ExpiresAt: now.Add(operatorchannel.DefaultChallengeTTL),
	})
	if err != nil {
		t.Fatal(err)
	}
	sameClaimantSettlement, err := fixture.settle(ctx, operatorChannelContractClaim(sameClaimantRebind, operatorchannel.ConversationScopeDirect, `{"principal":"account-a"}`, `{"room":"conversation-a"}`, uuid.NewString()), now.Add(6*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := fixture.store.ConfirmChannelBinding(ctx, operatorchannel.ConfirmRequest{
		OperationID: sameClaimantRebind.OperationID, PrincipalID: principal.ID, ExpectedRevision: sameClaimantSettlement.Operation.Revision,
		Approve: true, ConfirmedAt: now.Add(7 * time.Second),
	}); !errors.Is(err, operatorchannel.ErrConflict) {
		t.Fatalf("rebind admitted unchanged claimant: %v", err)
	}
	if _, _, err := fixture.store.UnbindOperatorChannel(ctx, operatorchannel.UnbindRequest{
		OperationID: uuid.NewString(), PrincipalID: principal.ID, Interface: identity, ExpectedRevision: 2,
		RequestKeyHash: "unbind-stale-key", RequestHash: "unbind-stale-body", RequestedAt: now.Add(7 * time.Second),
	}); !errors.Is(err, operatorchannel.ErrRevisionConflict) {
		t.Fatalf("unbind admitted stale expected revision: %v", err)
	}

	rebind := operatorchannel.BeginRequest{
		OperationID: uuid.NewString(), Kind: operatorchannel.OperationRebind, PrincipalID: principal.ID,
		Interface: identity, ExpectedRevision: 1, RequestKeyHash: "rebind-key", RequestHash: "rebind-body",
		RequestedAt: now.Add(6 * time.Second), ExpiresAt: now.Add(operatorchannel.DefaultChallengeTTL),
	}
	rebindOp, err := fixture.store.BeginChannelBinding(ctx, rebind)
	if err != nil {
		t.Fatal(err)
	}
	rebindClaim := operatorChannelContractClaim(rebindOp, operatorchannel.ConversationScopeShared, "account-b", "conversation-b", uuid.NewString())
	settlement, err = fixture.settle(ctx, rebindClaim, now.Add(7*time.Second))
	if err != nil || settlement.Operation.State != operatorchannel.StateAwaitingConfirmation {
		t.Fatalf("rebind claim = %#v, %v", settlement, err)
	}
	_, binding, err = fixture.store.ConfirmChannelBinding(ctx, operatorchannel.ConfirmRequest{
		OperationID: rebindOp.OperationID, PrincipalID: principal.ID, ExpectedRevision: 2, Approve: true, ConfirmedAt: now.Add(8 * time.Second),
	})
	if err != nil || binding.Revision != 2 || binding.ConversationScope != operatorchannel.ConversationScopeShared {
		t.Fatalf("rebind confirmation = %#v, %v", binding, err)
	}

	unbindOp, unbound, err := fixture.store.UnbindOperatorChannel(ctx, operatorchannel.UnbindRequest{
		OperationID: uuid.NewString(), PrincipalID: principal.ID, Interface: identity, ExpectedRevision: 2,
		RequestKeyHash: "unbind-key", RequestHash: "unbind-body", RequestedAt: now.Add(9 * time.Second),
	})
	if err != nil || unbindOp.State != operatorchannel.StateUnbound || unbound.Status != operatorchannel.BindingUnbound || unbound.Revision != 3 {
		t.Fatalf("unbind = op:%#v binding:%#v err:%v", unbindOp, unbound, err)
	}

	proof := operatorChannelContractProof(identity, binding, now.Add(8*time.Second))
	if _, err := fixture.store.BindOperatorChannelFromProof(ctx, operatorchannel.BootBindRequest{PrincipalID: principal.ID, Interface: identity, Proof: proof, RequestedAt: now.Add(10 * time.Second)}); !errors.Is(err, operatorchannel.ErrConflict) {
		t.Fatalf("proof bypassed unbind fence: %v", err)
	}

	expiredBegin := operatorchannel.BeginRequest{
		OperationID: uuid.NewString(), Kind: operatorchannel.OperationConnect, PrincipalID: principal.ID,
		Interface: operatorChannelContractIdentity("generation-expired"), ExpectedRevision: 0,
		RequestKeyHash: "expired-key", RequestHash: "expired-body", RequestedAt: now, ExpiresAt: now.Add(time.Second),
	}
	expiredOp, err := fixture.store.BeginChannelBinding(ctx, expiredBegin)
	if err != nil {
		t.Fatal(err)
	}
	expiredByOwner, err := fixture.store.ExpireChannelBinding(ctx, operatorchannel.ExpireRequest{
		OperationID: expiredOp.OperationID, PrincipalID: principal.ID, ExpectedRevision: expiredOp.Revision, ExpiredAt: now.Add(2 * time.Second),
	})
	if err != nil || expiredByOwner.State != operatorchannel.StateExpired || expiredByOwner.Revision != expiredOp.Revision+1 {
		t.Fatalf("owner expiry = %#v, %v", expiredByOwner, err)
	}
	replayedOwnerExpiry, err := fixture.store.ExpireChannelBinding(ctx, operatorchannel.ExpireRequest{
		OperationID: expiredOp.OperationID, PrincipalID: principal.ID, ExpectedRevision: expiredOp.Revision, ExpiredAt: now.Add(3 * time.Second),
	})
	if err != nil || replayedOwnerExpiry != expiredByOwner {
		t.Fatalf("owner expiry replay = %#v, %v, want %#v", replayedOwnerExpiry, err, expiredByOwner)
	}

	claimExpiredBegin := expiredBegin
	claimExpiredBegin.OperationID = uuid.NewString()
	claimExpiredBegin.RequestKeyHash = "claim-expired-key"
	claimExpiredBegin.RequestHash = "claim-expired-body"
	expiredOp, err = fixture.store.BeginChannelBinding(ctx, claimExpiredBegin)
	if err != nil {
		t.Fatal(err)
	}
	expiredSettlement, err := fixture.settle(ctx, operatorChannelContractClaim(expiredOp, operatorchannel.ConversationScopeDirect, "account-c", "conversation-c", uuid.NewString()), now.Add(2*time.Second))
	if err != nil || expiredSettlement.Disposition != operatorchannel.DispositionRejectedClaim || expiredSettlement.Operation.State != operatorchannel.StateExpired {
		t.Fatalf("expired settlement = %#v, %v", expiredSettlement, err)
	}

	confirmExpiryBegin := operatorchannel.BeginRequest{
		OperationID: uuid.NewString(), Kind: operatorchannel.OperationConnect, PrincipalID: principal.ID,
		Interface: operatorChannelContractIdentity("generation-confirm-expired"), ExpectedRevision: 0,
		RequestKeyHash: "confirm-expired-key", RequestHash: "confirm-expired-body", RequestedAt: now, ExpiresAt: now.Add(time.Second),
	}
	confirmExpiryOp, err := fixture.store.BeginChannelBinding(ctx, confirmExpiryBegin)
	if err != nil {
		t.Fatal(err)
	}
	confirmExpirySettlement, err := fixture.settle(ctx, operatorChannelContractClaim(confirmExpiryOp, operatorchannel.ConversationScopeDirect, "account-d", "conversation-d", uuid.NewString()), now.Add(500*time.Millisecond))
	if err != nil || confirmExpirySettlement.Operation.State != operatorchannel.StateAwaitingConfirmation {
		t.Fatalf("confirmation-expiry claim = %#v, %v", confirmExpirySettlement, err)
	}
	expiredConfirmation, expiredBinding, err := fixture.store.ConfirmChannelBinding(ctx, operatorchannel.ConfirmRequest{
		OperationID: confirmExpiryOp.OperationID, PrincipalID: principal.ID, ExpectedRevision: confirmExpirySettlement.Operation.Revision,
		Approve: true, ConfirmedAt: now.Add(2 * time.Second),
	})
	if !errors.Is(err, operatorchannel.ErrOperationTerminal) || expiredConfirmation.State != operatorchannel.StateExpired || expiredConfirmation.Revision != 3 || expiredBinding != (operatorchannel.Binding{}) {
		t.Fatalf("expired confirmation = op:%#v binding:%#v err:%v", expiredConfirmation, expiredBinding, err)
	}
	operations, err := fixture.store.ListOperatorChannelOperations(ctx, principal.ID)
	if err != nil {
		t.Fatal(err)
	}
	foundExpiredConfirmation := false
	for _, candidate := range operations {
		if candidate.OperationID == confirmExpiryOp.OperationID {
			foundExpiredConfirmation = candidate.State == operatorchannel.StateExpired && candidate.Revision == 3
		}
	}
	if !foundExpiredConfirmation {
		t.Fatalf("expired confirmation was not committed: %#v", operations)
	}
	replayedExpired, _, err := fixture.store.ConfirmChannelBinding(ctx, operatorchannel.ConfirmRequest{
		OperationID: confirmExpiryOp.OperationID, PrincipalID: principal.ID, ExpectedRevision: confirmExpirySettlement.Operation.Revision,
		Approve: true, ConfirmedAt: now.Add(3 * time.Second),
	})
	if !errors.Is(err, operatorchannel.ErrOperationTerminal) || replayedExpired.State != operatorchannel.StateExpired || replayedExpired.Revision != 3 {
		t.Fatalf("expired confirmation replay = op:%#v err:%v", replayedExpired, err)
	}

	unknownClaim := operatorChannelContractClaim(expiredOp, operatorchannel.ConversationScopeDirect, "account-c", "conversation-c", uuid.NewString())
	unknownClaim.Challenge = "SWARM-BBBBBBBBBBBBBBBB"
	unknownClaim.Text = unknownClaim.Challenge
	unknownSettlement, err := fixture.settle(ctx, unknownClaim, now.Add(3*time.Second))
	if err != nil || unknownSettlement.Disposition != operatorchannel.DispositionRejectedClaim || !unknownSettlement.Consumed {
		t.Fatalf("unknown settlement = %#v, %v", unknownSettlement, err)
	}
}

func openOperatorChannelContractFixture(t *testing.T, backend string) operatorChannelContractFixture {
	t.Helper()
	switch backend {
	case "sqlite":
		selected := newBootstrappedSQLiteRuntimeStoreForTest(t)
		return operatorChannelContractFixture{
			store: selected,
			settle: func(ctx context.Context, claim operatorchannel.InboundClaim, now time.Time) (operatorchannel.ClaimSettlement, error) {
				var result operatorchannel.ClaimSettlement
				err := selected.backend.RunTransaction(ctx, "operator channel contract claim", func(txctx context.Context, tx *sql.Tx) error {
					var err error
					result, err = selected.operatorChannelSQLiteOwner.SettleInboundClaimTx(txctx, tx, claim, now)
					return err
				})
				return result, err
			},
		}
	case "postgres":
		_, db, cleanup := testutil.StartPostgres(t)
		t.Cleanup(cleanup)
		selected := admitTestPostgresStore(t, db)
		return operatorChannelContractFixture{
			store: selected,
			settle: func(ctx context.Context, claim operatorchannel.InboundClaim, now time.Time) (operatorchannel.ClaimSettlement, error) {
				tx, err := db.BeginTx(ctx, nil)
				if err != nil {
					return operatorchannel.ClaimSettlement{}, err
				}
				result, err := selected.operatorChannelPostgresOwner.SettleInboundClaimTx(ctx, tx, claim, now)
				if err != nil {
					_ = tx.Rollback()
					return result, err
				}
				return result, tx.Commit()
			},
		}
	default:
		t.Fatalf("unknown backend %q", backend)
		return operatorChannelContractFixture{}
	}
}

func operatorChannelContractIdentity(generation string) operatorchannel.InterfaceIdentity {
	return operatorchannel.InterfaceIdentity{
		InterfaceRef: operatorchannel.InterfaceHITLChannelV2, ChannelPackID: "provider.telegram.hitl_channel",
		ChannelPackVersion: "1.0.0", ChannelManifestHash: "sha256:" + strings.Repeat("a", 64), SemanticGeneration: generation,
	}.Normalized()
}

func operatorChannelContractClaim(op operatorchannel.Operation, scope operatorchannel.ConversationScope, account, conversation, publicationID string) operatorchannel.InboundClaim {
	return operatorchannel.InboundClaim{
		TextFact: operatorchannel.TextFact{
			Interface: op.Interface, ExternalAccountRef: account, ConversationRef: conversation,
			ConversationScope: scope, Text: op.Challenge, AccountPresentation: "@operator",
		},
		Provider: "telegram", ProviderEventID: "event-" + publicationID, PublicationID: publicationID,
		ProviderAuthorization: "verified-pack-generation", Challenge: op.Challenge,
	}
}

func operatorChannelContractProof(identity operatorchannel.InterfaceIdentity, binding operatorchannel.Binding, at time.Time) operatorchannel.VerifiedProof {
	return operatorchannel.VerifiedProof{
		Format: operatorchannel.ProofFormat, ProofID: operatorchannel.ProofIDForInterface(identity), Revision: 1, Status: operatorchannel.ProofActive,
		Interface: identity, ExternalAccountRef: binding.ExternalAccountRef, ConversationRef: binding.ConversationRef,
		ConversationScope: binding.ConversationScope, AccountPresentation: binding.AccountPresentation,
		Method: string(operatorchannel.OperationRebind), Challenge: "SWARM-AAAAAAAAAAAAAAAA", OriginalOperationID: binding.OperationID,
		MintingStoreID: binding.PrincipalID, MintingDeploymentID: uuid.NewString(), VerifiedAt: at, OperatorConfirmed: true,
		ConsentScopes: []operatorchannel.ConsentScope{operatorchannel.ConsentNotify, operatorchannel.ConsentDecide},
	}
}
