package runtimepersistence

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

type channelFaultCleanupControl interface {
	InstallForTest(context.Context) error
	RestoreForTest(context.Context) error
}

func TestChannelFaultCleanupReconcilesUncertainCommitBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, kind := range []string{"render", "binding"} {
			for _, cut := range []string{"install_committed", "install_rolled_back", "restore_committed", "restore_rolled_back"} {
				t.Run(backend+"/"+kind+"/"+cut, func(t *testing.T) {
					proveChannelFaultUncertainCut(t, backend, kind, cut)
				})
			}
		}
	}
}

func proveChannelFaultUncertainCut(t *testing.T, backend, kind, cut string) {
	t.Helper()
	fault, snapshot, connector := newChannelCommitFaultControl(t, backend, kind)
	original, ctx := snapshot(), context.Background()
	operation := fault.InstallForTest
	if cut == "restore_committed" || cut == "restore_rolled_back" {
		if err := fault.InstallForTest(ctx); err != nil {
			t.Fatal(err)
		}
		operation = fault.RestoreForTest
	}
	cause := errors.New("channel fault COMMIT acknowledgement lost")
	calls := 0
	connector.arm(func(tx driver.Tx) error {
		calls++
		if cut == "install_rolled_back" || cut == "restore_rolled_back" {
			return errors.Join(cause, tx.Rollback())
		}
		return errors.Join(cause, tx.Commit())
	})
	if err := operation(ctx); !errors.Is(err, cause) {
		t.Fatalf("operation lost its original acknowledgement error: %v", err)
	}
	unchanged := reflect.DeepEqual(original, snapshot())
	if want := cut == "install_rolled_back" || cut == "restore_committed"; unchanged != want || calls != 1 {
		t.Fatalf("physical COMMIT cut not exercised: unchanged=%t want=%t commits=%d", unchanged, want, calls)
	}
	for attempt := 0; attempt < 2; attempt++ {
		if err := fault.RestoreForTest(ctx); err != nil {
			t.Fatalf("exact cleanup attempt %d: %v", attempt, err)
		}
		if after := snapshot(); !reflect.DeepEqual(original, after) {
			t.Fatalf("cleanup did not preserve complete original: before=%+v after=%+v", original, after)
		}
	}
}

func newChannelCommitFaultControl(t *testing.T, backend, kind string) (channelFaultCleanupControl, func() any, *stopCommitConnector) {
	t.Helper()
	selected, _, connector := newStopCommitStore(t, backend)
	ctx := testAuthorActivityContext()
	run := uuid.NewString()
	requireRunFixtureForTest(t, ctx, selected, semanticRunFixture{
		RunID: run, Origin: semanticEventRunOriginForTest(t, uuid.NewString(), "work.requested"), StartedAt: time.Now().UTC(),
	})
	var fault channelFaultCleanupControl
	var snapshot func() any
	var err error
	if kind == "binding" {
		fault, err = CaptureChannelInputBindingFaultForTest(ctx, selected, run)
		snapshot = func() any {
			row, found, err := selected.(interface {
				LoadRunForkSelectedContractBinding(context.Context, string) (runfork.RunForkSelectedContractBinding, bool, error)
			}).LoadRunForkSelectedContractBinding(ctx, run)
			if err != nil {
				t.Fatal(err)
			}
			return struct {
				Row   runfork.RunForkSelectedContractBinding
				Found bool
			}{row, found}
		}
	} else {
		card := prepareChannelCommitFaultRender(t, selected, run)
		fault, err = CaptureChannelCurrentRenderFaultForTest(ctx, selected, card)
		snapshot = func() any {
			row, err := ReadChannelCardRenderStorageForTest(ctx, selected, card)
			if err != nil {
				t.Fatal(err)
			}
			return row
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		connector.arm(nil)
		if err := fault.RestoreForTest(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return fault, snapshot, connector
}

func prepareChannelCommitFaultRender(t *testing.T, owner any, run string) string {
	t.Helper()
	ctx, now := testAuthorActivityContext(), time.Now().UTC()
	selected := owner.(selectedChannelDeliveryTestStore)
	op, activation := selectedChannelConfirmationAuthorityFixture(t, selected, now, effects.StateAuthorized)
	principal, err := selected.EnsureOperatorPrincipal(ctx, now)
	if err != nil {
		t.Fatal(err)
	}
	binding, err := selected.BeginChannelBinding(ctx, operatorchannel.BeginRequest{
		OperationID: op.IdentityOperationID, Kind: operatorchannel.OperationConnect, PrincipalID: principal.ID,
		Interface: activation.Interface, RequestKeyHash: op.IdentityOperationID, RequestHash: op.IdentityOperationID,
		ProviderAuthority: operatorChannelProviderAuthority(), RequestedAt: now, ExpiresAt: now.Add(operatorchannel.DefaultChallengeTTL),
	})
	if err != nil {
		t.Fatal(err)
	}
	claim := operatorChannelContractClaim(binding, operatorchannel.ConversationScopeDirect, "account", activation.ConversationRef, uuid.NewString())
	var settled operatorchannel.ClaimSettlement
	err = runWorkflowProjectionFault(ctx, owner, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		switch store := owner.(type) {
		case *PostgresStore:
			settled, err = store.operatorChannelPostgresOwner.SettleInboundClaimTx(ctx, tx, claim, now.Add(time.Second))
		case *SQLiteRuntimeStore:
			settled, err = store.operatorChannelSQLiteOwner.SettleInboundClaimTx(ctx, tx, claim, now.Add(time.Second))
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := selected.ConfirmChannelBinding(ctx, operatorchannel.ConfirmRequest{
		OperationID: binding.OperationID, PrincipalID: principal.ID, ExpectedRevision: settled.Operation.Revision,
		Approve: true, ProviderAuthorityCurrent: true, ConfirmedAt: now.Add(2 * time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := selected.AdvanceChannelOnboarding(ctx, channelonboarding.AdvanceRequest{
		OperationID: op.OperationID, ExpectedRevision: op.Revision, Phase: channelonboarding.PhaseSucceeded, Now: now.Add(20 * time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	card, continuation := newRootProposedEffectTestCard(t, run, now)
	if err := owner.(decisioncard.ProposedEffectStore).CreateProposedEffectCard(ctx, card, continuation); err != nil {
		t.Fatal(err)
	}
	created, err := selected.PlanOpenChannelCard(ctx, card.CardID)
	if err != nil || !created {
		t.Fatalf("native card plan created=%t: %v", created, err)
	}
	plans, err := selected.ListCurrentChannelDeliveryPlans(ctx, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, plan := range plans {
		if plan.SourceID == card.CardID {
			if _, err := selected.FreezeAndPersistChannelRender(ctx, plan.DeliveryID, packs.PresentationBounds{Actions: 8, TextRunes: 4096, LabelRunes: 64}); err != nil {
				t.Fatal(err)
			}
			return card.CardID
		}
	}
	t.Fatal("native card plan missing")
	return ""
}
