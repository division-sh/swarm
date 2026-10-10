package channelonboarding

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/google/uuid"
)

type sessionTargetProjectionFixture struct {
	cancellationTestActivations
	selected Candidate
	calls    int
}

func (a *sessionTargetProjectionFixture) AdmitChannelTarget(ctx context.Context, op Operation, _ Candidate) (Candidate, error) {
	a.calls++
	if ctx.Err() != nil {
		return Candidate{}, ctx.Err()
	}
	if op.Phase != PhaseAwaitingOperatorConfirmation || op.BindingRevision < 1 {
		return Candidate{}, errors.New("target selected before confirmation checkpoint")
	}
	return a.selected, nil
}

// Pure transition proof. The projected target is not an SDK/standing runtime
// installation; selected-store confirmation is proved with the encrypted peer.
func TestSessionTargetPromotionRequiresCheckpointedConfirmation(t *testing.T) {
	candidate := testCandidate(strings.Repeat("a", 64), "support")
	candidate.Provider, candidate.Target.Provider = "whatsapp", "whatsapp"
	candidate.Posture = ActivationSessionConnection
	candidate.Target.Selector = "ingress:support:whatsapp"
	candidate.ProviderCredentialRole, candidate.SigningCredentialRole, candidate.Target.SigningCredentialKey = "", "", ""
	candidate.ConnectionHealth = "provider_connection"
	candidate.Target.Generation, candidate.Target.PublicationSequence, candidate.Coordinate.TargetGeneration = 0, 0, 0
	account := operatorchannel.SessionAccountAdmission{Provider: "whatsapp", ConnectionID: uuid.NewString(),
		AccountRef: "15551234567@s.whatsapp.net", AdmissionID: uuid.NewString(), Revision: 1}
	op := Operation{OperationID: uuid.NewString(), Revision: 3, BindingRevision: 1, Coordinate: candidate.Coordinate,
		Provider: candidate.Provider, Interface: candidate.Interface, TargetSelector: candidate.Target.Selector,
		Posture: candidate.Posture, Phase: PhaseAwaitingOperatorConfirmation, SessionConnectionID: account.ConnectionID, SessionAccount: account}
	promoted := candidate
	promoted.Target.Generation, promoted.Target.PublicationSequence, promoted.Coordinate.TargetGeneration = 1, 1, 1
	for _, row := range []string{"current", "unconfirmed", "pairing", "missing_account", "foreign_runtime", "foreign_source", "canceled"} {
		t.Run(row, func(t *testing.T) {
			store := &cancellationTestStore{op: op}
			activation := &sessionTargetProjectionFixture{selected: promoted}
			service := &Service{store: store, activations: activation, now: time.Now}
			ctx := context.Background()
			switch row {
			case "unconfirmed":
				store.op.BindingRevision = 0
			case "pairing":
				store.op.Phase = PhaseActivatingProvider
			case "missing_account":
				store.op.SessionAccount = operatorchannel.SessionAccountAdmission{}
			case "foreign_runtime":
				activation.selected.Coordinate.RuntimeInstanceID = uuid.NewString()
			case "foreign_source":
				activation.selected.Coordinate.BundleHash = "bundle-v2:sha256:" + strings.Repeat("b", 64)
			case "canceled":
				caller, stop := context.WithCancel(ctx)
				stop()
				ctx = caller
			}
			before := store.op
			admitted, result, err := service.admitConfirmedTarget(ctx, before, candidate)
			if row != "current" {
				if err == nil || store.op.Revision != before.Revision || store.op.Coordinate != before.Coordinate {
					t.Fatal("rejected target changed selected authority", admitted, err)
				}
				if (row == "unconfirmed" || row == "pairing" || row == "missing_account") && activation.calls != 0 {
					t.Fatal("target owner was reached before native confirmation", activation.calls)
				}
				return
			}
			if err != nil || admitted.Coordinate != promoted.Coordinate || result.Coordinate != promoted.Coordinate || admitted.Revision != before.Revision+1 {
				t.Fatal("runless parent failed its exact target checkpoint", admitted, result, err)
			}
			repeated, _, err := service.admitConfirmedTarget(ctx, admitted, result)
			if err != nil || repeated.Revision != admitted.Revision || repeated.Coordinate != admitted.Coordinate {
				t.Fatal("target checkpoint repetition mutated responsibility", repeated, err)
			}
		})
	}
}
