//go:build linux || darwin

package serveapp

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/google/uuid"
	"go.mau.fi/whatsmeow/types"
)

var errRegisteredAckCut = errors.New("controlled registered acknowledgement interruption")

type registeredAckCut struct {
	effects.Store
	options    cliapp.ChannelRegistrationOptions
	mode       string
	fired      bool
	id         string
	outcome    effects.OperationOutcome
	bindings   []operatorchannel.Binding
	operations []channelonboarding.Operation
	readErr    error
}

type registeredAckCloseObserver struct {
	cliapp.ChannelRegistrationSession
	cut *registeredAckCut
}

func (s registeredAckCloseObserver) Close(ctx context.Context) error {
	closed := s.ChannelRegistrationSession.Close(ctx)
	if s.cut.fired && s.cut.id != "" {
		var found bool
		s.cut.outcome, found, s.cut.readErr = s.cut.GetExternalEffectOutcome(ctx, s.cut.id)
		if s.cut.readErr == nil && !found {
			s.cut.readErr = errors.New("acknowledgement cut lost its journal")
		}
		if s.cut.readErr == nil {
			s.cut.bindings, s.cut.readErr = s.cut.options.Identities.ListOperatorChannelBindings(ctx, s.cut.options.Principal.ID)
		}
		if s.cut.readErr == nil {
			s.cut.operations, s.cut.readErr = s.cut.options.Operations.ListChannelOnboardingOperations(ctx)
		}
	}
	return errors.Join(closed, s.cut.readErr)
}

func (s *registeredAckCut) GetExternalEffectOutcome(ctx context.Context, id string) (effects.OperationOutcome, bool, error) {
	return s.Store.(effects.OutcomeStore).GetExternalEffectOutcome(ctx, id)
}

func (s *registeredAckCut) AuthorizeExternalAttempt(ctx context.Context, a effects.Authority, req effects.AuthorizeRequest) (effects.Attempt, error) {
	if a.ChannelConfirmation.TransportParent != nil {
		s.id = req.OperationID
	}
	return s.Store.AuthorizeExternalAttempt(ctx, a, req)
}

func (s *registeredAckCut) MarkExternalAttemptLaunched(ctx context.Context, attempt effects.Attempt, now time.Time) (effects.ExternalAttemptLaunch, error) {
	if attempt.OperationID == s.id && !s.fired {
		switch s.mode {
		case "prelaunch":
			s.fired = true
			return effects.ExternalAttemptLaunch{}, errRegisteredAckCut
		case "unbind", "logout_pending":
			s.fired = true
			confirmation := attempt.Authority.ChannelConfirmation
			op, err := s.options.Operations.GetChannelOnboarding(ctx, confirmation.OnboardingOperationID)
			if err != nil {
				return effects.ExternalAttemptLaunch{}, err
			}
			if s.mode == "unbind" {
				_, _, err = s.options.Identities.UnbindOperatorChannel(ctx, operatorchannel.UnbindRequest{
					OperationID: uuid.NewString(), PrincipalID: op.PrincipalID, Interface: op.Interface,
					ExpectedRevision: confirmation.BindingRevision, RequestKeyHash: "ack-cut-unbind", RequestHash: "ack-cut-unbind", RequestedAt: now})
			} else {
				_, err = s.options.Operations.AdvanceChannelOnboarding(ctx, channelonboarding.AdvanceRequest{OperationID: op.OperationID,
					ExpectedRevision: op.Revision, Phase: op.Phase, FailureCode: "account_logout_pending", Now: now})
			}
			if err != nil {
				return effects.ExternalAttemptLaunch{}, err
			}
		}
	}
	return s.Store.MarkExternalAttemptLaunched(ctx, attempt, now)
}

func (s *registeredAckCut) SettleExternalAttempt(ctx context.Context, settlement effects.Settlement) error {
	if settlement.OperationID == s.id && settlement.State == effects.StateSettled && !s.fired {
		if s.mode == "response_observed" {
			s.fired = true
			return errRegisteredAckCut
		}
		if s.mode == "settlement_ack_lost" {
			s.fired = true
			if err := s.Store.SettleExternalAttempt(ctx, settlement); err != nil {
				return err
			}
			return errRegisteredAckCut
		}
	}
	return s.Store.SettleExternalAttempt(ctx, settlement)
}

func requireRegisteredAckMessage(t *testing.T, ctx context.Context, peer *serveNativeProtocolPeer, binding *operatorchannel.Binding, id string) {
	t.Helper()
	destination, err := types.ParseJID(binding.ConversationRef)
	receiver, found := peer.receiver(destination)
	if err != nil || !found {
		t.Fatal("bound acknowledgement destination lacks its exact peer mapping", err)
	}
	select {
	case sent := <-peer.sent:
		if (sent.To.ToNonAD() != destination && sent.To.ToNonAD() != receiver.lid) ||
			sent.Body.GetConversation() != "Swarm operator successfully connected" || sent.ID != id {
			t.Fatalf("registered operator acknowledgement mismatch: id=%s to=%s body=%v", sent.ID, sent.To, sent.Body)
		}
	case <-ctx.Done():
		t.Fatal("registered binding did not send its acknowledgement")
	}
}

func requireRegisteredAckCut(t *testing.T, ctx context.Context, peer *serveNativeProtocolPeer, cut *registeredAckCut, config string, code int, stderr string) {
	t.Helper()
	if code == 0 || !cut.fired || cut.id == "" {
		t.Fatal("controlled acknowledgement cut did not stop connect", cut.mode, code, stderr)
	}
	outcome := cut.outcome
	if cut.readErr != nil || len(cut.bindings) != 1 || len(cut.operations) != 1 {
		t.Fatal("acknowledgement cut lost its live-owner snapshot", cut.readErr)
	}
	binding := cut.bindings[0]
	if cut.mode != "unbind" && binding.Status != operatorchannel.BindingCurrent {
		t.Fatal("acknowledgement failure revoked the committed binding")
	}
	if cut.mode == "response_observed" || cut.mode == "settlement_ack_lost" {
		requireRegisteredAckMessage(t, ctx, peer, &binding, strings.ReplaceAll(cut.id, "-", ""))
		want := effects.StateResponseObserved
		if cut.mode == "settlement_ack_lost" {
			want = effects.StateSettled
		}
		if outcome.AttemptState != want {
			t.Fatal("wrong postlaunch acknowledgement evidence", outcome)
		}
	} else if outcome.AttemptState != effects.StateAuthorized && outcome.AttemptState != effects.StateTerminalFailure {
		t.Fatal("prelaunch refusal acquired dispatch evidence", outcome)
	}
	select {
	case sent := <-peer.sent:
		t.Fatal("unexpected acknowledgement application send", cut.mode, sent.ID)
	default:
	}
	if cut.mode == "unbind" || cut.mode == "logout_pending" {
		return // These are real retirement writes, not permission to restore them.
	}
	var out, errOut bytes.Buffer
	next := cliapp.Execute(ctx, []string{"--config", config, "channel", "connect", "whatsapp", cut.operations[0].AccountReference, "--yes"}, &out, &errOut, nil, nil, OpenChannelRegistration)
	if cut.mode == "response_observed" {
		if next == 0 || !strings.Contains(errOut.String(), "refusing blind resend") {
			t.Fatal("uncertain acknowledgement did not refuse replay", next, errOut.String())
		}
	} else if next != 0 {
		t.Fatal("exact acknowledgement reconciliation failed", next, errOut.String())
	}
	if cut.mode == "prelaunch" {
		requireRegisteredAckMessage(t, ctx, peer, &binding, strings.ReplaceAll(cut.id, "-", ""))
	}
	select {
	case sent := <-peer.sent:
		t.Fatal("acknowledgement reconciliation duplicated a send", sent.ID)
	default:
	}
}
