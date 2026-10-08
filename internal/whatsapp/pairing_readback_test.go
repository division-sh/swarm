package whatsapp

import (
	"context"
	"errors"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/google/uuid"
	"go.mau.fi/whatsmeow/types/events"
)

// Retained operation and upstream request authentication are finite inputs in
// this isolated disclosure proof, not a supported authenticated API journey.
type pairingReadbackFixture struct {
	op    channelonboarding.Operation
	err   error
	reads int
}

func (r *pairingReadbackFixture) GetChannelOnboarding(_ context.Context, id string) (channelonboarding.Operation, error) {
	r.reads++
	if id != r.op.OperationID {
		return channelonboarding.Operation{}, errPairingScope
	}
	return r.op, r.err
}

func TestWhatsAppPairingReadbackAuthorizationIsOriginalScopeOnly(t *testing.T) {
	for _, cell := range []string{"allowed", "foreign_principal", "missing_principal", "foreign_operation", "foreign_source",
		"successor_context", "expired_operation", "canceled_operation", "completed_operation", "wrong_provider", "wrong_transport", "io_failure", "canceled_request"} {
		t.Run(cell, func(t *testing.T) {
			scope := pairingScopeFixture(t)
			q, err := newPairingQR(context.Background(), scope)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := q.join(context.Background()); err != nil {
					t.Error(err)
				}
			}()
			const material = "private QR material must not enter errors"
			if err := q.handle(&events.QR{Codes: []string{material}}); err != nil {
				t.Fatal(err)
			}
			principal := operatorchannel.Principal{ID: scope.PrincipalID, CreatedAt: time.Now().UTC()}
			reader := &pairingReadbackFixture{op: channelonboarding.Operation{OperationID: scope.OperationID,
				PrincipalID: scope.PrincipalID, Provider: "whatsapp", Coordinate: scope.Coordinate,
				Posture: channelonboarding.ActivationSessionConnection, Ceremony: channelonboarding.CeremonyAuthenticatedTextChallenge,
				Phase: channelonboarding.PhaseCredentialsAdmitted, Revision: 1, RequestedAt: time.Now().UTC()}}
			coordinate := scope.Coordinate
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch cell {
			case "foreign_principal":
				principal.ID = uuid.NewString()
			case "missing_principal":
				principal.ID = ""
			case "foreign_operation":
				reader.op.PrincipalID = uuid.NewString()
			case "foreign_source":
				reader.op.Coordinate.BundleIdentity = "another source"
			case "successor_context":
				coordinate.ContextPublicationGeneration++
			case "expired_operation":
				reader.op.Phase, reader.op.FailureCode = channelonboarding.PhaseFailed, "operation_expired"
			case "canceled_operation":
				reader.op.Phase = channelonboarding.PhaseRetired
			case "completed_operation":
				reader.op.CompletedAt = time.Now().UTC()
			case "wrong_provider":
				reader.op.Provider = "telegram"
			case "wrong_transport":
				reader.op.Posture = channelonboarding.ActivationWebhookRegistration
			case "io_failure":
				reader.err = errors.New("retained operation unavailable")
			case "canceled_request":
				cancel()
			}
			result, err := q.readAuthorized(ctx, principal, reader, coordinate)
			if cell == "allowed" {
				if err != nil || result.Code != material || result.Paired || result.Connected {
					t.Fatalf("authorized projection changed observations: %+v %v", result, err)
				}
			} else if err == nil || result.Code != "" || strings.Contains(err.Error(), material) {
				t.Fatal("refused disclosure exposed material or reported success")
			}
		})
	}
}

func TestWhatsAppPairingReadbackExpiryAndObservationDoNotGrantExecution(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		scope := pairingScopeFixture(t)
		q, err := newPairingQR(context.Background(), scope)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := q.join(context.Background()); err != nil {
				t.Error(err)
			}
		}()
		principal := operatorchannel.Principal{ID: scope.PrincipalID, CreatedAt: time.Now().UTC()}
		reader := &pairingReadbackFixture{op: channelonboarding.Operation{OperationID: scope.OperationID,
			PrincipalID: scope.PrincipalID, Provider: "whatsapp", Coordinate: scope.Coordinate,
			Posture: channelonboarding.ActivationSessionConnection, Ceremony: channelonboarding.CeremonyAuthenticatedTextChallenge,
			Phase: channelonboarding.PhaseCredentialsAdmitted, Revision: 1, RequestedAt: time.Now().UTC()}}
		if err := q.handle(&events.QR{Codes: []string{"first"}}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(firstPairingQRLifetime)
		synctest.Wait()
		result, err := q.readAuthorized(context.Background(), principal, reader, scope.Coordinate)
		if err != nil || result.Status != pairingExpired || result.Code != "" {
			t.Fatal("expired QR survived authorized readback", err)
		}
		if err := q.handle(&events.PairSuccess{}); err != nil {
			t.Fatal(err)
		}
		result, err = q.readAuthorized(context.Background(), principal, reader, scope.Coordinate)
		if err != nil || !result.Paired || result.Connected || result.Code != "" {
			t.Fatal("pair success invented connected health", err)
		}
		if err := q.handle(&events.Connected{}); err != nil {
			t.Fatal(err)
		}
		result, err = q.readAuthorized(context.Background(), principal, reader, scope.Coordinate)
		if err != nil || !result.Paired || !result.Connected || result.Code != "" {
			t.Fatal("connected observation retained pairing material", err)
		}
		authority := operatorchannel.ProviderAuthority{Kind: operatorchannel.ProviderAuthoritySession, Session: captureFixture(t).Scope.Session}
		if err := authority.Validate(); err != nil {
			t.Fatal(err)
		}
		var unavailable *operatorchannel.SessionProviderUnavailableError
		if err := authority.RequireExecutable(); !errors.As(err, &unavailable) {
			t.Fatal("isolated pairing/readback installed executable session authority", err)
		}
	})
}
