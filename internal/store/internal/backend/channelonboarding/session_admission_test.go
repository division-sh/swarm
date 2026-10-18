package channelonboarding_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	domain "github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime/plangeneration"
	"github.com/google/uuid"
)

func sessionReservation(t *testing.T, principal string, now time.Time) domain.StartRequest {
	t.Helper()
	plan, err := plangeneration.FromCanonicalValue(map[string]string{"session": "account-admission"})
	if err != nil {
		t.Fatal(err)
	}
	id := uuid.NewString()
	return domain.StartRequest{
		OperationID: id, RequestKeyHash: id, RequestHash: id, PrincipalID: principal,
		Verb: domain.VerbConnect, Provider: "whatsapp",
		Interface: operatorchannel.InterfaceIdentity{InterfaceRef: operatorchannel.InterfaceHITLChannelV2,
			ChannelPackID: "provider.whatsapp.hitl_channel", ChannelPackVersion: "0.1.0",
			ChannelManifestHash: "sha256:session-manifest", SemanticGeneration: "sha256:session-plan"},
		Coordinate: domain.ChannelRuntimeContextCoordinate{BundleHash: "bundle-v2:sha256:" + strings.Repeat("a", 64),
			BundleIdentity: "session-proof", PackInventoryGeneration: "sha256:session-inventory",
			RuntimeInstanceID: uuid.NewString(), ContextPublicationGeneration: 1, PlanGeneration: plan},
		TargetSelector: "ingress:reception:whatsapp", Posture: domain.ActivationSessionConnection,
		Ceremony: domain.CeremonyAuthenticatedTextChallenge, RequestedAt: now,
	}
}

func TestSessionDeclarationReservesWithoutCredentialsOrBusinessTargetBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newCallbackLockFixture(t, backend)
			request := sessionReservation(t, f.principal, time.Now().UTC())
			op, err := f.selected.ReserveChannelOnboarding(context.Background(), request)
			if err != nil || op.Phase != domain.PhasePreparing || len(op.CredentialReservations) != 0 ||
				op.Coordinate.TargetGeneration != 0 || op.IdentityOperationID != "" || op.ActivationRevision != 0 {
				t.Fatalf("declaration-only reservation = %+v, %v", op, err)
			}
			again, err := f.selected.ReserveChannelOnboarding(context.Background(), request)
			if err != nil || again.OperationID != op.OperationID || again.Revision != op.Revision {
				t.Fatalf("exact reservation retry = %+v, %v", again, err)
			}
			request.OperationID, request.RequestKeyHash = uuid.NewString(), uuid.NewString()
			request.Posture = domain.ActivationWebhookRegistration
			if _, err := f.selected.ReserveChannelOnboarding(context.Background(), request); !errors.Is(err, domain.ErrInvalidRequest) {
				t.Fatalf("webhook accepted a credential waiver: %v", err)
			}
		})
	}
}

func TestSessionConnectionReservationPrecedesPairingBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newCallbackLockFixture(t, backend)
			request := sessionReservation(t, f.principal, time.Now().UTC())
			op, err := f.selected.ReserveChannelOnboarding(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			if uuid.Validate(op.SessionConnectionID) != nil || op.SessionConnectionID == op.OperationID {
				t.Fatal("selected onboarding has no durable connection identity before provider pairing")
			}
			for range 3 {
				request.OperationID = uuid.NewString()
				again, err := f.selected.ReserveChannelOnboarding(context.Background(), request)
				if err != nil || again.SessionConnectionID != op.SessionConnectionID || again.OperationID != op.OperationID {
					t.Fatal("request replay changed reserved connection identity", err)
				}
			}
			readback, err := f.selected.GetChannelOnboarding(context.Background(), op.OperationID)
			if err != nil || readback.SessionConnectionID != op.SessionConnectionID ||
				readback.SessionAccount != (operatorchannel.SessionAccountAdmission{}) || readback.ActivationRevision != 0 {
				t.Fatal("reservation manufactured account or executable authority", err)
			}
		})
	}
}

func TestSessionAccountEvidencePersistenceAndRevisionFencesBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newCallbackLockFixture(t, backend)
			ctx, now := context.Background(), time.Now().UTC()
			op, err := f.selected.ReserveChannelOnboarding(ctx, sessionReservation(t, f.principal, now))
			if err != nil {
				t.Fatal(err)
			}
			for _, phase := range []domain.Phase{domain.PhaseCredentialsAdmitted, domain.PhaseActivatingProvider} {
				op, err = f.selected.AdvanceChannelOnboarding(ctx, domain.AdvanceRequest{OperationID: op.OperationID, ExpectedRevision: op.Revision, Phase: phase, Now: now})
				if err != nil || op.Coordinate.TargetGeneration != 0 {
					t.Fatalf("runless pairing phase %s: %+v %v", phase, op, err)
				}
			}
			missing := domain.AdvanceRequest{OperationID: op.OperationID, ExpectedRevision: op.Revision, Phase: domain.PhaseAwaitingExternalIdentity, Now: now}
			if _, err := f.selected.AdvanceChannelOnboarding(ctx, missing); !errors.Is(err, domain.ErrInvalidRequest) {
				t.Fatalf("operator identity admitted before pairing: %v", err)
			}
			before, err := f.selected.GetChannelOnboarding(ctx, op.OperationID)
			if err != nil || before.Revision != op.Revision || before.SessionAccount != (operatorchannel.SessionAccountAdmission{}) {
				t.Fatalf("rejected pairing mutated responsibility: %+v %v", before, err)
			}
			account := operatorchannel.SessionAccountAdmission{Provider: "whatsapp", ConnectionID: op.SessionConnectionID,
				AccountRef: "15551234567@s.whatsapp.net", AdmissionID: uuid.NewString(), Revision: 1}
			admit := missing
			foreign := account
			foreign.ConnectionID = uuid.NewString()
			admit.SessionAccount = &foreign
			if _, err := f.selected.AdvanceChannelOnboarding(ctx, admit); !errors.Is(err, domain.ErrConflict) {
				t.Fatal("pairing adopted another connection reservation", err)
			}
			unchanged, err := f.selected.GetChannelOnboarding(ctx, op.OperationID)
			if err != nil || unchanged.Revision != op.Revision || unchanged.SessionConnectionID != op.SessionConnectionID ||
				unchanged.SessionAccount != (operatorchannel.SessionAccountAdmission{}) {
				t.Fatal("rejected account adoption changed reservation or evidence", err)
			}
			admit.SessionAccount = &account
			op, err = f.selected.AdvanceChannelOnboarding(ctx, admit)
			if err != nil || op.SessionAccount != account || op.Coordinate.TargetGeneration != 0 {
				t.Fatalf("retained pairing evidence: %+v %v", op, err)
			}
			if _, err := f.selected.AdvanceChannelOnboarding(ctx, admit); !errors.Is(err, domain.ErrRevisionConflict) {
				t.Fatalf("stale pairing completion repeated: %v", err)
			}
			readback, err := f.selected.GetChannelOnboarding(ctx, op.OperationID)
			if err != nil || readback.SessionAccount != account || readback.Revision != op.Revision {
				t.Fatalf("pairing evidence did not persist: %+v %v", readback, err)
			}
			responsibility := domain.AdmissionResponsibility{OperationID: op.OperationID, OperationRevision: op.Revision,
				Coordinate: op.Coordinate, TargetSelector: op.TargetSelector,
				Provider: op.Provider, SessionAccount: account}
			if current, err := domain.AdmissionResponsibilityCurrent(ctx, f.selected, responsibility, true); err != nil || !current {
				t.Fatalf("exact retained responsibility: %t %v", current, err)
			}
			for _, mutate := range []func(*domain.AdmissionResponsibility){
				func(r *domain.AdmissionResponsibility) { r.Coordinate.RuntimeInstanceID = uuid.NewString() },
				func(r *domain.AdmissionResponsibility) { r.Coordinate.ContextPublicationGeneration++ },
				func(r *domain.AdmissionResponsibility) {
					r.Coordinate.BundleHash = "bundle-v2:sha256:" + strings.Repeat("b", 64)
				},
				func(r *domain.AdmissionResponsibility) { r.Coordinate.PackInventoryGeneration = "other" },
				func(r *domain.AdmissionResponsibility) { r.Coordinate.TargetGeneration++ },
				func(r *domain.AdmissionResponsibility) { r.OperationRevision++ },
				func(r *domain.AdmissionResponsibility) { r.ActivationRevision = 1 },
			} {
				foreign := responsibility
				mutate(&foreign)
				if current, err := domain.AdmissionResponsibilityCurrent(ctx, f.selected, foreign, true); err != nil || current {
					t.Fatalf("foreign declaration occurrence passed fence: %t %v", current, err)
				}
			}
			for _, mutation := range []struct {
				name string
				edit func(*operatorchannel.SessionAccountAdmission)
			}{
				{"provider", func(a *operatorchannel.SessionAccountAdmission) { a.Provider = "other" }},
				{"connection", func(a *operatorchannel.SessionAccountAdmission) { a.ConnectionID = uuid.NewString() }},
				{"account", func(a *operatorchannel.SessionAccountAdmission) { a.AccountRef = "15557654321@s.whatsapp.net" }},
				{"admission", func(a *operatorchannel.SessionAccountAdmission) { a.AdmissionID = uuid.NewString() }},
				{"revision", func(a *operatorchannel.SessionAccountAdmission) { a.Revision++ }},
			} {
				t.Run(mutation.name, func(t *testing.T) {
					changed := account
					mutation.edit(&changed)
					foreign := responsibility
					foreign.SessionAccount = changed
					if current, err := domain.AdmissionResponsibilityCurrent(ctx, f.selected, foreign, true); err != nil || current {
						t.Fatalf("foreign account passed responsibility fence: %t %v", current, err)
					}
					if _, err := f.selected.AdvanceChannelOnboarding(ctx, domain.AdvanceRequest{OperationID: op.OperationID, ExpectedRevision: op.Revision, Phase: op.Phase, SessionAccount: &changed, Now: now}); !errors.Is(err, domain.ErrConflict) {
						t.Fatalf("replacement account adopted: %v", err)
					}
				})
			}
			op, err = f.selected.AdvanceChannelOnboarding(ctx, domain.AdvanceRequest{OperationID: op.OperationID, ExpectedRevision: op.Revision, Phase: domain.PhaseAwaitingOperatorConfirmation, Now: now})
			if err != nil || op.SessionAccount != account || op.Coordinate.TargetGeneration != 0 {
				t.Fatalf("runless claim: %+v %v", op, err)
			}
			if _, err := f.selected.AdvanceChannelOnboarding(ctx, domain.AdvanceRequest{OperationID: op.OperationID, ExpectedRevision: op.Revision, Phase: domain.PhasePublishingActivation, Now: now}); !errors.Is(err, domain.ErrInvalidRequest) {
				t.Fatalf("pairing evidence granted an executable target: %v", err)
			}
			authority := operatorchannel.ProviderAuthority{Kind: operatorchannel.ProviderAuthoritySession, Session: account}
			if err := authority.RequireExecutable(); err == nil {
				t.Fatal("stored account DTO granted execution")
			}
		})
	}
}
