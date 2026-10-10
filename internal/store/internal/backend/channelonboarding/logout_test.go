package channelonboarding_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	domain "github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/google/uuid"
)

func pairedLogoutOperation(t *testing.T, f callbackLockFixture) domain.Operation {
	return pairedLogoutRequestOperation(t, f, sessionReservation(t, f.principal, time.Now().UTC()))
}

func pairedLogoutRequestOperation(t *testing.T, f callbackLockFixture, request domain.StartRequest) domain.Operation {
	t.Helper()
	ctx, now := context.Background(), time.Now().UTC()
	op, err := f.selected.ReserveChannelOnboarding(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	for _, phase := range []domain.Phase{domain.PhaseCredentialsAdmitted, domain.PhaseActivatingProvider} {
		op, err = f.selected.AdvanceChannelOnboarding(ctx, domain.AdvanceRequest{
			OperationID: op.OperationID, ExpectedRevision: op.Revision, Phase: phase, Now: now,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	account := operatorchannel.SessionAccountAdmission{Provider: "whatsapp", ConnectionID: op.SessionConnectionID,
		AccountRef: "15551234567@s.whatsapp.net", AdmissionID: uuid.NewString(), Revision: 1}
	op, err = f.selected.AdvanceChannelOnboarding(ctx, domain.AdvanceRequest{
		OperationID: op.OperationID, ExpectedRevision: op.Revision, Phase: domain.PhaseAwaitingExternalIdentity,
		SessionAccount: &account, Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	return op
}

func logoutReservation(op domain.Operation) domain.ReserveTeardownRequest {
	return domain.ReserveTeardownRequest{
		TeardownID: uuid.NewString(), Kind: domain.TeardownLogout, PrincipalID: op.PrincipalID,
		RequestKeyHash: uuid.NewString(), RequestHash: uuid.NewString(),
		Scope: domain.TeardownScope{Interface: op.Interface}, RequestedAt: time.Now().UTC(),
		Logout: &domain.SessionLogoutTarget{OperationID: op.OperationID, OperationRevision: op.Revision,
			OccurrenceID: uuid.NewString(), Account: op.SessionAccount, Coordinate: op.Coordinate, TargetSelector: op.TargetSelector},
	}
}

func TestSessionLogoutReservationFreezesAndFencesBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newCallbackLockFixture(t, backend)
			op := pairedLogoutOperation(t, f)
			request := logoutReservation(op)
			ctx := context.Background()
			teardown, err := f.selected.ReserveChannelTeardown(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			if teardown.Kind != domain.TeardownLogout || teardown.Phase != domain.TeardownAuthorityRetired ||
				teardown.Logout == nil || *teardown.Logout != *request.Logout || teardown.RetiredOperations != 1 {
				t.Fatalf("logout did not retain exact fenced responsibility: %+v", teardown)
			}
			retired, err := f.selected.GetChannelOnboarding(ctx, op.OperationID)
			if err != nil || retired.Phase != domain.PhaseRetired || retired.Revision != op.Revision+1 ||
				retired.SessionAccount != op.SessionAccount || retired.SessionConnectionID != op.SessionConnectionID {
				t.Fatalf("logout discarded pairing evidence or left execution live: %+v, %v", retired, err)
			}
			if _, err := f.selected.AdvanceChannelOnboarding(ctx, domain.AdvanceRequest{
				OperationID: retired.OperationID, ExpectedRevision: retired.Revision,
				Phase: domain.PhaseAwaitingExternalIdentity, Now: time.Now().UTC(),
			}); !errors.Is(err, domain.ErrConflict) {
				t.Fatalf("retired session became executable again: %v", err)
			}
			request.TeardownID = uuid.NewString()
			replay, err := f.selected.ReserveChannelTeardown(ctx, request)
			if err != nil || replay.TeardownID != teardown.TeardownID || replay.Revision != teardown.Revision ||
				replay.Logout == nil || *replay.Logout != *teardown.Logout {
				t.Fatalf("exact replay changed responsibility: %+v, %v", replay, err)
			}
			request.RequestHash = "changed"
			if _, err := f.selected.ReserveChannelTeardown(ctx, request); !errors.Is(err, domain.ErrConflict) {
				t.Fatalf("changed replay accepted: %v", err)
			}
			request.RequestHash, request.RequestKeyHash = teardown.RequestHash, uuid.NewString()
			if _, err := f.selected.ReserveChannelTeardown(ctx, request); !errors.Is(err, domain.ErrConflict) {
				t.Fatalf("new key adopted an existing logout: %v", err)
			}
			readback, err := f.selected.GetChannelTeardown(ctx, teardown.TeardownID)
			if err != nil || readback.Logout == nil || *readback.Logout != *teardown.Logout {
				t.Fatalf("frozen logout readback: %+v, %v", readback, err)
			}
			encoded, err := json.Marshal(readback)
			if err != nil || strings.Contains(string(encoded), op.SessionAccount.AccountRef) ||
				strings.Contains(string(encoded), op.SessionConnectionID) || strings.Contains(string(encoded), request.Logout.OccurrenceID) {
				t.Fatalf("public teardown leaks session material: %s, %v", encoded, err)
			}
		})
	}
}

func TestSessionLogoutReservationRejectsContradictionsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newCallbackLockFixture(t, backend)
			op := pairedLogoutOperation(t, f)
			for _, tc := range []struct {
				name string
				edit func(*domain.ReserveTeardownRequest)
			}{
				{"principal", func(r *domain.ReserveTeardownRequest) { r.PrincipalID = uuid.NewString() }},
				{"revision", func(r *domain.ReserveTeardownRequest) { r.Logout.OperationRevision++ }},
				{"operation", func(r *domain.ReserveTeardownRequest) { r.Logout.OperationID = uuid.NewString() }},
				{"interface", func(r *domain.ReserveTeardownRequest) { r.Scope.Interface.ChannelPackVersion = "9.0.0" }},
				{"connection", func(r *domain.ReserveTeardownRequest) { r.Logout.Account.ConnectionID = uuid.NewString() }},
				{"account", func(r *domain.ReserveTeardownRequest) { r.Logout.Account.AccountRef = "15557654321@s.whatsapp.net" }},
				{"admission", func(r *domain.ReserveTeardownRequest) { r.Logout.Account.AdmissionID = uuid.NewString() }},
				{"account_revision", func(r *domain.ReserveTeardownRequest) { r.Logout.Account.Revision++ }},
				{"source", func(r *domain.ReserveTeardownRequest) {
					r.Logout.Coordinate.BundleHash = "bundle-v2:sha256:" + strings.Repeat("b", 64)
				}},
				{"runtime", func(r *domain.ReserveTeardownRequest) { r.Logout.Coordinate.RuntimeInstanceID = uuid.NewString() }},
				{"publication", func(r *domain.ReserveTeardownRequest) { r.Logout.Coordinate.ContextPublicationGeneration++ }},
				{"target", func(r *domain.ReserveTeardownRequest) { r.Logout.Coordinate.TargetGeneration++ }},
				{"selector", func(r *domain.ReserveTeardownRequest) { r.Logout.TargetSelector += "-other" }},
			} {
				t.Run(tc.name, func(t *testing.T) {
					request := logoutReservation(op)
					tc.edit(&request)
					if _, err := f.selected.ReserveChannelTeardown(context.Background(), request); err == nil {
						t.Fatal("contradictory logout target admitted")
					}
					current, err := f.selected.GetChannelOnboarding(context.Background(), op.OperationID)
					if err != nil || !reflect.DeepEqual(current, op) {
						t.Fatal("rejected logout mutated retained operation", err)
					}
					rows, err := f.selected.ListChannelTeardowns(context.Background())
					if err != nil || len(rows) != 0 {
						t.Fatal("rejected logout left durable responsibility", err)
					}
				})
			}
		})
	}
}

func TestSessionLogoutReservationOverlapsAndRollbackBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, first := range []domain.TeardownKind{domain.TeardownLogout, domain.TeardownUnbind, domain.TeardownContextRetirement} {
			t.Run(backend+"/"+string(first), func(t *testing.T) {
				f := newCallbackLockFixture(t, backend)
				op := pairedLogoutOperation(t, f)
				logout := logoutReservation(op)
				ordinary := domain.ReserveTeardownRequest{TeardownID: uuid.NewString(), RequestKeyHash: uuid.NewString(), RequestHash: uuid.NewString(),
					Kind: domain.TeardownUnbind, PrincipalID: f.principal, Scope: domain.TeardownScope{Interface: op.Interface},
					ExpectedBindingRevision: 1, RequestedAt: time.Now().UTC()}
				if first == domain.TeardownContextRetirement {
					ordinary.Kind, ordinary.ExpectedBindingRevision = first, 0
					ordinary.Scope = domain.TeardownScope{BundleHash: op.Coordinate.BundleHash, ContextPublicationGeneration: op.Coordinate.ContextPublicationGeneration}
				}
				one, two := ordinary, logout
				if first == domain.TeardownLogout {
					one, two = logout, ordinary
				}
				reserved, err := f.selected.ReserveChannelTeardown(context.Background(), one)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := f.selected.ReserveChannelTeardown(context.Background(), two); !errors.Is(err, domain.ErrConflict) {
					t.Fatalf("overlapping destructive responsibility admitted: %v", err)
				}
				if first == domain.TeardownLogout {
					return
				}
				if _, err := f.selected.CompleteChannelTeardown(context.Background(), domain.CompleteTeardownRequest{
					TeardownID: reserved.TeardownID, ExpectedRevision: reserved.Revision, Succeeded: false, Now: time.Now().UTC(),
				}); err != nil {
					t.Fatal(err)
				}
				// A reused primary key fails after the logout fence has run. The
				// selected transaction must roll back that fence, not strand it.
				logout.TeardownID = reserved.TeardownID
				if _, err := f.selected.ReserveChannelTeardown(context.Background(), logout); err == nil {
					t.Fatal("duplicate teardown identity accepted")
				}
				current, err := f.selected.GetChannelOnboarding(context.Background(), op.OperationID)
				if err != nil || !reflect.DeepEqual(current, op) {
					t.Fatal("failed responsibility insert left execution fenced", err)
				}
				logout.TeardownID = uuid.NewString()
				if _, err := f.selected.ReserveChannelTeardown(context.Background(), logout); err != nil {
					t.Fatal("clean rollback lost the original exact target", err)
				}
			})
		}
	}
}

func TestSessionLogoutConcurrentExactReservationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newCallbackLockFixture(t, backend)
			op := pairedLogoutOperation(t, f)
			request := logoutReservation(op)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			var workers sync.WaitGroup
			start := make(chan struct{})
			results := make(chan domain.TeardownOperation, 2)
			failures := make(chan error, 2)
			for range 2 {
				workers.Add(1)
				go func() {
					defer workers.Done()
					<-start
					replay := request
					replay.TeardownID = uuid.NewString()
					result, err := f.selected.ReserveChannelTeardown(ctx, replay)
					results <- result
					failures <- err
				}()
			}
			close(start)
			workers.Wait()
			for range 2 {
				if err := <-failures; err != nil {
					t.Fatal("concurrent exact caller lost responsibility", err)
				}
			}
			one, two := <-results, <-results
			if one.TeardownID != two.TeardownID || one.Revision != two.Revision || one.Logout == nil || two.Logout == nil || *one.Logout != *two.Logout {
				t.Fatal("concurrent callers created distinct responsibilities")
			}
		})
	}
}

func TestSessionLogoutFencesCurrentActivationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newCallbackLockFixture(t, backend)
			ctx, now := context.Background(), time.Now().UTC()
			op := pairedLogoutOperation(t, f)
			coordinate := op.Coordinate
			coordinate.TargetGeneration = 1
			var err error
			op, err = f.selected.AdvanceChannelOnboarding(ctx, domain.AdvanceRequest{OperationID: op.OperationID,
				ExpectedRevision: op.Revision, Phase: domain.PhaseAwaitingOperatorConfirmation, BindingRevision: 1, Now: now})
			if err != nil {
				t.Fatal(err)
			}
			op, err = f.selected.AdvanceChannelOnboarding(ctx, domain.AdvanceRequest{OperationID: op.OperationID,
				ExpectedRevision: op.Revision, Phase: domain.PhasePublishingActivation, RebindCoordinate: &coordinate, Now: now})
			if err != nil {
				t.Fatal(err)
			}
			op, activation, err := f.selected.PublishConnectedChannelActivation(ctx, domain.PublishActivationRequest{
				OperationID: op.OperationID, ExpectedRevision: op.Revision, ActivationID: uuid.NewString(),
				BindingRevision: 1, ConversationRef: "15551234567@s.whatsapp.net", Now: now})
			if err != nil {
				t.Fatal(err)
			}
			f.finish(t, op.OperationID, now)
			op, err = f.selected.GetChannelOnboarding(ctx, op.OperationID)
			if err != nil {
				t.Fatal(err)
			}
			logout, err := f.selected.ReserveChannelTeardown(ctx, logoutReservation(op))
			if err != nil || logout.RetiredOperations != 1 || logout.RetiredActivations != 1 {
				t.Fatalf("current executable consumers were not fenced: %+v, %v", logout, err)
			}
			current, err := f.selected.ListCurrentConnectedChannelActivations(ctx)
			if err != nil || len(current) != 0 {
				t.Fatalf("activation remains current: %+v, %v", current, err)
			}
			if _, _, err := f.selected.PublishConnectedChannelActivation(ctx, domain.PublishActivationRequest{
				OperationID: op.OperationID, ExpectedRevision: op.Revision, ActivationID: uuid.NewString(),
				BindingRevision: 1, ConversationRef: activation.ConversationRef, Now: now,
			}); err == nil {
				t.Fatal("logout permitted standing republication")
			}
			if _, err := f.selected.CompleteChannelTeardown(ctx, domain.CompleteTeardownRequest{
				TeardownID: logout.TeardownID, ExpectedRevision: logout.Revision, Succeeded: true, Now: now,
			}); !errors.Is(err, domain.ErrConflict) {
				t.Fatal("unjournaled logout became successful", err)
			}
		})
	}
}

type logoutRecoveryIdentity struct {
	domain.DestructiveIdentityLifecycle
	principal string
}

func (i logoutRecoveryIdentity) Principal() (operatorchannel.Principal, error) {
	return operatorchannel.Principal{ID: i.principal}, nil
}

type logoutRecoveryCleanup struct{}

func (logoutRecoveryCleanup) ReleaseOperation(context.Context, domain.Operation, ...credentials.ValueEvidence) error {
	return errors.New("logout must not use credential teardown")
}

func (logoutRecoveryCleanup) RefreshChannelActivations(context.Context) error {
	return errors.New("logout must not disconnect through ordinary activation retirement")
}

type logoutRecoveryProbe struct {
	prepared   int
	dispatched []domain.TeardownOperation
}

func (p *logoutRecoveryProbe) PrepareSessionLogout(context.Context, domain.Operation) (domain.SessionLogoutTarget, error) {
	p.prepared++
	return domain.SessionLogoutTarget{}, errors.New("recovery must not select a successor SDK occurrence")
}

func (p *logoutRecoveryProbe) DispatchSessionLogout(ctx context.Context, op domain.TeardownOperation) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p.dispatched = append(p.dispatched, op)
	return nil
}

func TestSessionLogoutRecoveryConsumesFrozenResponsibilityBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newCallbackLockFixture(t, backend)
			op := pairedLogoutOperation(t, f)
			request := logoutReservation(op)
			teardown, err := f.selected.ReserveChannelTeardown(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			probe := &logoutRecoveryProbe{}
			service, err := domain.NewDestructiveService(f.selected, logoutRecoveryIdentity{principal: f.principal}, logoutRecoveryCleanup{}, logoutRecoveryCleanup{}, nil, nil, probe)
			if err != nil {
				t.Fatal(err)
			}
			for range 2 {
				if err := service.Recover(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			if probe.prepared != 0 || len(probe.dispatched) != 2 {
				t.Fatal("recovery re-selected or lost its responsibility")
			}
			for _, resumed := range probe.dispatched {
				if resumed.TeardownID != teardown.TeardownID || resumed.Logout == nil || *resumed.Logout != *request.Logout {
					t.Fatal("recovery rewrote frozen session evidence")
				}
			}
			// The adapter above is an observation probe, not a journal or SDK
			// implementation. Recovery cannot fabricate a successful outcome.
			current, err := f.selected.GetChannelTeardown(context.Background(), teardown.TeardownID)
			if err != nil || current.Phase != domain.TeardownAuthorityRetired || current.Revision != teardown.Revision {
				t.Fatal("recovery fabricated settlement", err)
			}
			readback, err := domain.NewSessionLogoutReadback(current)
			if err != nil || readback.OperationID != op.OperationID || readback.ExpectedRevision != op.Revision {
				t.Fatal("recovery lost authenticated operation readback", err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if err := service.Recover(ctx); !errors.Is(err, context.Canceled) {
				t.Fatal("canceled recovery reached dispatch", err)
			}
		})
	}
}
