//go:build linux || darwin

package serveapp

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/google/uuid"
)

type serveSessionObserver interface {
	ObserveSession(context.Context, channelonboarding.Operation) (operatorchannel.ProviderAuthority, operatorchannel.SessionConnectionObservation, bool, error)
}

func pairedServeSessionObserver(t *testing.T, backend string) (*serveBootstrapTestFixture, channelonboarding.Operation, serveSessionObserver) {
	t.Helper()
	f := newServeBootstrapTestFixture(t, backend)
	serveBootstrapWireFixture(t, "paired", f.owner)
	seedServePairedDevice(t, f)
	if err := f.adapter.ResumeSession(f.ctx, f.op, f.candidate); err != nil {
		t.Fatal(err)
	}
	wait, stop := context.WithTimeout(f.ctx, 5*time.Second)
	defer stop()
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	var op channelonboarding.Operation
	for {
		current, paired, err := f.adapter.CheckpointSessionPairing(wait, f.op)
		if err != nil {
			t.Fatal("actual SDK pairing did not checkpoint", err)
		}
		if paired {
			op = current
			break
		}
		select {
		case <-tick.C:
		case <-wait.Done():
			t.Fatal("SDK login did not complete within the existing fixture bound", context.Cause(wait))
		}
	}
	coordinate := op.Coordinate
	coordinate.TargetGeneration = 1
	op, err := f.adapter.store.AdvanceChannelOnboarding(f.ctx, channelonboarding.AdvanceRequest{
		OperationID: op.OperationID, ExpectedRevision: op.Revision, Phase: op.Phase, RebindCoordinate: &coordinate, Now: time.Now().UTC()})
	if err != nil {
		t.Fatal(err)
	}
	observer, ok := any(f.adapter).(serveSessionObserver)
	if !ok {
		t.Fatal("the concrete serve owner does not expose owned session observation")
	}
	return f, op, observer
}

func TestServePairedSessionObservationCancelsPendingResumeBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, op, observer := pairedServeSessionObserver(t, backend)
			if err := f.adapter.connection(op.OperationID).Close(f.ctx); err != nil {
				t.Fatal(err)
			}
			entered := serveBootstrapWireFixture(t, "blocked_handshake", f.owner)
			caller, cancel := context.WithCancel(f.ctx)
			defer cancel()
			result := make(chan error, 1)
			go func() { result <- f.adapter.ResumeSession(caller, op, f.candidate) }()
			select {
			case <-entered:
			case err := <-result:
				t.Fatal("paired resume did not retain its pending original attempt", err)
			case <-time.After(5 * time.Second):
				t.Fatal("paired resume did not reach the SDK handshake")
			}
			wait, stop := context.WithTimeout(f.ctx, 50*time.Millisecond)
			provider, observation, current, err := observer.ObserveSession(wait, op)
			provider.CloseExecution()
			stop()
			if !errors.Is(err, context.DeadlineExceeded) || current || observation.Connected {
				t.Fatal("pending paired readiness ignored cancellation or granted health", err, current, observation)
			}
			cancel()
			select {
			case err := <-result:
				if !errors.Is(err, context.Canceled) {
					t.Fatal("original paired resume lost caller cancellation", err)
				}
			case <-time.After(time.Second):
				t.Fatal("paired resume caller waits inline for retained cleanup")
			}
			join, finish := context.WithTimeout(f.ctx, 30*time.Second)
			defer finish()
			if err := f.adapter.connection(op.OperationID).Close(join); err != nil {
				t.Fatal("paired observation discarded original cleanup ownership", err)
			}
			if err := f.owner.WaitForQuiescence(join); err != nil {
				t.Fatal("paired observation released unresolved original work", err)
			}
		})
	}
}

func TestServeSessionReadinessUsesOriginalOwnedObservationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, op, observer := pairedServeSessionObserver(t, backend)
			provider, observation, current, err := observer.ObserveSession(f.ctx, op)
			defer provider.CloseExecution()
			if err != nil || !current || provider.RequireExecutable() != nil || observation.Validate() != nil || !observation.Connected ||
				observation.Admission != op.SessionAccount || !observation.ObservedAt.After(time.Time{}) {
				t.Fatal("readiness did not consume genuine connected account evidence", current, observation, err)
			}
			if id, revision := provider.SessionParent(); id != op.OperationID || revision != op.Revision {
				t.Fatal("health adopted a different selected parent", id, revision)
			}
			readiness := &serveConnectedChannelReadiness{sessions: f.adapter}
			activation := channelonboarding.ConnectedChannelActivation{OperationID: op.OperationID, PrincipalID: op.PrincipalID,
				Posture: op.Posture, Provider: op.Provider, Interface: op.Interface, Revision: op.ActivationRevision,
				BindingRevision: op.BindingRevision, SessionAccount: op.SessionAccount, Coordinate: op.Coordinate}
			joined := channelonboarding.ReadinessFacts{}
			if current, err := readiness.observeSessionReadiness(f.ctx, op, activation, &joined); err != nil || !current ||
				joined.SessionAuthority.RequireExecutable() != nil || joined.SessionObservation == nil ||
				joined.ObservedAt != joined.SessionObservation.ObservedAt || joined.SessionObservation.Admission != op.SessionAccount {
				joined.SessionAuthority.CloseExecution()
				t.Fatal("serve readback did not consume its original session owner", current, err)
			}
			joined.SessionAuthority.CloseExecution()
			// The other readiness gates are pure projection inputs here, not a
			// claimed served activation, confirmation receipt or target admission.
			publication, err := channelonboarding.NewChannelActivationPublication(nil)
			if err != nil {
				t.Fatal(err)
			}
			coordinate := op.Coordinate
			coordinate.TargetGeneration = 1
			facts := channelonboarding.ReadinessFacts{Coordinate: coordinate, Interface: op.Interface,
				PlanGeneration: coordinate.PlanGeneration, ActivationGeneration: publication.Generation(),
				ActivationRevision: 1, ActivationCurrent: true, BindingRevision: 1, ExpectedBindingRevision: 1,
				CredentialsCurrent: true, ConfirmationTerminalSuccess: true, ConfirmationActivationRevision: 1, ConfirmationBindingRevision: 1,
				Posture: channelonboarding.ActivationSessionConnection, SessionAuthority: provider, SessionObservation: &observation,
				TargetGeneration: coordinate.TargetGeneration, ExpectedTargetGeneration: coordinate.TargetGeneration,
				ObservedAt: observation.ObservedAt}
			if projection := channelonboarding.ProjectReadiness(facts); !projection.Ready {
				t.Fatal("owned observation did not satisfy the canonical health gate", projection)
			}
			provider.CloseExecution()
			if projection := channelonboarding.ProjectReadiness(facts); projection.Ready || projection.Reason != channelonboarding.ReadinessSessionUnavailable {
				t.Fatal("released admission remained readiness authority", projection)
			}
			if err := f.adapter.connection(op.OperationID).Close(f.ctx); err != nil {
				t.Fatal(err)
			}
			provider, observation, current, err = observer.ObserveSession(f.ctx, op)
			provider.CloseExecution()
			if err != nil || current || observation.Connected {
				t.Fatal("closed cached ownership reported live health", current, observation, err)
			}
		})
	}
}

func TestServeSessionReadinessRejectsActivationContradictionsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, op, _ := pairedServeSessionObserver(t, backend)
			readiness := &serveConnectedChannelReadiness{sessions: f.adapter}
			activation := channelonboarding.ConnectedChannelActivation{OperationID: op.OperationID, PrincipalID: op.PrincipalID,
				Posture: op.Posture, Provider: op.Provider, Interface: op.Interface, Revision: op.ActivationRevision,
				BindingRevision: op.BindingRevision, SessionAccount: op.SessionAccount, Coordinate: op.Coordinate}
			for _, mutate := range []func(*channelonboarding.ConnectedChannelActivation){
				func(a *channelonboarding.ConnectedChannelActivation) { a.OperationID = uuid.NewString() },
				func(a *channelonboarding.ConnectedChannelActivation) { a.PrincipalID = uuid.NewString() },
				func(a *channelonboarding.ConnectedChannelActivation) {
					a.Posture = channelonboarding.ActivationWebhookRegistration
				},
				func(a *channelonboarding.ConnectedChannelActivation) { a.Provider = "telegram" },
				func(a *channelonboarding.ConnectedChannelActivation) { a.Interface.Selector = "foreign" },
				func(a *channelonboarding.ConnectedChannelActivation) { a.Revision++ },
				func(a *channelonboarding.ConnectedChannelActivation) { a.BindingRevision++ },
				func(a *channelonboarding.ConnectedChannelActivation) { a.SessionAccount.AdmissionID = uuid.NewString() },
				func(a *channelonboarding.ConnectedChannelActivation) { a.Coordinate.ContextPublicationGeneration++ },
			} {
				changed, facts := activation, channelonboarding.ReadinessFacts{}
				mutate(&changed)
				current, err := readiness.observeSessionReadiness(f.ctx, op, changed, &facts)
				facts.SessionAuthority.CloseExecution()
				if !errors.Is(err, channelonboarding.ErrRevisionConflict) || current || facts.SessionObservation != nil {
					t.Fatal("contradictory activation adopted session health", current, facts, err)
				}
			}
			facts := channelonboarding.ReadinessFacts{}
			if current, err := (&serveConnectedChannelReadiness{}).observeSessionReadiness(f.ctx, op, activation, &facts); err != nil || current || facts.SessionObservation != nil {
				t.Fatal("uninstalled session acquired readiness", current, facts, err)
			}
		})
	}
}

func TestServeSessionReadinessRejectsChangedParentsAndObservationErrorsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, op, observer := pairedServeSessionObserver(t, backend)
			for _, mutate := range []func(*channelonboarding.Operation){
				func(op *channelonboarding.Operation) { op.Revision++ },
				func(op *channelonboarding.Operation) { op.PrincipalID = uuid.NewString() },
				func(op *channelonboarding.Operation) { op.SessionAccount.AdmissionID = uuid.NewString() },
				func(op *channelonboarding.Operation) { op.SessionAccount.AccountRef = "foreign@s.whatsapp.net" },
				func(op *channelonboarding.Operation) { op.Coordinate.ContextPublicationGeneration++ },
				func(op *channelonboarding.Operation) { op.Coordinate.TargetGeneration++ },
				func(op *channelonboarding.Operation) { op.ActivationRevision++ },
				func(op *channelonboarding.Operation) { op.BindingRevision++ },
				func(op *channelonboarding.Operation) { op.TargetSelector = "ingress:foreign:whatsapp" },
			} {
				changed := op
				mutate(&changed)
				provider, observation, current, err := observer.ObserveSession(f.ctx, changed)
				provider.CloseExecution()
				if err == nil || current || observation.Connected {
					t.Fatal("contradictory requested parent granted health", current, observation, err)
				}
			}
			cause := errors.New("readiness selected observation failed")
			fault := &serveSessionReadFaultStore{Store: f.adapter.store, cause: cause}
			fault.failAt.Store(1)
			f.adapter.store = fault
			provider, observation, current, err := observer.ObserveSession(f.ctx, op)
			provider.CloseExecution()
			if !errors.Is(err, cause) || current || observation.Connected || f.selections.Load() != 2 {
				t.Fatal("observation failure became absence, cached success or reconnect", current, observation, err)
			}
			caller, cancel := context.WithCancel(f.ctx)
			cancel()
			provider, observation, current, err = observer.ObserveSession(caller, op)
			provider.CloseExecution()
			if !errors.Is(err, context.Canceled) || current || observation.Connected {
				t.Fatal("canceled request disclosed readiness authority", current, observation, err)
			}
			lookup, stop := context.WithCancel(f.ctx)
			defer stop()
			fault.failAt.Store(0)
			fault.onRead = stop
			provider, observation, current, err = observer.ObserveSession(lookup, op)
			provider.CloseExecution()
			if !errors.Is(err, context.Canceled) || current || observation.Connected {
				t.Fatal("lookup cancellation disclosed readiness authority", current, observation, err)
			}
		})
	}
}
