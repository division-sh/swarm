//go:build linux || darwin

package serveapp

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/sessionprovider"
	"github.com/division-sh/swarm/internal/store/sessionstate"
	"github.com/google/uuid"
)

func TestServeSessionTeardownJoinsOriginalOwnerAndPreservesPairingBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, kind := range []channelonboarding.TeardownKind{channelonboarding.TeardownInterfaceRetirement, channelonboarding.TeardownContextRetirement} {
			t.Run(backend+"/"+string(kind), func(t *testing.T) {
				f, op, _ := pairedServeSessionObserver(t, backend)
				original := f.adapter.connection(op.OperationID)
				if f.owner.ActiveCount() == 0 {
					t.Fatal("paired SDK fixture has no counted connection ownership")
				}
				path := filepath.Join(f.adapter.directory, op.SessionConnectionID)
				header, err := os.ReadFile(filepath.Join(path, "session.json"))
				if err != nil {
					t.Fatal(err)
				}
				if err := f.adapter.RetireInactiveSessions(f.ctx); err != nil || original.CheckBootstrap(f.ctx) != nil {
					t.Fatal("ordinary reconciliation retired current responsibility", err)
				}
				store := f.adapter.store.(channelonboarding.TeardownStore)
				scope := channelonboarding.TeardownScope{Interface: op.Interface}
				if kind == channelonboarding.TeardownContextRetirement {
					scope = channelonboarding.TeardownScope{BundleHash: op.Coordinate.BundleHash,
						ContextPublicationGeneration: op.Coordinate.ContextPublicationGeneration}
				}
				now := time.Now().UTC()
				teardown, err := store.ReserveChannelTeardown(f.ctx, channelonboarding.ReserveTeardownRequest{
					TeardownID: uuid.NewString(), RequestKeyHash: uuid.NewString(), RequestHash: uuid.NewString(),
					Kind: kind, PrincipalID: op.PrincipalID, Scope: scope, RequestedAt: now})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := store.RetireChannelTeardownAuthority(f.ctx, channelonboarding.RetireTeardownAuthorityRequest{
					TeardownID: teardown.TeardownID, ExpectedRevision: teardown.Revision, Reason: string(kind), Now: now}); err != nil {
					t.Fatal(err)
				}
				retired, err := f.adapter.store.GetChannelOnboarding(f.ctx, op.OperationID)
				if err != nil || retired.Phase != channelonboarding.PhaseRetired {
					t.Fatal("canonical retirement did not retire the original parent", retired, err)
				}
				if err := f.adapter.RetireInactiveSessions(f.ctx); err != nil {
					t.Fatal(err)
				}
				if f.owner.ActiveCount() != 0 || original.Connect(f.ctx) == nil ||
					f.adapter.connection(op.OperationID) != original || f.selections.Load() != 2 {
					t.Fatal("retired parent retained live SDK ownership or was replaced")
				}
				if err := f.adapter.RetireInactiveSessions(f.ctx); err != nil {
					t.Fatal("complete cleanup could not be repeated", err)
				}
				after, err := os.ReadFile(filepath.Join(path, "session.json"))
				if err != nil || !bytes.Equal(header, after) {
					t.Fatal("ordinary retirement changed reserved pairing identity", err)
				}
				fixture, owner, err := sessionstate.OpenSDKFixture(f.ctx, filepath.Join(path, "provider.db"))
				if err != nil {
					t.Fatal("complete join did not release private state", err)
				}
				defer func() {
					if err := fixture.Close(); err != nil {
						t.Error(err)
					}
				}()
				device, err := owner.CurrentDevice(f.ctx, op.SessionAccount.AccountRef)
				if err != nil || device.ID == nil || device.ID.ToNonAD().String() != op.SessionAccount.AccountRef {
					t.Fatal("retirement deleted or adopted paired account state", err)
				}
			})
		}
	}
}

func TestServeSessionTeardownPreservesObservationFailureAndCancellationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, op, _ := pairedServeSessionObserver(t, backend)
			original := f.adapter.connection(op.OperationID)
			cause := errors.New("teardown selected observation failed")
			fault := &serveSessionReadFaultStore{Store: f.adapter.store, cause: cause}
			fault.failAt.Store(1)
			f.adapter.store = fault
			refresher := &serveChannelActivationRefresher{sessions: f.adapter}
			if err := refresher.RefreshChannelActivations(f.ctx); !errors.Is(err, cause) || original.CheckBootstrap(f.ctx) != nil {
				t.Fatal("failed observation became successful destructive cleanup", err)
			}
			fault.failAt.Store(0)
			caller, cancel := context.WithCancel(f.ctx)
			cancel()
			if err := f.adapter.RetireInactiveSessions(caller); !errors.Is(err, context.Canceled) || original.CheckBootstrap(f.ctx) != nil {
				t.Fatal("canceled retirement discarded unresolved ownership", err)
			}
			caller, cancel = context.WithCancel(f.ctx)
			defer cancel()
			fault.onRead = cancel
			if err := f.adapter.RetireInactiveSessions(caller); !errors.Is(err, context.Canceled) || original.CheckBootstrap(f.ctx) != nil {
				t.Fatal("lookup cancellation became cleanup success", err)
			}
		})
	}
}

func TestServeSessionTeardownInterruptedJoinRetainsOriginalOwnerBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, op, observer := pairedServeSessionObserver(t, backend)
			original := f.adapter.connection(op.OperationID)
			provider, _, current, err := observer.ObserveSession(f.ctx, op)
			defer provider.CloseExecution()
			if err != nil || !current || provider.RequireExecutable() != nil {
				t.Fatal("fixture lacks genuine held SDK admission", err)
			}
			if _, err := f.adapter.store.AdvanceChannelOnboarding(f.ctx, channelonboarding.AdvanceRequest{
				OperationID: op.OperationID, ExpectedRevision: op.Revision, Phase: channelonboarding.PhaseFailed,
				FailureCode: "test_retirement", FailureMessage: "bounded join proof", Now: time.Now().UTC()}); err != nil {
				t.Fatal(err)
			}
			wait, cancel := context.WithTimeout(f.ctx, 50*time.Millisecond)
			defer cancel()
			if err := f.adapter.RetireInactiveSessions(wait); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal("unresolved SDK admission became successful cleanup", err)
			}
			if f.owner.ActiveCount() == 0 || f.adapter.connection(op.OperationID) != original || f.selections.Load() != 2 {
				t.Fatal("interrupted cleanup discarded or substituted original ownership")
			}
			provider.CloseExecution()
			complete, stop := context.WithTimeout(f.ctx, 5*time.Second)
			defer stop()
			for range 2 {
				if err := f.adapter.RetireInactiveSessions(complete); err != nil || f.owner.ActiveCount() != 0 {
					t.Fatal("retry did not join exactly the original failed parent", err)
				}
			}
		})
	}
}

func TestServeSessionTeardownWaitsForOriginalConstructionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newServeBootstrapTestFixture(t, backend)
			entered, release := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			defer unblock()
			cause := errors.New("construction ended without a connection")
			f.adapter.selectRuntime = func(context.Context, channelonboarding.Candidate) (context.Context, *sessionprovider.RuntimeIncomingOptions, func() error, error) {
				f.selections.Add(1)
				close(entered)
				<-release
				return nil, nil, nil, cause
			}
			result := make(chan error, 1)
			go func() { result <- f.adapter.BootstrapSession(f.ctx, f.op, f.candidate) }()
			<-entered
			if _, err := f.adapter.store.AdvanceChannelOnboarding(f.ctx, channelonboarding.AdvanceRequest{
				OperationID: f.op.OperationID, ExpectedRevision: f.op.Revision, Phase: channelonboarding.PhaseFailed,
				FailureCode: "test_construction_retirement", FailureMessage: "bounded construction proof", Now: time.Now().UTC()}); err != nil {
				unblock()
				<-result
				t.Fatal(err)
			}
			wait, cancel := context.WithTimeout(f.ctx, 50*time.Millisecond)
			defer cancel()
			if err := f.adapter.RetireInactiveSessions(wait); !errors.Is(err, context.DeadlineExceeded) {
				unblock()
				<-result
				t.Fatal("unfinished construction was mistaken for joined cleanup", err)
			}
			unblock()
			if err := <-result; !errors.Is(err, cause) {
				t.Fatal("construction result was discarded", err)
			}
			if err := f.adapter.RetireInactiveSessions(f.ctx); err != nil || f.selections.Load() != 1 {
				t.Fatal("cleanup retried provider construction", err)
			}
		})
	}
}

func TestServeSessionTeardownKeepsCurrentSuccessorBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, op, _ := pairedServeSessionObserver(t, backend)
			original := f.adapter.connection(op.OperationID)
			if _, err := f.adapter.store.AdvanceChannelOnboarding(f.ctx, channelonboarding.AdvanceRequest{
				OperationID: op.OperationID, ExpectedRevision: op.Revision, Phase: channelonboarding.PhaseFailed,
				FailureCode: "test_supersession", FailureMessage: "original responsibility failed", Now: time.Now().UTC()}); err != nil {
				t.Fatal(err)
			}
			id := uuid.NewString()
			successor, err := f.adapter.store.ReserveChannelOnboarding(f.ctx, channelonboarding.StartRequest{
				OperationID: id, RequestKeyHash: id, RequestHash: id, PrincipalID: op.PrincipalID, Verb: channelonboarding.VerbConnect,
				Provider: f.candidate.Provider, Interface: f.candidate.Interface, Coordinate: f.candidate.Coordinate,
				TargetSelector: f.candidate.Target.Selector, Posture: f.candidate.Posture, Ceremony: f.candidate.Ceremony, RequestedAt: time.Now().UTC()})
			if err != nil {
				t.Fatal(err)
			}
			for _, phase := range []channelonboarding.Phase{channelonboarding.PhaseCredentialsAdmitted, channelonboarding.PhaseActivatingProvider} {
				successor, err = f.adapter.store.AdvanceChannelOnboarding(f.ctx, channelonboarding.AdvanceRequest{
					OperationID: id, ExpectedRevision: successor.Revision, Phase: phase, Now: time.Now().UTC()})
				if err != nil {
					t.Fatal(err)
				}
			}
			if err := f.adapter.BootstrapSession(f.ctx, successor, f.candidate); err != nil {
				t.Fatal(err)
			}
			current := f.adapter.connection(id)
			before := f.owner.ActiveCount()
			if err := f.adapter.RetireInactiveSessions(f.ctx); err != nil || current.CheckBootstrap(f.ctx) != nil ||
				f.owner.ActiveCount() == 0 || f.owner.ActiveCount() >= before || original.Connect(f.ctx) == nil {
				t.Fatal("old responsibility cleanup fenced or substituted its current sibling", err)
			}
			if f.adapter.connection(id) != current || f.adapter.connection(op.OperationID) != original || f.selections.Load() != 3 {
				t.Fatal("cleanup replaced a retained SDK owner")
			}
		})
	}
}
