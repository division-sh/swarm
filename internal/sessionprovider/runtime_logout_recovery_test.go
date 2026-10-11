//go:build linux || darwin

package sessionprovider

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/google/uuid"
)

func acquireLogoutProcessFixture(t *testing.T, f *activeInputFixture, runtimeID string) startupownership.ProcessCapability {
	t.Helper()
	owner, err := f.selected.(startupownership.Store).AcquireProcessCapability(f.ctx, startupownership.AcquireRequest{
		OwnerID: "logout-process-fixture", BootID: uuid.NewString(), RuntimeInstanceID: runtimeID})
	if err != nil {
		t.Fatal("acquire real selected process responsibility", err)
	}
	t.Cleanup(func() {
		if err := owner.Release(context.Background()); err != nil {
			t.Error("release logout process responsibility", err)
		}
	})
	return owner
}

func requireLogoutContextRetirement(t *testing.T, f *activeInputFixture, logout channelonboarding.TeardownOperation) {
	t.Helper()
	retired, err := f.selected.ReserveChannelTeardown(f.ctx, channelonboarding.ReserveTeardownRequest{
		TeardownID: uuid.NewString(), RequestKeyHash: uuid.NewString(), RequestHash: uuid.NewString(),
		Kind: channelonboarding.TeardownContextRetirement, PrincipalID: logout.PrincipalID,
		Scope: channelonboarding.TeardownScope{BundleHash: logout.Logout.Coordinate.BundleHash,
			ContextPublicationGeneration: logout.Logout.Coordinate.ContextPublicationGeneration}, RequestedAt: time.Now().UTC()})
	if err != nil || retired.Kind != channelonboarding.TeardownContextRetirement || retired.Phase != channelonboarding.TeardownReserved {
		t.Fatal("terminal logout still blocked exact context retirement", retired, err)
	}
}

func TestSessionLogoutPreauthorizationRecoveryRequiresWithdrawnOriginalBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newActiveInputFixture(t, backend)
			c := openRuntimeConnectionFixture(t, f)
			connectRuntimeConnectionFixture(t, f, c)
			op := reserveRuntimeLogoutFixture(t, f, c)
			unobserved, err := f.selected.RecoverUnlaunchedChannelLogout(f.ctx, op.TeardownID, op.Revision, time.Now().UTC())
			if err != nil || unobserved.Revision != op.Revision || unobserved.Phase != op.Phase {
				t.Fatal("missing process facts fabricated original withdrawal", unobserved, err)
			}
			process := acquireLogoutProcessFixture(t, f, f.operation.Coordinate.RuntimeInstanceID)
			for range 2 {
				observed, err := f.selected.RecoverUnlaunchedChannelLogout(f.ctx, op.TeardownID, op.Revision, time.Now().UTC())
				if err != nil || observed.Phase != op.Phase || observed.Revision != op.Revision || *observed.Logout != *op.Logout {
					t.Fatal("live original authorizer was falsely settled from temporary absence", observed, err)
				}
			}
			canceled, stop := context.WithCancel(f.ctx)
			stop()
			if _, err := f.selected.RecoverUnlaunchedChannelLogout(canceled, op.TeardownID, op.Revision, time.Now().UTC()); !errors.Is(err, context.Canceled) {
				t.Fatal("canceled recovery fabricated completion", err)
			}
			if err := process.Release(f.ctx); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				settled, err := f.selected.RecoverUnlaunchedChannelLogout(f.ctx, op.TeardownID, op.Revision, time.Now().UTC())
				if err != nil || settled.Phase != channelonboarding.TeardownFailed || settled.FailureCode != "logout_not_launched" ||
					settled.Revision != op.Revision+1 || settled.Logout == nil || *settled.Logout != *op.Logout {
					t.Fatal("withdrawn never-authorized original did not close exactly once", settled, err)
				}
			}
			if _, err := f.selected.RecoverUnlaunchedChannelLogout(f.ctx, op.TeardownID, op.Revision+9, time.Now().UTC()); !errors.Is(err, channelonboarding.ErrRevisionConflict) {
				t.Fatal("terminal recovery admitted a contradictory revision", err)
			}
			ctx := logoutJournalFixture(t, f, op)
			if err := c.DispatchSessionLogout(ctx, op); err == nil {
				t.Fatal("late original caller authorized after no-launch settlement")
			}
			identity, _ := effects.ChannelLogoutOperationID(op.TeardownID)
			if _, found, err := f.selected.(effects.OutcomeStore).GetExternalEffectOutcome(ctx, identity); err != nil || found {
				t.Fatal("no-launch recovery or refused late caller manufactured an attempt", found, err)
			}
			select {
			case frame := <-f.peer.frames:
				t.Fatal("withdrawn original sent SDK unlink", frame.node)
			default:
			}
			requireLogoutContextRetirement(t, f, op)
		})
	}
}

type logoutAuthorizationCommitFixture struct {
	effects.Store
	committed chan effects.Attempt
	release   chan struct{}
}

func (f *logoutAuthorizationCommitFixture) AuthorizeExternalAttempt(ctx context.Context, a effects.Authority, req effects.AuthorizeRequest) (effects.Attempt, error) {
	attempt, err := f.Store.AuthorizeExternalAttempt(ctx, a, req)
	if err == nil {
		f.committed <- attempt
		<-f.release
	}
	return attempt, err
}

func TestSessionLogoutPreauthorizationRecoveryPreservesCommittedJournalBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newActiveInputFixture(t, backend)
			process := acquireLogoutProcessFixture(t, f, f.operation.Coordinate.RuntimeInstanceID)
			c := openRuntimeConnectionFixture(t, f)
			connectRuntimeConnectionFixture(t, f, c)
			op := reserveRuntimeLogoutFixture(t, f, c)
			ctx := logoutJournalFixture(t, f, op)
			barrier := &logoutAuthorizationCommitFixture{Store: f.selected.(effects.Store), committed: make(chan effects.Attempt, 1), release: make(chan struct{})}
			ctx = effects.WithController(ctx, effects.NewController(barrier).WithExecutionPosture(executionposture.Live))
			caller, cancel := context.WithCancel(ctx)
			var once sync.Once
			release := func() { once.Do(func() { close(barrier.release) }) }
			done, joined := make(chan error, 1), make(chan struct{})
			go func() { defer close(joined); done <- c.DispatchSessionLogout(caller, op) }()
			t.Cleanup(func() { cancel(); release(); <-joined })
			select {
			case attempt := <-barrier.committed:
				if attempt.Kind != effects.KindChannelLogout || !attempt.AuthorizationAcknowledged {
					t.Fatal("commit barrier missed durable authorization", attempt)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("original authorizer did not commit")
			}
			cancel()
			if err := process.Release(f.ctx); err != nil {
				t.Fatal(err)
			}
			observed, err := f.selected.RecoverUnlaunchedChannelLogout(f.ctx, op.TeardownID, op.Revision, time.Now().UTC())
			if err != nil || observed.Phase != op.Phase || observed.Revision != op.Revision || *observed.Logout != *op.Logout {
				t.Fatal("committed authorization was misclassified as never-authorized", observed, err)
			}
			release()
			if err := awaitOccurrenceProbe(t, f.peer.ctx, done); !errors.Is(err, context.Canceled) {
				t.Fatal("original interrupted authorizer lost cancellation", err)
			}
			for range 2 {
				if _, err := f.selected.(effects.RecoveryStore).ReconcileExternalEffectAttempts(ctx,
					effects.NewRecoveryRequest(time.Now().UTC(), executionposture.Live)); err != nil {
					t.Fatal("journal recovery lost committed original authorization", err)
				}
			}
			identity, _ := effects.ChannelLogoutOperationID(op.TeardownID)
			outcome, found, err := f.selected.(effects.OutcomeStore).GetExternalEffectOutcome(ctx, identity)
			if err != nil || !found || outcome.AttemptState != effects.StateTerminalFailure {
				t.Fatal("journal owner lost the original no-launch result", outcome, err)
			}
			select {
			case frame := <-f.peer.frames:
				t.Fatal("interrupted commit raced into SDK unlink", frame.node)
			default:
			}
			requireLogoutContextRetirement(t, f, op)
		})
	}
}
