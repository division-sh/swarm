//go:build linux || darwin

package sessionprovider

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/core/processbinding"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/google/uuid"
)

func acquireLogoutProcessFixture(t *testing.T, f *activeInputFixture, runtimeID string, exact ...startupownership.AcquireRequest) startupownership.ProcessCapability {
	t.Helper()
	request := startupownership.AcquireRequest{OwnerID: "logout-process-fixture", BootID: uuid.NewString(), RuntimeInstanceID: runtimeID}
	if len(exact) == 1 {
		request = exact[0]
	}
	owner, err := f.selected.(startupownership.Store).AcquireProcessCapability(f.ctx, request)
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

func TestSessionLogoutSuccessorFencesOriginalAndPreservesJournalBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, authorization := range []string{"absent", "committed"} {
			t.Run(backend+"/"+authorization, func(t *testing.T) {
				f := newActiveInputFixture(t, backend)
				original := acquireLogoutProcessFixture(t, f, f.operation.Coordinate.RuntimeInstanceID)
				attribution, err := original.Evidence()
				if err != nil {
					t.Fatal(err)
				}
				c := openRuntimeLogoutConnectionFixture(t, f, original)
				connectRuntimeConnectionFixture(t, f, c)
				noise := *c.state.currentOccurrence().client.Store.NoiseKey.Priv
				op := reserveRuntimeLogoutFixture(t, f, c)
				if op.Logout.ProcessStamp != attribution.LeaseStamp() || op.Logout.ProcessBinding != c.processBinding {
					t.Fatal("reservation did not freeze original native authority", op.Logout)
				}
				ctx := logoutJournalFixture(t, f, op)
				raw, err := json.Marshal(op.Logout)
				if err != nil {
					t.Fatal(err)
				}
				var handle *effects.Handle
				if authorization == "committed" {
					handle, err = effects.BeginChannelLogout(ctx, raw)
					if err != nil {
						t.Fatal("original authorization refused", err)
					}
				}
				if err := original.Release(f.ctx); err != nil {
					t.Fatal(err)
				}
				request := startupownership.AcquireRequest{OwnerID: attribution.OwnerID, BootID: attribution.BootID, RuntimeInstanceID: attribution.RuntimeInstanceID}
				canceled, cancel := context.WithCancel(f.ctx)
				cancel()
				if failed, err := f.selected.(startupownership.Store).AcquireProcessCapability(canceled, request); err == nil || failed != nil {
					t.Fatal("canceled acquisition supplied successor authority", err)
				}
				pending, err := f.selected.RecoverUnlaunchedChannelLogout(f.ctx, op.TeardownID, op.Revision, time.Now().UTC())
				if err != nil || pending.Phase != op.Phase || pending.Revision != op.Revision {
					t.Fatal("release/canceled acquisition fabricated no-launch settlement", pending, err)
				}
				for range 2 {
					successor := acquireLogoutProcessFixture(t, f, attribution.RuntimeInstanceID, request)
					evidence, err := successor.Evidence()
					if err != nil || evidence.AuthorityID == attribution.AuthorityID || evidence.AuthorityGeneration <= attribution.AuthorityGeneration ||
						evidence.RuntimeInstanceID != attribution.RuntimeInstanceID || evidence.OwnerID != attribution.OwnerID || evidence.BootID != attribution.BootID {
						t.Fatal("successor fixture did not genuinely reacquire identical caller coordinates", evidence, err)
					}
					if err := successor.Release(f.ctx); err != nil {
						t.Fatal(err)
					}
				}
				if authorization == "committed" {
					if err := handle.MarkLaunched(ctx); err == nil {
						t.Fatal("late original launch accepted a successor's authority")
					}
					pending, err := f.selected.RecoverUnlaunchedChannelLogout(f.ctx, op.TeardownID, op.Revision, time.Now().UTC())
					if err != nil || pending.Phase != op.Phase || pending.Revision != op.Revision {
						t.Fatal("committed original journal misclassified as absent", pending, err)
					}
					if _, err := f.selected.(effects.RecoveryStore).ReconcileExternalEffectAttempts(ctx, effects.NewRecoveryRequest(time.Now().UTC(), executionposture.Live)); err != nil {
						t.Fatal("historical journal settlement required the dead original", err)
					}
				} else {
					if _, err := effects.BeginChannelLogout(ctx, raw); err == nil {
						t.Fatal("late original authorization adopted successor authority")
					}
					if _, err := f.selected.RecoverUnlaunchedChannelLogout(f.ctx, op.TeardownID, op.Revision, time.Now().UTC()); err != nil {
						t.Fatal(err)
					}
				}
				settled, err := f.selected.GetChannelTeardown(f.ctx, op.TeardownID)
				if err != nil || settled.Phase != channelonboarding.TeardownFailed || settled.FailureCode != "logout_not_launched" ||
					settled.Revision != op.Revision+1 || settled.Logout == nil || *settled.Logout != *op.Logout {
					t.Fatal("successor recovery lost exact failed historical responsibility", settled, err)
				}
				preparation := op.Logout.SessionLogoutPreparation
				replay, err := f.selected.ReserveChannelTeardown(f.ctx, channelonboarding.ReserveTeardownRequest{
					TeardownID: uuid.NewString(), RequestKeyHash: op.RequestKeyHash, RequestHash: op.RequestHash,
					Kind: op.Kind, PrincipalID: op.PrincipalID, Scope: op.Scope, Logout: &preparation, RequestedAt: time.Now().UTC()})
				if err != nil || replay.TeardownID != settled.TeardownID || replay.Revision != settled.Revision || replay.Logout == nil || *replay.Logout != *op.Logout {
					t.Fatal("exact replay refreshed original process stamp", replay, err)
				}
				if err := c.DispatchSessionLogout(ctx, op); err == nil {
					t.Fatal("retired original redispatched after recovery")
				}
				identity, _ := effects.ChannelLogoutOperationID(op.TeardownID)
				outcome, found, err := f.selected.(effects.OutcomeStore).GetExternalEffectOutcome(ctx, identity)
				if err != nil || found != (authorization == "committed") || found && outcome.AttemptState != effects.StateTerminalFailure {
					t.Fatal("recovery manufactured or replayed journal evidence", outcome, found, err)
				}
				if c.state.currentOccurrence().client.Store.Deleted || *c.state.currentOccurrence().client.Store.NoiseKey.Priv != noise {
					t.Fatal("recovery changed original pairing")
				}
				select {
				case frame := <-f.peer.frames:
					t.Fatal("historical recovery emitted SDK unlink", frame.node)
				default:
				}
				requireLogoutContextRetirement(t, f, op)
			})
		}
	}
}

func openRuntimeLogoutConnectionFixture(t *testing.T, f *activeInputFixture, process ...startupownership.ProcessCapability) *RuntimeConnection {
	t.Helper()
	var owner startupownership.ProcessCapability
	if len(process) == 1 {
		owner = process[0]
	} else {
		owner = acquireLogoutProcessFixture(t, f, f.operation.Coordinate.RuntimeInstanceID)
	}
	return openRuntimeConnectionFixture(t, f, admitLogoutGenerationFixture(t, f, owner))
}

func admitLogoutGenerationFixture(t *testing.T, f *activeInputFixture, owner startupownership.ProcessCapability) processbinding.Binding {
	t.Helper()
	plan, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{{BundleHash: f.operation.Coordinate.BundleHash}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	stored, found, err := owner.CurrentSourceSet(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if found {
		if stored.Revision != plan.Revision {
			t.Fatal("successor fixture cannot adopt a different source set")
		}
		if _, err := owner.RestoreSourceSet(f.ctx, agenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), Plan: plan, ExpectedRevision: stored.Revision}); err != nil {
			t.Fatal(err)
		}
	} else {
		if _, err := owner.InstallCompleteSourceSet(f.ctx, agenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), Plan: plan}); err != nil {
			t.Fatal(err)
		}
	}
	grant, err := owner.IssueGenerationGrant(f.ctx, startupownership.GrantRequest{BundleHash: f.operation.Coordinate.BundleHash,
		RuntimeInstanceID: f.operation.Coordinate.RuntimeInstanceID, RuntimeGeneration: 1, SourceSetRevision: plan.Revision})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := grant.MarkProbesSettled(f.ctx, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := grant.AdmitExecution(f.ctx); err != nil {
		t.Fatal(err)
	}
	binding, err := grant.ProcessExecutionBinding()
	if err != nil {
		t.Fatal(err)
	}
	return binding
}

func TestSessionLogoutPreparationCannotAdoptSuccessorGrantBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newActiveInputFixture(t, backend)
			original := acquireLogoutProcessFixture(t, f, f.operation.Coordinate.RuntimeInstanceID)
			attribution, err := original.Evidence()
			if err != nil {
				t.Fatal(err)
			}
			c := openRuntimeLogoutConnectionFixture(t, f, original)
			connectRuntimeConnectionFixture(t, f, c)
			prepared, err := c.PrepareSessionLogout(f.ctx, f.operation)
			if err != nil {
				t.Fatal(err)
			}
			if err := original.Release(f.ctx); err != nil {
				t.Fatal(err)
			}
			successor := acquireLogoutProcessFixture(t, f, attribution.RuntimeInstanceID, startupownership.AcquireRequest{
				OwnerID: attribution.OwnerID, BootID: attribution.BootID, RuntimeInstanceID: attribution.RuntimeInstanceID})
			current := admitLogoutGenerationFixture(t, f, successor)
			if current.ProcessAuthorityID == prepared.ProcessBinding.ProcessAuthorityID {
				t.Fatal("fixture did not acquire a distinct native lease")
			}
			request := channelonboarding.ReserveTeardownRequest{TeardownID: uuid.NewString(), RequestKeyHash: uuid.NewString(), RequestHash: uuid.NewString(),
				Kind: channelonboarding.TeardownLogout, PrincipalID: f.operation.PrincipalID, Scope: channelonboarding.TeardownScope{Interface: f.operation.Interface},
				Logout: &prepared, RequestedAt: time.Now().UTC()}
			if _, err := f.selected.ReserveChannelTeardown(f.ctx, request); err == nil {
				t.Fatal("delayed preparation adopted current singleton/successor grant")
			}
			freshObservation, err := c.PrepareSessionLogout(f.ctx, f.operation)
			if err != nil || freshObservation.ProcessBinding != prepared.ProcessBinding {
				t.Fatal("old SDK occurrence refreshed its immutable process attribution", err)
			}
			request.Logout = &freshObservation
			if _, err := f.selected.ReserveChannelTeardown(f.ctx, request); err == nil {
				t.Fatal("old SDK observation gained successor authority")
			}
			retained, err := f.selected.GetChannelOnboarding(f.ctx, f.operation.OperationID)
			if err != nil || retained.Revision != f.operation.Revision || retained.Phase != f.operation.Phase {
				t.Fatal("failed reservation retired original admission", retained, err)
			}
			rows, err := f.selected.ListChannelTeardowns(f.ctx)
			if err != nil || len(rows) != 0 {
				t.Fatal("failed stale reservation persisted responsibility", rows, err)
			}
		})
	}
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
			process := acquireLogoutProcessFixture(t, f, f.operation.Coordinate.RuntimeInstanceID)
			c := openRuntimeLogoutConnectionFixture(t, f, process)
			connectRuntimeConnectionFixture(t, f, c)
			op := reserveRuntimeLogoutFixture(t, f, c)
			unobserved, err := f.selected.RecoverUnlaunchedChannelLogout(f.ctx, op.TeardownID, op.Revision, time.Now().UTC())
			if err != nil || unobserved.Revision != op.Revision || unobserved.Phase != op.Phase {
				t.Fatal("missing process facts fabricated original withdrawal", unobserved, err)
			}
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
			pending, err := f.selected.RecoverUnlaunchedChannelLogout(f.ctx, op.TeardownID, op.Revision, time.Now().UTC())
			if err != nil || pending.Phase != op.Phase || pending.Revision != op.Revision {
				t.Fatal("physical release fabricated successor authority", pending, err)
			}
			acquireLogoutProcessFixture(t, f, f.operation.Coordinate.RuntimeInstanceID)
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

type logoutLaunchCommitFixture struct {
	effects.Store
	committed chan effects.Attempt
	release   chan struct{}
}

func (f *logoutLaunchCommitFixture) MarkExternalAttemptLaunched(ctx context.Context, a effects.Attempt, now time.Time) (effects.ExternalAttemptLaunch, error) {
	result, err := f.Store.MarkExternalAttemptLaunched(ctx, a, now)
	if err == nil {
		f.committed <- a
		<-f.release
	}
	return result, err
}

func TestSessionLogoutSuccessorBeforeSDKUnlinkRetainsUncertainJournalBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newActiveInputFixture(t, backend)
			original := acquireLogoutProcessFixture(t, f, f.operation.Coordinate.RuntimeInstanceID)
			c := openRuntimeLogoutConnectionFixture(t, f, original)
			connectRuntimeConnectionFixture(t, f, c)
			op := reserveRuntimeLogoutFixture(t, f, c)
			ctx := logoutJournalFixture(t, f, op)
			barrier := &logoutLaunchCommitFixture{Store: f.selected.(effects.Store), committed: make(chan effects.Attempt, 1), release: make(chan struct{})}
			ctx = effects.WithController(ctx, effects.NewController(barrier).WithExecutionPosture(executionposture.Live))
			caller, cancel := context.WithCancel(ctx)
			var once sync.Once
			release := func() { once.Do(func() { close(barrier.release) }) }
			done, joined := make(chan error, 1), make(chan struct{})
			go func() { defer close(joined); done <- c.DispatchSessionLogout(caller, op) }()
			t.Cleanup(func() { cancel(); release(); <-joined })
			select {
			case <-barrier.committed:
			case <-time.After(5 * time.Second):
				t.Fatal("original journal did not durably launch")
			}
			if err := original.Release(f.ctx); err != nil {
				t.Fatal(err)
			}
			acquireLogoutProcessFixture(t, f, f.operation.Coordinate.RuntimeInstanceID)
			release()
			if err := awaitOccurrenceProbe(t, f.peer.ctx, done); err == nil {
				t.Fatal("superseded original dispatched SDK unlink")
			}
			settled, err := f.selected.GetChannelTeardown(f.ctx, op.TeardownID)
			if err != nil || settled.Phase != channelonboarding.TeardownFailed || settled.FailureCode != "logout_outcome_uncertain" ||
				settled.Revision != op.Revision+1 || settled.Logout == nil || *settled.Logout != *op.Logout {
				t.Fatal("durable launch was rewritten as never-authorized", settled, err)
			}
			if c.state.currentOccurrence().client.Store.Deleted {
				t.Fatal("superseded SDK deleted original pairing")
			}
			select {
			case frame := <-f.peer.frames:
				t.Fatal("superseded SDK emitted unlink", frame.node)
			default:
			}
			if _, err := f.selected.(effects.RecoveryStore).ReconcileExternalEffectAttempts(ctx, effects.NewRecoveryRequest(time.Now().UTC(), executionposture.Live)); err != nil {
				t.Fatal(err)
			}
			if err := c.DispatchSessionLogout(ctx, op); err == nil {
				t.Fatal("uncertain journal became redispatchable")
			}
			requireLogoutContextRetirement(t, f, op)
		})
	}
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
			c := openRuntimeLogoutConnectionFixture(t, f, process)
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
			acquireLogoutProcessFixture(t, f, f.operation.Coordinate.RuntimeInstanceID)
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
