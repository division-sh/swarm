//go:build linux || darwin

package serveapp

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/google/uuid"
)

func TestServeSessionLogoutDispatcherRetainsOriginalUntilSettlementBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, op, _ := pairedServeSessionObserver(t, backend)
			d := &serveSessionLogoutDispatcher{sessions: f.adapter, effects: f.adapter.store.(effects.Store), posture: executionposture.Live}
			target, err := d.PrepareSessionLogout(f.ctx, op)
			if err != nil || !target.MatchesOperation(op) {
				t.Fatal("served logout did not prepare the original SDK", err)
			}
			original := f.adapter.connection(op.OperationID)
			reserved, err := f.adapter.store.ReserveChannelTeardown(f.ctx, channelonboarding.ReserveTeardownRequest{
				TeardownID: uuid.NewString(), RequestKeyHash: uuid.NewString(), RequestHash: uuid.NewString(),
				Kind: channelonboarding.TeardownLogout, PrincipalID: op.PrincipalID,
				Scope: channelonboarding.TeardownScope{Interface: op.Interface}, Logout: &target, RequestedAt: time.Now().UTC(),
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := f.adapter.RetireInactiveSessions(f.ctx); err != nil || f.owner.ActiveCount() == 0 {
				t.Fatal("ordinary cleanup consumed the pending logout transport", err)
			}
			if _, err := original.ChannelExecution(f.ctx); err == nil {
				t.Fatal("pending logout retained business execution authority")
			}
			if err := d.DispatchSessionLogout(f.ctx, reserved); err != nil {
				t.Fatal("served logout failed to use its retained original", err)
			}
			settled, err := f.adapter.store.GetChannelTeardown(f.ctx, reserved.TeardownID)
			if err != nil || settled.Phase != channelonboarding.TeardownSucceeded || settled.Revision != reserved.Revision+1 {
				t.Fatal("served unlink did not settle its responsibility", settled, err)
			}
			if err := f.adapter.RetireInactiveSessions(f.ctx); err != nil || f.owner.ActiveCount() != 0 {
				t.Fatal("complete logout retained cleanup work", err)
			}
			if f.adapter.connection(op.OperationID) != original || f.selections.Load() != 2 {
				t.Fatal("served logout opened a replacement connection")
			}
		})
	}
}

func TestServeSessionDestructiveLogoutUsesJournalAndExactReplayBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, op, _ := pairedServeSessionObserver(t, backend)
			proofs, err := operatorchannel.NewFileProofStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			identities, err := operatorchannel.NewService(f.adapter.store.(operatorchannel.Store), proofs, f.adapter, []operatorchannel.InterfaceIdentity{op.Interface}, uuid.NewString())
			if err != nil {
				t.Fatal(err)
			}
			if _, err := identities.PreparePrincipal(f.ctx, time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
			files, err := credentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
			if err != nil {
				t.Fatal(err)
			}
			writer, err := channelonboarding.NewCredentialWriter(files)
			if err != nil {
				t.Fatal(err)
			}
			d := &serveSessionLogoutDispatcher{sessions: f.adapter, effects: f.adapter.store.(effects.Store), posture: executionposture.Live}
			service, err := channelonboarding.NewDestructiveService(f.adapter.store, identities, writer,
				&serveChannelActivationRefresher{store: f.adapter.store, sessions: f.adapter}, nil, nil, d)
			if err != nil {
				t.Fatal(err)
			}
			key, request := uuid.NewString(), uuid.NewString()
			if _, err := service.Logout(f.ctx, op.OperationID, op.Revision+1, key, request); !errors.Is(err, channelonboarding.ErrRevisionConflict) {
				t.Fatal("stale caller revision selected logout", err)
			}
			result, err := service.Logout(f.ctx, op.OperationID, op.Revision, key, request)
			if err != nil || result.Validate() != nil || result.Teardown.Phase != channelonboarding.TeardownSucceeded {
				t.Fatal("destructive service bypassed typed SDK settlement", result, err)
			}
			replay, err := service.Logout(f.ctx, op.OperationID, op.Revision, key, request)
			if err != nil || replay.Teardown.TeardownID != result.Teardown.TeardownID || replay.Teardown.Revision != result.Teardown.Revision || replay.EffectOperationID != result.EffectOperationID {
				t.Fatal("exact terminal replay changed or redispatched logout", err)
			}
			if _, err := service.Logout(f.ctx, op.OperationID, op.Revision, uuid.NewString(), request); !errors.Is(err, channelonboarding.ErrConflict) {
				t.Fatal("new caller key adopted terminal destruction", err)
			}
			canceled, cancel := context.WithCancel(f.ctx)
			cancel()
			if _, err := service.Logout(canceled, op.OperationID, op.Revision, key, request); !errors.Is(err, context.Canceled) {
				t.Fatal("canceled replay reported success", err)
			}
		})
	}
}
