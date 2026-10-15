//go:build linux || darwin

package serveapp

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorchannel"
)

type serveBootstrapAuthCut struct {
	held          chan struct{}
	release       chan struct{}
	authenticated chan struct{}
	heldOnce      sync.Once
	authOnce      sync.Once
}

func TestServeSessionHeldAuthenticationCheckpointBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newServeBootstrapTestFixture(t, backend)
			cut := &serveBootstrapAuthCut{held: make(chan struct{}), release: make(chan struct{}), authenticated: make(chan struct{})}
			authenticated := serveBootstrapWireFixture(t, "paired", f.owner, cut)
			accountRef := seedServePairedDevice(t, f)
			select {
			case <-authenticated:
				t.Fatal("unpaired seed socket supplied paired login evidence")
			default:
			}
			// A finite diagnostic bound, not a changed product or existing
			// fixture deadline. Auth release and observation use channels.
			ctx, cancel := context.WithTimeout(f.ctx, 20*time.Second)
			defer cancel()
			if err := awaitServeSessionBootstrapFixture(ctx, f.adapter, f.op, f.candidate, true); err != nil {
				t.Fatal("original explicit bootstrap resume failed before authentication cut")
			}
			select {
			case <-cut.held:
			case <-ctx.Done():
				t.Fatal("paired SDK socket did not reach the held authentication cut")
			}
			select {
			case <-authenticated:
				t.Fatal("Noise completion supplied held authentication evidence")
			default:
			}
			current := f.adapter.connection(f.op.OperationID)
			f.adapter.mu.Lock()
			attempt := f.adapter.connections[f.op.OperationID]
			f.adapter.mu.Unlock()
			attempt.mu.Lock()
			completed := attempt.connected && !attempt.connecting && attempt.connectErr == nil
			attempt.mu.Unlock()
			before, ok, err := f.adapter.CheckpointSessionPairing(ctx, f.op)
			t.Logf("held auth cut: same_occurrence=%t live=%t transport_connected=%t bootstrap_completed=%t checkpoint_ok=%t checkpoint_error=%t",
				current == f.adapter.connection(f.op.OperationID), f.owned.Err() == nil,
				current.CheckBootstrap(ctx) == nil, completed, ok, err != nil)
			if err != nil || ok || !completed || current.CheckBootstrap(ctx) != nil || before.OperationID != f.op.OperationID ||
				before.Revision != f.op.Revision || before.SessionAccount != (operatorchannel.SessionAccountAdmission{}) ||
				f.selections.Load() != 2 {
				t.Fatal("held authentication did not isolate completed transport without an account checkpoint")
			}
			headerPath := filepath.Join(f.adapter.directory, f.op.SessionConnectionID, "session.json")
			header, err := os.ReadFile(headerPath)
			if err != nil {
				t.Fatal("read original private header")
			}
			close(cut.release)
			awaitServePairedAuthenticationFixture(t, ctx, authenticated)
			select {
			case <-cut.authenticated:
			case <-ctx.Done():
				t.Fatal("released SDK authentication did not reach its post-login wire milestone")
			}
			after, ok, err := f.adapter.CheckpointSessionPairing(ctx, f.op)
			t.Logf("released auth cut: same_occurrence=%t live=%t transport_connected=%t post_login_iq_observed=%t checkpoint_ok=%t checkpoint_error=%t",
				current == f.adapter.connection(f.op.OperationID), f.owned.Err() == nil,
				current.CheckBootstrap(ctx) == nil, true, ok, err != nil)
			retainedHeader, headerErr := os.ReadFile(headerPath)
			if err != nil || !ok || after.OperationID != f.op.OperationID || after.SessionConnectionID != f.op.SessionConnectionID ||
				after.Coordinate != f.op.Coordinate || after.Revision != f.op.Revision+1 || after.SessionAccount.AccountRef != accountRef ||
				after.IdentityOperationID != "" || after.ActivationRevision != 0 || after.Coordinate.TargetGeneration != 0 ||
				current != f.adapter.connection(f.op.OperationID) || f.selections.Load() != 2 || headerErr != nil || !bytes.Equal(header, retainedHeader) {
				t.Fatal("released authentication changed responsibility, identity, header or checkpoint ownership")
			}
			if err := current.Close(ctx); err != nil {
				t.Fatal("original authenticated socket did not join", err)
			}
			if err := awaitServeSessionBootstrapFixture(ctx, f.adapter, after, f.candidate, true); err != nil {
				t.Fatal("distinct retained-account successor did not authenticate", err)
			}
			if f.adapter.connection(f.op.OperationID) == current || f.selections.Load() != 3 {
				t.Fatal("successor did not construct one distinct authenticated socket")
			}
			select {
			case <-authenticated:
				t.Fatal("consumed first-socket evidence remained usable for a distinct successor")
			default:
			}
		})
	}
}
