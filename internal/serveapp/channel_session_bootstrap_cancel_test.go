//go:build linux || darwin

package serveapp

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/sessionprovider"
)

func TestServeBootstrapOriginalCallerCancellationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newServeBootstrapTestFixture(t, backend)
			entered := serveBootstrapWireFixture(t, "blocked_handshake", f.owner)
			caller, cancel := context.WithCancel(f.ctx)
			defer cancel()
			finished := make(chan error, 1)
			go func() { finished <- f.adapter.BootstrapSession(caller, f.op, f.candidate) }()
			select {
			case <-entered:
			case err := <-finished:
				t.Fatal("did not reach handshake", err)
			}
			cancel()
			prompt := true
			select {
			case err := <-finished:
				if !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				prompt = false
				t.Error("original bootstrap caller remains blocked after cancellation; cleanup owns its wait")
				select {
				case err := <-finished:
					if !errors.Is(err, context.Canceled) {
						t.Fatal(err)
					}
				case <-time.After(30 * time.Second):
					t.Fatal("cleanup did not join within original fixture safety bound")
				}
			}
			connection := f.adapter.connection(f.op.OperationID)
			if connection == nil {
				t.Fatal("canceled original caller lost cleanup ownership")
			}
			if prompt {
				if f.owner.ActiveCount() == 0 {
					t.Fatal("pending SDK join released counted runtime ownership")
				}
				other, err := sessionprovider.OpenRuntimeBootstrap(f.owned, sessionprovider.RuntimeConnectionOptions{
					Directory: f.adapter.directory, OperationID: f.op.OperationID, Store: f.adapter.store, Plan: f.candidate.Plan})
				if other != nil {
					if closeErr := other.Close(context.Background()); closeErr != nil {
						t.Error(closeErr)
					}
				}
				if err == nil {
					t.Fatal("successor acquired provider state while original SDK cleanup remained pending")
				}
			}
			join, stop := context.WithTimeout(context.Background(), 30*time.Second)
			defer stop()
			if err := connection.Close(join); err != nil {
				t.Fatal("original cleanup failed to join", err)
			}
			if f.owner.ActiveCount() != 0 {
				t.Fatal("completed SDK join retained counted work")
			}
			if err := f.adapter.BootstrapSession(f.ctx, f.op, f.candidate); err == nil || f.selections.Load() != 1 {
				t.Fatal("canceled original attempt became success or automatic reconnect", err)
			}
		})
	}
}
