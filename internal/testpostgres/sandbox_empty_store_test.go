package testpostgres

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSandboxEmptyStoreAdministrationPreservesCancellationAndLeaseIdentity(t *testing.T) {
	manager := integrationManager(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	sandbox, err := manager.Acquire(ctx, true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := sandbox.Release(context.Background()); err != nil {
			t.Error(err)
		}
	})
	before, err := sandbox.PublicTableCount(ctx)
	if err != nil || before == 0 {
		t.Fatalf("native template has no public schema: count=%d err=%v", before, err)
	}
	cancelled, stop := context.WithCancel(ctx)
	stop()
	if err := sandbox.ResetPublicSchema(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled reset succeeded: %v", err)
	}
	if after, err := sandbox.PublicTableCount(ctx); err != nil || after != before {
		t.Fatalf("cancelled reset changed schema: before=%d after=%d err=%v", before, after, err)
	}
	if _, err := sandbox.PublicTableCount(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled inspection supplied empty-store evidence: %v", err)
	}
	foreign := &Sandbox{Name: sandbox.Name + "_foreign", Connection: sandbox.Connection, DB: sandbox.DB, manager: manager, leaseConn: sandbox.leaseConn}
	assertEmptyStoreAdministrationRefused(t, foreign)
	assertEmptyStoreAdministrationRefused(t, nil)
	assertEmptyStoreAdministrationRefused(t, &Sandbox{})
	if err := sandbox.ResetPublicSchema(ctx); err != nil {
		t.Fatal(err)
	}
	if count, err := sandbox.PublicTableCount(ctx); err != nil || count != 0 {
		t.Fatalf("reset bootstrapped the store: count=%d err=%v", count, err)
	}
	if err := sandbox.Release(ctx); err != nil {
		t.Fatal(err)
	}
	assertEmptyStoreAdministrationRefused(t, sandbox)
}

func assertEmptyStoreAdministrationRefused(t *testing.T, sandbox *Sandbox) {
	t.Helper()
	if _, err := sandbox.PublicTableCount(context.Background()); err == nil {
		t.Fatal("foreign or released sandbox supplied empty-schema evidence")
	}
	if err := sandbox.ResetPublicSchema(context.Background()); err == nil {
		t.Fatal("foreign or released sandbox reset a schema")
	}
}
