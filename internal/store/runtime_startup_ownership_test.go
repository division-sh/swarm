package store

import (
	"context"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/testutil"
)

func TestPostgresStore_AcquireRuntimeStartupOwnership_DeniesCompetingOwner(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}

	lease1, err := pg.AcquireRuntimeStartupOwnership(context.Background(), "runtime-1")
	if err != nil {
		t.Fatalf("AcquireRuntimeStartupOwnership(runtime-1): %v", err)
	}
	t.Cleanup(func() { _ = lease1.Release(context.Background()) })

	lease2, err := pg.AcquireRuntimeStartupOwnership(context.Background(), "runtime-2")
	if lease2 != nil {
		t.Fatalf("AcquireRuntimeStartupOwnership(runtime-2) lease = %#v, want nil", lease2)
	}
	if err == nil || !strings.Contains(err.Error(), "shared runtime store already owned by another runtime instance") {
		t.Fatalf("AcquireRuntimeStartupOwnership(runtime-2) error = %v, want explicit ownership denial", err)
	}
}

func TestPostgresStore_AcquireRuntimeStartupOwnership_ReleaseAllowsSuccessor(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}

	lease1, err := pg.AcquireRuntimeStartupOwnership(context.Background(), "runtime-1")
	if err != nil {
		t.Fatalf("AcquireRuntimeStartupOwnership(runtime-1): %v", err)
	}
	if err := lease1.Release(context.Background()); err != nil {
		t.Fatalf("Release(runtime-1): %v", err)
	}

	lease2, err := pg.AcquireRuntimeStartupOwnership(context.Background(), "runtime-2")
	if err != nil {
		t.Fatalf("AcquireRuntimeStartupOwnership(runtime-2): %v", err)
	}
	if err := lease2.Release(context.Background()); err != nil {
		t.Fatalf("Release(runtime-2): %v", err)
	}
}
