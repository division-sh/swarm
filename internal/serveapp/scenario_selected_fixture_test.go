package serveapp

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/config"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	storeselected "github.com/division-sh/swarm/internal/store/selected"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
)

// The retained DSN is a location, not a pool or a reconstructed mutation owner.
// Each serve occurrence constructs and closes its own selected runtime owner.
func installScenarioPostgresRuntime(t *testing.T) {
	t.Helper()
	dsn := testutil.StartEmptyPostgresDSN(t)
	previous := buildStoresForServe
	buildStoresForServe = func(ctx context.Context, selection storebackend.Selection, cfg *config.Config) (*selectedStoreOwner, error) {
		if selection.Backend != storebackend.BackendPostgres {
			return nil, fmt.Errorf("scenario fixture requires the selected PostgreSQL backend")
		}
		return storeselected.OpenRuntime(ctx, storeselected.RuntimeRequest{
			Selection: selection, PostgresDSN: dsn, SessionLockTTL: runtimeSessionLockTTL(cfg),
		})
	}
	t.Cleanup(func() { buildStoresForServe = previous })
}

func installPublicInputPublicationFailure(t *testing.T, selected receiverProofStateReader, rt *runtimepkg.Runtime, cut storetest.PublicationFailureCut) {
	t.Helper()
	fault, err := storetest.NewPublicationFailure(selected, cut, "telegram-chat")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		// Runtime admission and every local consumer retire before DDL removal;
		// the enclosing serve process still owns the open selected store here.
		if err := rt.Shutdown(); err != nil {
			t.Error(err)
			return
		}
		if err := fault.RestoreForTest(context.Background()); err != nil {
			t.Error(err)
		}
	})
	if err := fault.InstallForTest(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func requirePublicInputPublicationFailureMarker(t testing.TB, rpcErr *servedJSONRPCError, marker string) {
	t.Helper()
	// A prerequisite error is a different cut and cannot prove late rollback.
	if !strings.Contains(fmt.Sprint(rpcErr), marker) {
		t.Fatalf("publication did not reach its exact late fault %q: %+v", marker, rpcErr)
	}
}
