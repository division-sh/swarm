package runtimepersistence

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestGateRouteAdmissionPrivateAdapterParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			var selected interface {
				RequireGateRouteAdmitted(context.Context, string) error
			}
			var fixtureStore any

			switch backend {
			case "sqlite":
				store := newBootstrappedSQLiteRuntimeStoreForTest(t)
				selected = store
				fixtureStore = store
			case "postgres":
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				store := admitTestPostgresStore(t, db)
				selected = store
				fixtureStore = store
			default:
				t.Fatalf("unknown backend %q", backend)
			}

			runningRunID := uuid.NewString()
			pausedRunID := uuid.NewString()
			now := time.Now().UTC()
			requireRunningRunForTest(t, ctx, fixtureStore, runningRunID, now)
			requirePausedRunForTest(t, ctx, fixtureStore, pausedRunID, now)

			if err := selected.RequireGateRouteAdmitted(ctx, runningRunID); err != nil {
				t.Fatalf("running route admission: %v", err)
			}
			for _, test := range []struct {
				name  string
				runID string
				want  string
			}{
				{name: "empty", want: "run id is required"},
				{name: "missing", runID: uuid.NewString(), want: "is unavailable"},
				{name: "paused", runID: pausedRunID, want: "is not routable in status paused"},
			} {
				t.Run(test.name, func(t *testing.T) {
					err := selected.RequireGateRouteAdmitted(ctx, test.runID)
					if err == nil || !strings.Contains(err.Error(), test.want) {
						t.Fatalf("RequireGateRouteAdmitted error = %v, want containing %q", err, test.want)
					}
				})
			}
		})
	}
}
