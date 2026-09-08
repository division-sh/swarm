package apiv1

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/google/uuid"
)

func acquireAdministrativeResetCapability(t *testing.T, selected startupownership.Store) startupownership.ProcessCapability {
	t.Helper()
	cap, err := selected.AcquireProcessCapability(context.Background(), startupownership.AcquireRequest{OwnerID: "administrative-layering", BootID: uuid.NewString(), RuntimeInstanceID: uuid.NewString()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := cap.Release(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return cap
}

type administrativeResetCleanup struct {
	capability startupownership.ProcessCapability
}

func (s administrativeResetCleanup) ApplyDestructiveResetCleanup(ctx context.Context, req destructivereset.CleanupRequest) (destructivereset.CleanupResult, error) {
	return s.capability.ApplyDestructiveResetCleanup(ctx, req, nil)
}

// These capacity tests contain no runtime contexts. Served lifecycle tests own
// proof of actual execution withdrawal, joining and reconstruction.
type emptyAdministrativeResetLifecycle struct{}

func (emptyAdministrativeResetLifecycle) BeginDestructiveReset(context.Context, string) (destructivereset.RuntimeReset, error) {
	return emptyAdministrativeResetLifecycle{}, nil
}
func (emptyAdministrativeResetLifecycle) Complete(context.Context, bool) error { return nil }
func (emptyAdministrativeResetLifecycle) Release()                             {}
