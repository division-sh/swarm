package apiv1

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/google/uuid"
)

func (emptyAdministrativeResetLifecycle) SettleResources(context.Context) error { return nil }

func (emptyAdministrativeResetLifecycle) ResetSourceProjections(context.Context) ([]destructivereset.SourceProjection, error) {
	return nil, nil
}

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

func administrativeResetContext(t *testing.T, ctx context.Context, capability startupownership.ProcessCapability) context.Context {
	t.Helper()
	original, err := capability.Evidence()
	if err != nil {
		t.Fatal(err)
	}
	process := worklifetime.NewProcess()
	if err := process.BindOriginalProcess(original.LeaseStamp()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		process.Retire()
		if _, err := process.Join(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return worklifetime.WithProcess(ctx, process)
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
