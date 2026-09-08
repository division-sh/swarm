package serveapp

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/google/uuid"
)

type blockedAllocationSession struct {
	*supervisorTestRetainedSession
	operation destructivereset.Operation
	entered   chan struct{}
	settle    chan struct{}
}

func (s *blockedAllocationSession) ReadResetOperation(context.Context, string) (destructivereset.Operation, error) {
	return s.operation, nil
}
func (s *blockedAllocationSession) AdvanceResetOperation(ctx context.Context, _, _ destructivereset.Operation) error {
	close(s.entered)
	select {
	case <-s.settle:
	case <-ctx.Done():
	}
	return errors.New("allocation admission rejected")
}

func TestResetProjectionCreationWaitsForDurableAllocationAdmission(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "schema.yaml"), []byte("name: allocation-fence\n"), 0600); err != nil {
		t.Fatal(err)
	}
	artifact, err := sourceartifact.AdmitDirectory(root)
	if err != nil {
		t.Fatal(err)
	}
	private := t.TempDir()
	t.Setenv("TMPDIR", private)
	operation, err := destructivereset.NewOperation(destructivereset.Request{OperationID: uuid.NewString(), ActorTokenID: "operator", RequestHash: "allocation-fence", RequestedAt: time.Now().UTC(), IncludeSourceArtifactsSet: true})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{{BundleHash: artifact.BundleHash()}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	operation.SourceSet = &plan
	operation.Phase, operation.Revision = destructivereset.PhaseContainersSettled, 5
	operation.Plan = &destructivereset.Result{OperationName: destructivereset.DefaultOperationName, PlannedAt: operation.Request.RequestedAt, Plan: destructivereset.Plan{CleanupRunSetKnown: true}}
	operation.Quiescence = &destructivereset.QuiescenceResult{OperationName: destructivereset.DefaultOperationName}
	operation.Cleanup = &destructivereset.CleanupResult{OperationName: destructivereset.DefaultOperationName}
	operation.Containers = &destructivereset.ContainerResetResult{OperationName: destructivereset.DefaultOperationName}
	if err := operation.Validate(); err != nil {
		t.Fatal(err)
	}
	session := &blockedAllocationSession{supervisorTestRetainedSession: &supervisorTestRetainedSession{}, operation: operation, entered: make(chan struct{}), settle: make(chan struct{})}
	capability, err := startupownership.NewProcessCapability(session)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := capability.Release(context.Background()); err != nil {
			t.Error(err)
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	supervisor := &processLifecycleSupervisor{processCapability: capability, resetOperationID: operation.Request.OperationID}
	done := make(chan error, 1)
	go func() {
		projection, err := supervisor.materializeResetProjection(ctx, artifact, true)
		if projection != nil {
			err = errors.Join(err, projection.Release(), errors.New("rejected admission returned a projection"))
		}
		done <- err
	}()
	select {
	case <-session.entered:
	case err := <-done:
		t.Fatalf("did not reach allocation admission: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("allocation admission not reached")
	}
	entries, readErr := os.ReadDir(private)
	close(session.settle)
	if readErr != nil || len(entries) != 0 {
		t.Errorf("projection created before durable admission: %v, %v", entries, readErr)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("rejected allocation returned success")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("rejected allocation did not settle")
	}
	entries, err = os.ReadDir(private)
	if err != nil || len(entries) != 0 {
		t.Fatalf("rejected allocation left resources: %v, %v", entries, err)
	}
}
