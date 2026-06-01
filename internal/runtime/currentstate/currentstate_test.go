package currentstate

import (
	"context"
	"strings"
	"testing"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/google/uuid"
)

func TestRequireIdentityRequiresRunID(t *testing.T) {
	_, err := RequireIdentity(context.Background(), uuid.NewString())
	if err == nil || !strings.Contains(err.Error(), "run_id is required") {
		t.Fatalf("RequireIdentity error = %v, want missing run_id", err)
	}
}

func TestRunIDFromContextReportsAbsentRunID(t *testing.T) {
	runID, ok, err := RunIDFromContext(context.Background())
	if err != nil {
		t.Fatalf("RunIDFromContext: %v", err)
	}
	if ok || runID != "" {
		t.Fatalf("RunIDFromContext = (%q, %v), want absent", runID, ok)
	}
}

func TestRequireIdentityAcceptsRunScopedEntity(t *testing.T) {
	runID := uuid.NewString()
	entityID := uuid.NewString()
	got, err := RequireIdentity(runtimecorrelation.WithRunID(context.Background(), runID), entityID)
	if err != nil {
		t.Fatalf("RequireIdentity: %v", err)
	}
	if got.RunID != runID || got.EntityID != entityID {
		t.Fatalf("identity = %+v, want run=%s entity=%s", got, runID, entityID)
	}
}
