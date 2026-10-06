package runtimepersistence

import (
	"context"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/google/uuid"
)

// CaptureRunForkSnapshotForTest captures all canonical historical families on
// the selected coordinator without lending revision or transaction authority.
func CaptureRunForkSnapshotForTest(ctx context.Context, selected any, runID string) error {
	runID = strings.TrimSpace(runID)
	if _, err := uuid.Parse(runID); err != nil {
		return fmt.Errorf("run-fork snapshot fixture requires a run identity: %w", err)
	}
	capture := func(_ context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
		for _, family := range runforkrevision.AllFamilies() {
			if err := attempt.AddWholeFamily(runID, family); err != nil {
				return struct{}{}, err
			}
		}
		return struct{}{}, nil
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		if owner != nil && owner.backend != nil {
			if err := owner.requireCurrentSchema(); err != nil {
				return err
			}
			return mutationprotocol.RunPostgres(ctx, owner.backend, mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, owner.runLifecycleCandidates, capture).Err()
		}
	case *SQLiteRuntimeStore:
		if owner != nil && owner.backend != nil {
			if err := owner.requireCurrentSchema(); err != nil {
				return err
			}
			return mutationprotocol.RunSQLite(ctx, owner.backend, "run-fork snapshot fixture", mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, owner.runLifecycleCandidates, capture).Err()
		}
	}
	return fmt.Errorf("run-fork snapshot fixture requires the original selected store, got %T", selected)
}
