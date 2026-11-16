package runtimepersistence

import (
	"context"
	"fmt"
	"strings"

	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
)

func ReadCommittedPipelineScopeForTest(ctx context.Context, selected any, eventID string) (runtimepipelineobligation.CommittedScope, error) {
	if err := validateSelectedForkStorageIdentity(eventID); err != nil {
		return "", err
	}
	if err := validateChannelObservationOwner(selected); err != nil {
		return "", err
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		return owner.pipelinePostgresOwner.ReadCommittedPipelineScopeForTest(ctx, eventID)
	case *SQLiteRuntimeStore:
		return owner.pipelineSQLiteOwner.ReadCommittedPipelineScopeForTest(ctx, eventID)
	}
	return "", nil
}

// ReadSemanticEventFixtureEvidenceForTest keeps record, revision and delivery
// observations in one read snapshot owned by the original selected coordinator.

func ReadSemanticEventFixtureEvidenceForTest(ctx context.Context, selected any, runID, eventID string) (SemanticEventFixtureEvidence, error) {
	var empty SemanticEventFixtureEvidence
	if strings.TrimSpace(runID) == "" || strings.TrimSpace(eventID) == "" {
		return empty, fmt.Errorf("semantic event fixture evidence requires run and event identities")
	}
	if _, err := eventFixtureDialectForTest(selected); err != nil {
		return empty, err
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		return owner.eventPostgresOwner.ReadSemanticEventFixtureEvidenceForTest(ctx, runID, eventID)
	case *SQLiteRuntimeStore:
		return owner.eventSQLiteOwner.ReadSemanticEventFixtureEvidenceForTest(ctx, runID, eventID)
	default:
		return empty, fmt.Errorf("event fixture store %T is unsupported", selected)
	}
}
