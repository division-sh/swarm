package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
)

// ReadSemanticEventFixtureEvidenceForTest keeps record, revision and delivery
// observations in one read snapshot owned by the original selected coordinator.

func ReadSemanticEventFixtureEvidenceForTest(ctx context.Context, selected any, runID, eventID string) (SemanticEventFixtureEvidence, error) {
	var empty SemanticEventFixtureEvidence
	if strings.TrimSpace(runID) == "" || strings.TrimSpace(eventID) == "" {
		return empty, fmt.Errorf("semantic event fixture evidence requires run and event identities")
	}
	dialect, err := eventFixtureDialectForTest(selected)
	if err != nil {
		return empty, err
	}
	evidence := SemanticEventFixtureEvidence{
		DeliveryProjections: make(map[string][18]string),
		DeliveryStatuses:    make(map[string]string),
	}
	read := func(ctx context.Context, tx *sql.Tx) error {
		if err := readSemanticEventFacts(ctx, tx, string(dialect) == "postgres", runID, eventID, &evidence); err != nil {
			return err
		}
		count, err := runforkrevision.CountActivityJournalRevisionsForTest(ctx, tx, runID)
		if err != nil {
			return err
		}
		evidence.RevisionCount = int(count)
		if err := readSemanticEventPipelineReceipt(ctx, tx, eventID, &evidence); err != nil {
			return err
		}
		evidence.DeliveryProjections, evidence.DeliveryStatuses, err = delivery.ReadSemanticEventDeliveryStorage(ctx, tx, eventID)
		if err != nil {
			return err
		}
		return tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_receipts
			WHERE event_id = $1 AND subscriber_type <> 'platform'`, eventID).Scan(&evidence.NonPlatformReceiptCount)
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		err = owner.backend.RunReadTransaction(ctx, read)
	case *SQLiteRuntimeStore:
		err = owner.backend.RunReadTransaction(ctx, read)
	}
	if err != nil {
		return empty, err
	}
	return evidence, nil
}

func readSemanticEventFacts(ctx context.Context, tx *sql.Tx, postgres bool, runID, eventID string, evidence *SemanticEventFixtureEvidence) error {
	var err error
	evidence.Record, evidence.RecordFound, err = loadCanonicalFixtureRecordTx(ctx, tx, postgres, eventID)
	if err != nil {
		return err
	}
	if evidence.RecordFound && evidence.Record.RunID != runID {
		return fmt.Errorf("semantic event fixture %s does not belong to run %s", eventID, runID)
	}
	if err := tx.QueryRowContext(ctx, `SELECT status, COALESCE(event_count, 0) FROM runs WHERE run_id = $1`, runID).
		Scan(&evidence.RunStatus, &evidence.RunEventCount); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	scope, err := pipelinepersistence.LoadCommittedScope(ctx, tx, eventID, postgres)
	if err == nil {
		evidence.CommittedScope = string(scope)
		evidence.CommittedScopeFound = true
	} else if !errors.Is(err, runtimepipelineobligation.ErrMissingScope) {
		return err
	}
	evidence.SettledDeliveryAttemptCount, err = delivery.ReadSemanticEventSettledAttemptCount(ctx, tx, eventID)
	return err
}

func readSemanticEventPipelineReceipt(ctx context.Context, tx *sql.Tx, eventID string, evidence *SemanticEventFixtureEvidence) error {
	receipt, err := pipelinepersistence.ReadPipelineReceiptStorage(ctx, tx, eventID)
	if err != nil {
		return err
	}
	evidence.PipelineReceiptCount = receipt.Count
	evidence.PipelineReceiptOutcome = receipt.Outcome
	evidence.PipelineReceiptReason = receipt.Reason
	evidence.PipelineReceiptFailure = receipt.Failure
	return nil
}
