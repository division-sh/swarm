package eventpersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	eventrecordpostgres "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/postgres"
	eventrecordsqlite "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/sqlite"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
)

type SemanticEventFixtureEvidence struct {
	Record                      eventrecord.Record
	RecordFound                 bool
	RevisionCount               int
	PipelineReceiptCount        int
	PipelineReceiptOutcome      string
	PipelineReceiptReason       string
	PipelineReceiptFailure      *runtimefailures.Envelope
	CommittedScope              string
	CommittedScopeFound         bool
	RunStatus                   string
	RunEventCount               int
	SettledDeliveryAttemptCount int
	DeliveryProjections         map[string][18]string
	DeliveryStatuses            map[string]string
	NonPlatformReceiptCount     int
}

func loadCanonicalFixtureRecordTx(ctx context.Context, tx *sql.Tx, postgres bool, eventID string) (eventrecord.Record, bool, error) {
	if postgres {
		return eventrecordpostgres.Load(ctx, tx, eventID)
	}
	return eventrecordsqlite.Load(ctx, tx, eventID)
}

func (s *EventPostgresOwner) ReadSemanticEventFixtureEvidenceForTest(ctx context.Context, runID, eventID string) (SemanticEventFixtureEvidence, error) {
	var evidence SemanticEventFixtureEvidence
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		evidence, err = readSemanticEventFixtureEvidenceTx(ctx, tx, true, runID, eventID)
		return err
	})
	if err != nil {
		return SemanticEventFixtureEvidence{}, err
	}
	return evidence, nil
}

func (s *EventSQLiteOwner) ReadSemanticEventFixtureEvidenceForTest(ctx context.Context, runID, eventID string) (SemanticEventFixtureEvidence, error) {
	var evidence SemanticEventFixtureEvidence
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		evidence, err = readSemanticEventFixtureEvidenceTx(ctx, tx, false, runID, eventID)
		return err
	})
	if err != nil {
		return SemanticEventFixtureEvidence{}, err
	}
	return evidence, nil
}

func readSemanticEventFixtureEvidenceTx(ctx context.Context, tx *sql.Tx, postgres bool, runID, eventID string) (SemanticEventFixtureEvidence, error) {
	evidence := SemanticEventFixtureEvidence{
		DeliveryProjections: make(map[string][18]string),
		DeliveryStatuses:    make(map[string]string),
	}
	read := func() error {
		if err := readSemanticEventFacts(ctx, tx, postgres, runID, eventID, &evidence); err != nil {
			return err
		}
		var count int64
		err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_fork_revisions WHERE run_id=$1`, runID).Scan(&count)
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

	if err := read(); err != nil {
		return SemanticEventFixtureEvidence{}, err
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
	if err := tx.QueryRowContext(ctx, `SELECT status, (SELECT c.event_count FROM run_event_counts c WHERE c.run_id = runs.run_id) FROM runs WHERE run_id = $1`, runID).
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
