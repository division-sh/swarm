package pipelinepersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

type SelectedFanOutPublicationCounts struct {
	Events, Deliveries, Lineages, Continued int
}

type SelectedFanOutPublicationRollbackProof struct {
	Drafted, AfterRollback SelectedFanOutPublicationCounts
}

var errSelectedFanOutProofRollback = errors.New("selected fan-out publication proof rollback")

// This fixed fault executes the real publication owner under its original
// claim, then forces rollback after all four facts have been drafted.
func draftSelectedFanOutRollbackProof(ctx context.Context, attempt *mutationprotocol.Attempt, writer SelectedForkCommitTxOwner, request runtimebus.CommitSelectedForkEventRequest, proof *SelectedFanOutPublicationRollbackProof) (runtimebus.CommittedSelectedForkEvent, error) {
	result, err := writer.CommitSelectedForkTx(ctx, attempt, request)
	if err != nil {
		return result, err
	}
	if result.AppendOutcome != runtimebus.EventAppendInserted {
		return result, fmt.Errorf("rollback proof requires a newly drafted selected publication")
	}
	if err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		proof.Drafted, err = readSelectedFanOutPublicationCountsTx(ctx, tx, request)
		return err
	}); err != nil {
		return result, err
	}
	want := SelectedFanOutPublicationCounts{Events: 1, Deliveries: len(request.Commit.DeliveryRoutes), Lineages: 1, Continued: 1}
	if proof.Drafted != want || want.Deliveries == 0 {
		return result, fmt.Errorf("rollback proof lacks complete continuing publication: got=%+v want=%+v", proof.Drafted, want)
	}
	return result, errSelectedFanOutProofRollback
}

func readSelectedFanOutPublicationCountsTx(ctx context.Context, tx *sql.Tx, request runtimebus.CommitSelectedForkEventRequest) (SelectedFanOutPublicationCounts, error) {
	var out SelectedFanOutPublicationCounts
	err := tx.QueryRowContext(ctx, `SELECT
		(SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_id=$2),
		(SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND event_id=$2),
		(SELECT COUNT(*) FROM run_fork_selected_contract_executions WHERE fork_run_id=$1 AND fork_event_id=$2),
		(SELECT COUNT(*) FROM fan_out_outcomes WHERE run_id=$1 AND event_id=$2 AND outcome_kind='continued')`,
		request.Commit.Event.Event().RunID(), request.Commit.Event.ID()).
		Scan(&out.Events, &out.Deliveries, &out.Lineages, &out.Continued)
	return out, err
}

func (s *PipelinePostgresOwner) ProveSelectedFanOutPublicationRollbackForTest(ctx context.Context, request runtimebus.CommitSelectedForkEventRequest) (SelectedFanOutPublicationRollbackProof, error) {
	var proof SelectedFanOutPublicationRollbackProof
	if s == nil || s.selectedFork == nil {
		return proof, errors.New("rollback proof requires the original selected publication owner")
	}
	ctx, err := selectedForkCommitContext(ctx, request)
	if err != nil {
		return proof, err
	}
	state, err := s.lockPostgresPipelineClaim(request.Commit.PipelineClaim)
	if err != nil {
		return proof, err
	}
	defer state.operationMu.Unlock()
	outcome := mutationprotocol.RunRetainedPostgresWithOptions(ctx, state.postgresLease.Session(), &sql.TxOptions{Isolation: sql.LevelSerializable}, mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, func(ctx context.Context, attempt *mutationprotocol.Attempt) (runtimebus.CommittedSelectedForkEvent, error) {
		return draftSelectedFanOutRollbackProof(ctx, attempt, s.selectedFork, request, &proof)
	})
	if outcome.Acknowledged() || outcome.Err() != errSelectedFanOutProofRollback {
		return proof, fmt.Errorf("rollback proof acknowledgment=%t: %w", outcome.Acknowledged(), outcome.Err())
	}
	err = s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		proof.AfterRollback, err = readSelectedFanOutPublicationCountsTx(ctx, tx, request)
		return err
	})
	return proof, err
}

func (s *PipelineSQLiteOwner) ProveSelectedFanOutPublicationRollbackForTest(ctx context.Context, request runtimebus.CommitSelectedForkEventRequest) (SelectedFanOutPublicationRollbackProof, error) {
	var proof SelectedFanOutPublicationRollbackProof
	if s == nil || s.selectedFork == nil {
		return proof, errors.New("rollback proof requires the original selected publication owner")
	}
	ctx, err := selectedForkCommitContext(ctx, request)
	if err != nil {
		return proof, err
	}
	state, err := s.lockSQLitePipelineClaim(request.Commit.PipelineClaim)
	if err != nil {
		return proof, err
	}
	defer state.operationMu.Unlock()
	outcome := mutationprotocol.RunSQLite(ctx, s.backend, "sqlite selected fan-out publication rollback proof", mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, func(ctx context.Context, attempt *mutationprotocol.Attempt) (runtimebus.CommittedSelectedForkEvent, error) {
		return draftSelectedFanOutRollbackProof(ctx, attempt, s.selectedFork, request, &proof)
	})
	if outcome.Acknowledged() || outcome.Err() != errSelectedFanOutProofRollback {
		return proof, fmt.Errorf("rollback proof acknowledgment=%t: %w", outcome.Acknowledged(), outcome.Err())
	}
	err = s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		proof.AfterRollback, err = readSelectedFanOutPublicationCountsTx(ctx, tx, request)
		return err
	})
	return proof, err
}
