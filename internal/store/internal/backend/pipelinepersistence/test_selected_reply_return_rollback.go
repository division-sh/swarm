package pipelinepersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

type SelectedReplyReturnPublicationCounts struct {
	Events, Deliveries, Lineages, Scopes int
}

type SelectedReplyReturnPublicationRollbackProof struct {
	Drafted, AfterRollback SelectedReplyReturnPublicationCounts
}

var errSelectedReplyReturnProofRollback = errors.New("selected accepted-return publication proof rollback")

func draftSelectedReplyReturnRollbackProof(ctx context.Context, attempt *mutationprotocol.Attempt, writer SelectedForkCommitTxOwner, request bus.CommitSelectedForkEventRequest, proof *SelectedReplyReturnPublicationRollbackProof) (bus.CommittedSelectedForkEvent, error) {
	if len(request.Commit.ReplyReturns) != 1 || len(request.Commit.ReplyClaims) != 0 || len(request.Commit.ReplyCreations) != 0 {
		return bus.CommittedSelectedForkEvent{}, fmt.Errorf("rollback proof requires its exact require-only accepted return")
	}
	result, err := writer.CommitSelectedForkTx(ctx, attempt, request)
	if err != nil {
		return result, err
	}
	if result.AppendOutcome != bus.EventAppendInserted {
		return result, fmt.Errorf("rollback proof requires a newly drafted accepted return")
	}
	if err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		proof.Drafted, err = readSelectedReplyReturnPublicationCountsTx(ctx, tx, request)
		return err
	}); err != nil {
		return result, err
	}
	want := SelectedReplyReturnPublicationCounts{Events: 1, Deliveries: len(request.Commit.DeliveryRoutes), Lineages: 1, Scopes: 1}
	if proof.Drafted != want || want.Deliveries == 0 {
		return result, fmt.Errorf("accepted-return rollback proof lacks complete drafted evidence: got=%+v want=%+v", proof.Drafted, want)
	}
	return result, errSelectedReplyReturnProofRollback
}

func readSelectedReplyReturnPublicationCountsTx(ctx context.Context, tx *sql.Tx, request bus.CommitSelectedForkEventRequest) (SelectedReplyReturnPublicationCounts, error) {
	var out SelectedReplyReturnPublicationCounts
	err := tx.QueryRowContext(ctx, `SELECT
		(SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_id=$2),
		(SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND event_id=$2),
		(SELECT COUNT(*) FROM run_fork_selected_contract_executions WHERE fork_run_id=$1 AND fork_event_id=$2),
		(SELECT COUNT(*) FROM committed_replay_scopes WHERE event_id=$2)`,
		request.Commit.Event.Event().RunID(), request.Commit.Event.ID()).
		Scan(&out.Events, &out.Deliveries, &out.Lineages, &out.Scopes)
	return out, err
}

func (s *PipelinePostgresOwner) ReadSelectedReplyReturnPublicationCountsForTest(ctx context.Context, request bus.CommitSelectedForkEventRequest) (SelectedReplyReturnPublicationCounts, error) {
	var out SelectedReplyReturnPublicationCounts
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = readSelectedReplyReturnPublicationCountsTx(ctx, tx, request)
		return err
	})
	return out, err
}

func (s *PipelineSQLiteOwner) ReadSelectedReplyReturnPublicationCountsForTest(ctx context.Context, request bus.CommitSelectedForkEventRequest) (SelectedReplyReturnPublicationCounts, error) {
	var out SelectedReplyReturnPublicationCounts
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = readSelectedReplyReturnPublicationCountsTx(ctx, tx, request)
		return err
	})
	return out, err
}

func (s *PipelinePostgresOwner) ProveSelectedReplyReturnPublicationRollbackForTest(ctx context.Context, request bus.CommitSelectedForkEventRequest) (SelectedReplyReturnPublicationRollbackProof, error) {
	var proof SelectedReplyReturnPublicationRollbackProof
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
	outcome := mutationprotocol.RunRetainedPostgresWithOptions(ctx, state.postgresLease.Session(), &sql.TxOptions{Isolation: sql.LevelSerializable}, mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, func(ctx context.Context, attempt *mutationprotocol.Attempt) (bus.CommittedSelectedForkEvent, error) {
		return draftSelectedReplyReturnRollbackProof(ctx, attempt, s.selectedFork, request, &proof)
	})
	if outcome.Acknowledged() || outcome.Err() != errSelectedReplyReturnProofRollback {
		return proof, fmt.Errorf("accepted-return rollback acknowledgment=%t: %w", outcome.Acknowledged(), outcome.Err())
	}
	err = s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		proof.AfterRollback, err = readSelectedReplyReturnPublicationCountsTx(ctx, tx, request)
		return err
	})
	return proof, err
}

func (s *PipelineSQLiteOwner) ProveSelectedReplyReturnPublicationRollbackForTest(ctx context.Context, request bus.CommitSelectedForkEventRequest) (SelectedReplyReturnPublicationRollbackProof, error) {
	var proof SelectedReplyReturnPublicationRollbackProof
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
	outcome := mutationprotocol.RunSQLite(ctx, s.backend, "sqlite selected accepted-return rollback proof", mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, func(ctx context.Context, attempt *mutationprotocol.Attempt) (bus.CommittedSelectedForkEvent, error) {
		return draftSelectedReplyReturnRollbackProof(ctx, attempt, s.selectedFork, request, &proof)
	})
	if outcome.Acknowledged() || outcome.Err() != errSelectedReplyReturnProofRollback {
		return proof, fmt.Errorf("accepted-return rollback acknowledgment=%t: %w", outcome.Acknowledged(), outcome.Err())
	}
	err = s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		proof.AfterRollback, err = readSelectedReplyReturnPublicationCountsTx(ctx, tx, request)
		return err
	})
	return proof, err
}
