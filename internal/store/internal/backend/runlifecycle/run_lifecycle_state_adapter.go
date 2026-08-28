package runlifecycle

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	privaterunforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
)

const runLifecycleActiveStateSQLValues = "'" +
	string(runtimerunlifecycle.StateRunning) + "', '" +
	string(runtimerunlifecycle.StatePaused) + "'"

type terminalRunMutation struct {
	RunID                         string
	State                         runtimerunlifecycle.State
	Failure                       *runtimefailures.Envelope
	ContinuedAsRunID              string
	EndedAt                       time.Time
	IncludeCommittedDecisionCards bool
}

func terminalRunMutationFromRequest(request runtimerunlifecycle.TerminalRequest) (terminalRunMutation, error) {
	request.EndedAt = runtimerunlifecycle.CanonicalTimestamp(request.EndedAt)
	if err := request.Validate(); err != nil {
		return terminalRunMutation{}, err
	}
	return terminalRunMutation{
		RunID: request.RunID, State: request.State, Failure: request.Failure,
		ContinuedAsRunID: request.ContinuedAsRunID, EndedAt: request.EndedAt,
	}, nil
}

func terminalRunMutationFromForkSource(request runtimerunlifecycle.ForkSourceRequest) (terminalRunMutation, error) {
	request.EndedAt = runtimerunlifecycle.CanonicalTimestamp(request.EndedAt)
	if err := request.Validate(); err != nil {
		return terminalRunMutation{}, err
	}
	return terminalRunMutation{
		RunID: request.RunID, State: runtimerunlifecycle.StateForked,
		ContinuedAsRunID: request.ContinuedAsRunID, EndedAt: request.EndedAt,
		IncludeCommittedDecisionCards: true,
	}, nil
}

func terminalRunMutationForCompletion(runID string, endedAt time.Time) (terminalRunMutation, error) {
	runID = strings.TrimSpace(runID)
	endedAt = runtimerunlifecycle.CanonicalTimestamp(endedAt)
	if runID == "" {
		return terminalRunMutation{}, errors.New("successful run completion requires run_id")
	}
	if endedAt.IsZero() {
		return terminalRunMutation{}, errors.New("successful run completion requires ended_at")
	}
	return terminalRunMutation{
		RunID: runID, State: runtimerunlifecycle.StateCompleted, EndedAt: endedAt,
	}, nil
}

func (s *RunLifecyclePostgresOwner) MarkTerminalRun(
	ctx context.Context,
	request runtimerunlifecycle.TerminalRequest,
) (runtimerunlifecycle.Snapshot, runtimerunlifecycle.MutationDisposition, error) {
	type result struct {
		snapshot    runtimerunlifecycle.Snapshot
		disposition runtimerunlifecycle.MutationDisposition
	}
	value, err := runPostgresLifecycleOperation(ctx, s, func(ctx context.Context, mutation postgresRunLifecycleMutation) (result, error) {
		snapshot, disposition, err := mutation.MarkTerminal(ctx, request)
		return result{snapshot: snapshot, disposition: disposition}, err
	})
	return value.snapshot, value.disposition, err
}

func (s *RunLifecycleSQLiteOwner) MarkTerminalRun(
	ctx context.Context,
	request runtimerunlifecycle.TerminalRequest,
) (runtimerunlifecycle.Snapshot, runtimerunlifecycle.MutationDisposition, error) {
	type result struct {
		snapshot    runtimerunlifecycle.Snapshot
		disposition runtimerunlifecycle.MutationDisposition
	}
	value, err := runSQLiteLifecycleOperation(ctx, s, func(ctx context.Context, mutation sqliteRunLifecycleMutation) (result, error) {
		snapshot, disposition, err := mutation.MarkTerminal(ctx, request)
		return result{snapshot: snapshot, disposition: disposition}, err
	})
	return value.snapshot, value.disposition, err
}

func (s *RunLifecyclePostgresOwner) LoadSnapshotTx(ctx context.Context, tx *sql.Tx, runID string, forUpdate bool) (runtimerunlifecycle.Snapshot, error) {
	return loadPostgresRunLifecycleSnapshot(ctx, tx, runID, forUpdate)
}

func (s *RunLifecycleSQLiteOwner) LoadSnapshotTx(ctx context.Context, tx *sql.Tx, runID string) (runtimerunlifecycle.Snapshot, error) {
	return loadSQLiteRunLifecycleSnapshot(ctx, tx, runID)
}

func loadPostgresRunLifecycleSnapshot(
	ctx context.Context,
	q rowQueryer,
	runID string,
	forUpdate bool,
) (runtimerunlifecycle.Snapshot, error) {
	runID = nullUUIDString(runID)
	if q == nil || runID == "" {
		return runtimerunlifecycle.Snapshot{}, errors.New("run lifecycle snapshot requires query authority and run_id")
	}
	query := `
		SELECT run_id::text, status, bundle_hash, bundle_source, origin_kind,
		       COALESCE(trigger_event_id::text, ''), COALESCE(trigger_event_type, ''),
		       COALESCE(origin_service_id::text, ''), COALESCE(origin_generation, 0),
		       COALESCE(forked_from_run_id::text, ''), COALESCE(forked_from_event_id::text, ''),
		       COALESCE(event_count, 0),
		       COALESCE((SELECT COUNT(DISTINCT es.entity_id)::integer FROM entity_state es WHERE es.run_id = runs.run_id), 0),
		       failure, COALESCE(continued_as_run_id::text, ''), started_at, ended_at
		FROM runs
		WHERE run_id = $1::uuid
	`
	if forUpdate {
		query += ` FOR UPDATE`
	}
	var (
		snapshot      runtimerunlifecycle.Snapshot
		state         string
		originKind    string
		eventID       string
		eventType     string
		serviceID     string
		generation    int64
		sourceRunID   string
		sourceEventID string
		failureRaw    []byte
		startedAt     sql.NullTime
		endedAt       sql.NullTime
	)
	err := q.QueryRowContext(ctx, query, runID).Scan(
		&snapshot.RunID, &state, &snapshot.BundleHash, &snapshot.BundleSource,
		&originKind, &eventID, &eventType, &serviceID, &generation, &sourceRunID, &sourceEventID,
		&snapshot.EventCount, &snapshot.EntityCount, &failureRaw, &snapshot.ContinuedAsRunID, &startedAt, &endedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return runtimerunlifecycle.Snapshot{}, &runtimerunlifecycle.RunNotFoundError{RunID: runID}
	}
	if err != nil {
		return runtimerunlifecycle.Snapshot{}, fmt.Errorf("load PostgreSQL run lifecycle snapshot: %w", err)
	}
	snapshot.Origin, err = runtimerunlifecycle.DecodeRunOrigin(
		originKind, eventID, eventType, serviceID, generation, sourceRunID, sourceEventID,
	)
	if err != nil {
		return runtimerunlifecycle.Snapshot{}, fmt.Errorf("decode PostgreSQL run lifecycle origin: %w", err)
	}
	if err := requireStandingGenerationOriginRelation(ctx, q, snapshot.RunID, snapshot.Origin, true); err != nil {
		return runtimerunlifecycle.Snapshot{}, err
	}
	snapshot.State, err = runtimerunlifecycle.ParseState(state)
	if err != nil {
		return runtimerunlifecycle.Snapshot{}, err
	}
	if len(failureRaw) > 0 {
		failure, decodeErr := runtimefailures.UnmarshalEnvelope(failureRaw)
		if decodeErr != nil {
			return runtimerunlifecycle.Snapshot{}, fmt.Errorf("decode PostgreSQL run lifecycle failure: %w", decodeErr)
		}
		snapshot.Failure = &failure
	}
	if startedAt.Valid {
		snapshot.StartedAt = startedAt.Time.UTC()
	}
	if endedAt.Valid {
		value := endedAt.Time.UTC()
		snapshot.EndedAt = &value
	}
	if err := snapshot.Validate(); err != nil {
		return runtimerunlifecycle.Snapshot{}, fmt.Errorf("validate PostgreSQL run lifecycle snapshot: %w", err)
	}
	return snapshot, nil
}

func loadSQLiteRunLifecycleSnapshot(
	ctx context.Context,
	q rowQueryer,
	runID string,
) (runtimerunlifecycle.Snapshot, error) {
	runID = nullUUIDString(runID)
	if q == nil || runID == "" {
		return runtimerunlifecycle.Snapshot{}, errors.New("run lifecycle snapshot requires query authority and run_id")
	}
	var (
		snapshot      runtimerunlifecycle.Snapshot
		state         string
		originKind    string
		eventID       string
		eventType     string
		serviceID     string
		generation    int64
		sourceRunID   string
		sourceEventID string
		failureRaw    sql.NullString
		startedAt     any
		endedAt       any
	)
	err := q.QueryRowContext(ctx, `
		SELECT run_id, status, bundle_hash, bundle_source, origin_kind,
		       COALESCE(trigger_event_id, ''), COALESCE(trigger_event_type, ''),
		       COALESCE(origin_service_id, ''), COALESCE(origin_generation, 0),
		       COALESCE(forked_from_run_id, ''), COALESCE(forked_from_event_id, ''),
		       COALESCE(event_count, 0),
		       COALESCE((SELECT COUNT(DISTINCT es.entity_id) FROM entity_state es WHERE es.run_id = runs.run_id), 0),
		       failure, COALESCE(continued_as_run_id, ''), started_at, ended_at
		FROM runs
		WHERE run_id = ?
	`, runID).Scan(
		&snapshot.RunID, &state, &snapshot.BundleHash, &snapshot.BundleSource,
		&originKind, &eventID, &eventType, &serviceID, &generation, &sourceRunID, &sourceEventID,
		&snapshot.EventCount, &snapshot.EntityCount, &failureRaw, &snapshot.ContinuedAsRunID, &startedAt, &endedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return runtimerunlifecycle.Snapshot{}, &runtimerunlifecycle.RunNotFoundError{RunID: runID}
	}
	if err != nil {
		return runtimerunlifecycle.Snapshot{}, fmt.Errorf("load SQLite run lifecycle snapshot: %w", err)
	}
	snapshot.Origin, err = runtimerunlifecycle.DecodeRunOrigin(
		originKind, eventID, eventType, serviceID, generation, sourceRunID, sourceEventID,
	)
	if err != nil {
		return runtimerunlifecycle.Snapshot{}, fmt.Errorf("decode SQLite run lifecycle origin: %w", err)
	}
	if err := requireStandingGenerationOriginRelation(ctx, q, snapshot.RunID, snapshot.Origin, false); err != nil {
		return runtimerunlifecycle.Snapshot{}, err
	}
	snapshot.State, err = runtimerunlifecycle.ParseState(state)
	if err != nil {
		return runtimerunlifecycle.Snapshot{}, err
	}
	if failureRaw.Valid && strings.TrimSpace(failureRaw.String) != "" {
		failure, decodeErr := runtimefailures.UnmarshalEnvelope([]byte(failureRaw.String))
		if decodeErr != nil {
			return runtimerunlifecycle.Snapshot{}, fmt.Errorf("decode SQLite run lifecycle failure: %w", decodeErr)
		}
		snapshot.Failure = &failure
	}
	if value, ok, decodeErr := sqliteTimeValue(startedAt); decodeErr != nil {
		return runtimerunlifecycle.Snapshot{}, decodeErr
	} else if ok {
		snapshot.StartedAt = value.UTC()
	}
	if value, ok, decodeErr := sqliteTimeValue(endedAt); decodeErr != nil {
		return runtimerunlifecycle.Snapshot{}, decodeErr
	} else if ok {
		value = value.UTC()
		snapshot.EndedAt = &value
	}
	if err := snapshot.Validate(); err != nil {
		return runtimerunlifecycle.Snapshot{}, fmt.Errorf("validate SQLite run lifecycle snapshot: %w", err)
	}
	return snapshot, nil
}

func requireStandingGenerationOriginRelation(
	ctx context.Context,
	q rowQueryer,
	runID string,
	origin runtimerunlifecycle.RunOrigin,
	postgres bool,
) error {
	query := `
		SELECT
			COUNT(*),
			COALESCE(SUM(CASE WHEN service_id = ? AND generation = ? THEN 1 ELSE 0 END), 0)
		FROM standing_service_generations
		WHERE run_id = ?
	`
	args := []any{origin.ServiceID(), origin.Generation(), runID}
	if postgres {
		query = `
			SELECT
				COUNT(*),
				COUNT(*) FILTER (WHERE service_id = NULLIF($2, '')::uuid AND generation = $3)
			FROM standing_service_generations
			WHERE run_id = $1::uuid
		`
		args = []any{runID, origin.ServiceID(), origin.Generation()}
	}
	var relations, matches int
	if err := q.QueryRowContext(ctx, query, args...).Scan(&relations, &matches); err != nil {
		return fmt.Errorf("validate standing generation run origin relation: %w", err)
	}
	if origin.Kind() == runtimerunlifecycle.OriginStandingGeneration {
		if relations == 1 && matches == 1 {
			return nil
		}
		return fmt.Errorf(
			"standing generation run origin relation mismatch: run_id=%s service_id=%s generation=%d relations=%d matches=%d",
			runID, origin.ServiceID(), origin.Generation(), relations, matches,
		)
	}
	if relations != 0 || matches != 0 {
		return fmt.Errorf(
			"non-standing run origin has standing generation relation: run_id=%s kind=%s relations=%d matches=%d",
			runID, origin.Kind(), relations, matches,
		)
	}
	return nil
}

func (s *RunLifecyclePostgresOwner) markRunTerminalTx(
	ctx context.Context,
	tx *sql.Tx,
	story runtimeauthoractivity.Mutation,
	effects *privaterunforkrevision.Effects,
	request runtimerunlifecycle.TerminalRequest,
) (runtimerunlifecycle.Snapshot, runtimerunlifecycle.MutationDisposition, error) {
	mutation, err := terminalRunMutationFromRequest(request)
	if err != nil {
		return runtimerunlifecycle.Snapshot{}, "", err
	}
	return s.markRunTerminalStateTx(ctx, tx, story, effects, mutation)
}

func (s *RunLifecyclePostgresOwner) markForkSourceTx(
	ctx context.Context,
	tx *sql.Tx,
	story runtimeauthoractivity.Mutation,
	effects *privaterunforkrevision.Effects,
	request runtimerunlifecycle.ForkSourceRequest,
) (runtimerunlifecycle.Snapshot, runtimerunlifecycle.MutationDisposition, error) {
	mutation, err := terminalRunMutationFromForkSource(request)
	if err != nil {
		return runtimerunlifecycle.Snapshot{}, "", err
	}
	return s.markRunTerminalStateTx(ctx, tx, story, effects, mutation)
}

func (s *RunLifecycleSQLiteOwner) markForkSourceTx(
	ctx context.Context,
	tx *sql.Tx,
	story runtimeauthoractivity.Mutation,
	effects *privaterunforkrevision.Effects,
	request runtimerunlifecycle.ForkSourceRequest,
) (runtimerunlifecycle.Snapshot, runtimerunlifecycle.MutationDisposition, error) {
	mutation, err := terminalRunMutationFromForkSource(request)
	if err != nil {
		return runtimerunlifecycle.Snapshot{}, "", err
	}
	return s.markRunTerminalStateTx(ctx, tx, story, effects, mutation)
}

func (s *RunLifecyclePostgresOwner) completeRunTx(
	ctx context.Context,
	tx *sql.Tx,
	story runtimeauthoractivity.Mutation,
	effects *privaterunforkrevision.Effects,
	runID string,
	endedAt time.Time,
) (runtimerunlifecycle.Snapshot, runtimerunlifecycle.MutationDisposition, error) {
	mutation, err := terminalRunMutationForCompletion(runID, endedAt)
	if err != nil {
		return runtimerunlifecycle.Snapshot{}, "", err
	}
	return s.markRunTerminalStateTx(ctx, tx, story, effects, mutation)
}

func (s *RunLifecyclePostgresOwner) markRunTerminalStateTx(
	ctx context.Context,
	tx *sql.Tx,
	story runtimeauthoractivity.Mutation,
	effects *privaterunforkrevision.Effects,
	request terminalRunMutation,
) (runtimerunlifecycle.Snapshot, runtimerunlifecycle.MutationDisposition, error) {
	if tx == nil {
		return runtimerunlifecycle.Snapshot{}, "", errors.New("PostgreSQL terminal run lifecycle mutation requires transaction")
	}
	if story == nil {
		return runtimerunlifecycle.Snapshot{}, "", fmt.Errorf("terminal run lifecycle mutation requires private story ownership")
	}
	current, err := loadPostgresRunLifecycleSnapshot(ctx, tx, request.RunID, true)
	if err != nil {
		return runtimerunlifecycle.Snapshot{}, "", err
	}
	if current.State.Terminal() {
		if current.State != request.State ||
			!sameRunLifecycleFailure(current.Failure, request.Failure) ||
			strings.TrimSpace(current.ContinuedAsRunID) != strings.TrimSpace(request.ContinuedAsRunID) {
			return runtimerunlifecycle.Snapshot{}, "", fmt.Errorf(
				"run %s already terminal with state %s", request.RunID, current.State,
			)
		}
		if err := clearPostgresCompletionCandidateTx(ctx, tx, request.RunID); err != nil {
			return runtimerunlifecycle.Snapshot{}, "", err
		}
		return current, runtimerunlifecycle.MutationExactNoop, nil
	}
	if effects == nil {
		return runtimerunlifecycle.Snapshot{}, "", errors.New("terminal run lifecycle mutation requires revision effects")
	}
	if err := (postgresRunLifecycleMutation{store: s, tx: tx, story: story, effects: effects}).SyncCounters(ctx, request.RunID); err != nil {
		return runtimerunlifecycle.Snapshot{}, "", err
	}
	if request.State != runtimerunlifecycle.StateCompleted {
		if _, err := s.delivery.TerminalizeRunDeliveriesTx(ctx, tx, story, effects, request.RunID, "run_"+string(request.State)); err != nil {
			return runtimerunlifecycle.Snapshot{}, "", err
		}
	}
	if err := s.decisionCards.SupersedeRunTx(
		ctx, tx, story, effects, request.RunID, "run_"+string(request.State), request.EndedAt.UTC(), request.IncludeCommittedDecisionCards,
	); err != nil {
		return runtimerunlifecycle.Snapshot{}, "", err
	}
	failureJSON, err := marshalRunLifecycleFailure(request.Failure)
	if err != nil {
		return runtimerunlifecycle.Snapshot{}, "", err
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE runs
		SET status = $2,
		    failure = $3::jsonb,
		    ended_at = $4,
		    continued_as_run_id = NULLIF($5, '')::uuid,
		    completion_due_at = NULL
		WHERE run_id = $1::uuid
		  AND status IN (`+runLifecycleActiveStateSQLValues+`)
	`, request.RunID, string(request.State), failureJSON, request.EndedAt.UTC(), strings.TrimSpace(request.ContinuedAsRunID))
	if err != nil {
		return runtimerunlifecycle.Snapshot{}, "", fmt.Errorf("write PostgreSQL terminal run lifecycle: %w", err)
	}
	if rows, rowsErr := result.RowsAffected(); rowsErr != nil {
		return runtimerunlifecycle.Snapshot{}, "", rowsErr
	} else if rows != 1 {
		return runtimerunlifecycle.Snapshot{}, "", errors.New("PostgreSQL terminal run lifecycle lost locked transition")
	}
	snapshot, err := loadPostgresRunLifecycleSnapshot(ctx, tx, request.RunID, false)
	if err != nil {
		return runtimerunlifecycle.Snapshot{}, "", err
	}
	if err := recordTerminalRunActivity(ctx, story, snapshot, request.Failure); err != nil {
		return runtimerunlifecycle.Snapshot{}, "", err
	}
	return snapshot, runtimerunlifecycle.MutationApplied, nil
}

func (s *RunLifecycleSQLiteOwner) markRunTerminalTx(
	ctx context.Context,
	tx *sql.Tx,
	story runtimeauthoractivity.Mutation,
	effects *privaterunforkrevision.Effects,
	request runtimerunlifecycle.TerminalRequest,
) (runtimerunlifecycle.Snapshot, runtimerunlifecycle.MutationDisposition, error) {
	mutation, err := terminalRunMutationFromRequest(request)
	if err != nil {
		return runtimerunlifecycle.Snapshot{}, "", err
	}
	return s.markRunTerminalStateTx(ctx, tx, story, effects, mutation)
}

func (s *RunLifecycleSQLiteOwner) markRunTerminalStateTx(
	ctx context.Context,
	tx *sql.Tx,
	story runtimeauthoractivity.Mutation,
	effects *privaterunforkrevision.Effects,
	request terminalRunMutation,
) (runtimerunlifecycle.Snapshot, runtimerunlifecycle.MutationDisposition, error) {
	if tx == nil {
		return runtimerunlifecycle.Snapshot{}, "", errors.New("SQLite terminal run lifecycle mutation requires transaction")
	}
	if story == nil {
		return runtimerunlifecycle.Snapshot{}, "", fmt.Errorf("terminal run lifecycle mutation requires private story ownership")
	}
	current, err := loadSQLiteRunLifecycleSnapshot(ctx, tx, request.RunID)
	if err != nil {
		return runtimerunlifecycle.Snapshot{}, "", err
	}
	if current.State.Terminal() {
		if current.State != request.State ||
			!sameRunLifecycleFailure(current.Failure, request.Failure) ||
			strings.TrimSpace(current.ContinuedAsRunID) != strings.TrimSpace(request.ContinuedAsRunID) {
			return runtimerunlifecycle.Snapshot{}, "", fmt.Errorf(
				"run %s already terminal with state %s", request.RunID, current.State,
			)
		}
		if err := clearSQLiteCompletionCandidateTx(ctx, tx, request.RunID); err != nil {
			return runtimerunlifecycle.Snapshot{}, "", err
		}
		return current, runtimerunlifecycle.MutationExactNoop, nil
	}
	if effects == nil {
		return runtimerunlifecycle.Snapshot{}, "", errors.New("terminal run lifecycle mutation requires revision effects")
	}
	if err := (sqliteRunLifecycleMutation{store: s, tx: tx, story: story, effects: effects}).SyncCounters(ctx, request.RunID); err != nil {
		return runtimerunlifecycle.Snapshot{}, "", err
	}
	if request.State != runtimerunlifecycle.StateCompleted {
		if _, err := s.delivery.TerminalizeRunDeliveriesTx(ctx, tx, story, effects, request.RunID, "run_"+string(request.State)); err != nil {
			return runtimerunlifecycle.Snapshot{}, "", err
		}
	}
	if err := s.decisionCards.SupersedeRunTx(
		ctx, tx, story, effects, request.RunID, "run_"+string(request.State), request.EndedAt.UTC(), request.IncludeCommittedDecisionCards,
	); err != nil {
		return runtimerunlifecycle.Snapshot{}, "", err
	}
	failureJSON, err := marshalRunLifecycleFailure(request.Failure)
	if err != nil {
		return runtimerunlifecycle.Snapshot{}, "", err
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE runs
		SET status = ?,
		    failure = ?,
		    ended_at = ?,
		    continued_as_run_id = NULLIF(?, ''),
		    completion_due_at = NULL
		WHERE run_id = ?
		  AND status IN (`+runLifecycleActiveStateSQLValues+`)
	`, string(request.State), failureJSON, request.EndedAt.UTC(), strings.TrimSpace(request.ContinuedAsRunID), request.RunID)
	if err != nil {
		return runtimerunlifecycle.Snapshot{}, "", fmt.Errorf("write SQLite terminal run lifecycle: %w", err)
	}
	if rows, rowsErr := result.RowsAffected(); rowsErr != nil {
		return runtimerunlifecycle.Snapshot{}, "", rowsErr
	} else if rows != 1 {
		return runtimerunlifecycle.Snapshot{}, "", errors.New("SQLite terminal run lifecycle lost locked transition")
	}
	snapshot, err := loadSQLiteRunLifecycleSnapshot(ctx, tx, request.RunID)
	if err != nil {
		return runtimerunlifecycle.Snapshot{}, "", err
	}
	if err := recordTerminalRunActivity(ctx, story, snapshot, request.Failure); err != nil {
		return runtimerunlifecycle.Snapshot{}, "", err
	}
	return snapshot, runtimerunlifecycle.MutationApplied, nil
}

func (s *RunLifecycleSQLiteOwner) completeRunTx(
	ctx context.Context,
	tx *sql.Tx,
	story runtimeauthoractivity.Mutation,
	effects *privaterunforkrevision.Effects,
	runID string,
	endedAt time.Time,
) (runtimerunlifecycle.Snapshot, runtimerunlifecycle.MutationDisposition, error) {
	mutation, err := terminalRunMutationForCompletion(runID, endedAt)
	if err != nil {
		return runtimerunlifecycle.Snapshot{}, "", err
	}
	return s.markRunTerminalStateTx(ctx, tx, story, effects, mutation)
}

func recordTerminalRunActivity(
	ctx context.Context,
	story runtimeauthoractivity.Mutation,
	snapshot runtimerunlifecycle.Snapshot,
	failure *runtimefailures.Envelope,
) error {
	scope, err := runtimeauthoractivity.BundleScopeForTarget(ctx, snapshot.BundleHash)
	if err != nil {
		return err
	}
	occurredAt := snapshot.StartedAt
	if snapshot.EndedAt != nil {
		occurredAt = snapshot.EndedAt.UTC()
	}
	draft := runtimeauthoractivity.Draft{
		Kind: runtimeauthoractivity.KindRunLifecycle, Transition: string(snapshot.State),
		SourceOwner: "runs", SourceIdentity: snapshot.RunID + ":" + string(snapshot.State),
		DedupKey:   "run-terminal:" + snapshot.RunID + ":" + string(snapshot.State),
		OccurredAt: occurredAt, RunID: snapshot.RunID, Scope: scope, Failure: failure,
		Projection: runtimeauthoractivity.Projection{SubjectType: "run", SubjectID: snapshot.RunID},
	}
	if story != nil {
		return story.Record(ctx, draft)
	}
	return fmt.Errorf("terminal run lifecycle activity requires private story ownership")
}

func marshalRunLifecycleFailure(failure *runtimefailures.Envelope) (any, error) {
	if failure == nil {
		return nil, nil
	}
	raw, err := runtimefailures.MarshalEnvelope(*failure)
	if err != nil {
		return nil, fmt.Errorf("encode terminal run lifecycle failure: %w", err)
	}
	return string(raw), nil
}

func sameRunLifecycleFailure(left, right *runtimefailures.Envelope) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	leftRaw, leftErr := runtimefailures.MarshalEnvelope(*left)
	rightRaw, rightErr := runtimefailures.MarshalEnvelope(*right)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftRaw, rightRaw)
}

type TerminalRunMutation = terminalRunMutation

func (s *RunLifecyclePostgresOwner) CompleteRunTx(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, effects *privaterunforkrevision.Effects, runID string, endedAt time.Time) (runtimerunlifecycle.Snapshot, runtimerunlifecycle.MutationDisposition, error) {
	return s.completeRunTx(ctx, tx, story, effects, runID, endedAt)
}

func (s *RunLifecycleSQLiteOwner) CompleteRunTx(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, effects *privaterunforkrevision.Effects, runID string, endedAt time.Time) (runtimerunlifecycle.Snapshot, runtimerunlifecycle.MutationDisposition, error) {
	return s.completeRunTx(ctx, tx, story, effects, runID, endedAt)
}

func (s *RunLifecyclePostgresOwner) MarkRunTerminalStateTx(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, effects *privaterunforkrevision.Effects, request TerminalRunMutation) (runtimerunlifecycle.Snapshot, runtimerunlifecycle.MutationDisposition, error) {
	return s.markRunTerminalStateTx(ctx, tx, story, effects, request)
}

func (s *RunLifecycleSQLiteOwner) MarkRunTerminalStateTx(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, effects *privaterunforkrevision.Effects, request TerminalRunMutation) (runtimerunlifecycle.Snapshot, runtimerunlifecycle.MutationDisposition, error) {
	return s.markRunTerminalStateTx(ctx, tx, story, effects, request)
}
