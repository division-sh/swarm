package pipelinepersistence

import (
	"context"
	"database/sql"
	"errors"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	privaterunforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
)

func (s *PipelinePostgresOwner) CommitSelectedForkEvent(ctx context.Context, request runtimebus.CommitSelectedForkEventRequest) (runtimebus.CommittedSelectedForkEvent, error) {
	if s == nil || s.selectedFork == nil {
		return runtimebus.CommittedSelectedForkEvent{}, errors.New("pipeline PostgreSQL selected-fork owner is required")
	}
	state, err := s.lockPostgresPipelineClaim(request.Commit.PipelineClaim)
	if err != nil {
		return runtimebus.CommittedSelectedForkEvent{}, err
	}
	defer state.operationMu.Unlock()
	effects := newRevisionEffects()
	var result runtimebus.CommittedSelectedForkEvent
	err = state.postgresLease.RunTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		story, err := privateauthoractivity.Begin(txctx, tx, privateauthoractivity.DialectPostgres)
		if err != nil {
			return err
		}
		result, err = s.selectedFork.CommitSelectedForkTx(txctx, tx, story, effects, request)
		if err != nil {
			return err
		}
		if _, err := privaterunforkrevision.FinalizePostgres(txctx, tx, effects); err != nil {
			return err
		}
		return story.Finalize(txctx)
	})
	return result, err
}

func (s *PipelineSQLiteOwner) CommitSelectedForkEvent(ctx context.Context, request runtimebus.CommitSelectedForkEventRequest) (runtimebus.CommittedSelectedForkEvent, error) {
	if s == nil || s.selectedFork == nil {
		return runtimebus.CommittedSelectedForkEvent{}, errors.New("pipeline SQLite selected-fork owner is required")
	}
	state, err := s.lockSQLitePipelineClaim(request.Commit.PipelineClaim)
	if err != nil {
		return runtimebus.CommittedSelectedForkEvent{}, err
	}
	defer state.operationMu.Unlock()
	effects := newRevisionEffects()
	var result runtimebus.CommittedSelectedForkEvent
	err = s.backend.RunTransaction(ctx, "sqlite selected-fork event commit", func(txctx context.Context, tx *sql.Tx) error {
		story, err := privateauthoractivity.Begin(txctx, tx, privateauthoractivity.DialectSQLite)
		if err != nil {
			return err
		}
		result, err = s.selectedFork.CommitSelectedForkTx(txctx, tx, story, effects, request)
		if err != nil {
			return err
		}
		if _, err := privaterunforkrevision.FinalizeSQLite(txctx, tx, effects); err != nil {
			return err
		}
		return story.Finalize(txctx)
	})
	return result, err
}
