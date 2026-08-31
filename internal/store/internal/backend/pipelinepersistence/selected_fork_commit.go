package pipelinepersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	privaterunforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
)

func (s *PipelinePostgresOwner) CommitSelectedForkEvent(ctx context.Context, request runtimebus.CommitSelectedForkEventRequest) (runtimebus.CommittedSelectedForkEvent, error) {
	if s == nil || s.selectedFork == nil {
		return runtimebus.CommittedSelectedForkEvent{}, errors.New("pipeline PostgreSQL selected-fork owner is required")
	}
	ctx, err := selectedForkCommitContext(ctx, request)
	if err != nil {
		return runtimebus.CommittedSelectedForkEvent{}, err
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
	ctx, err := selectedForkCommitContext(ctx, request)
	if err != nil {
		return runtimebus.CommittedSelectedForkEvent{}, err
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

func selectedForkCommitContext(ctx context.Context, request runtimebus.CommitSelectedForkEventRequest) (context.Context, error) {
	if request.HasAuthorScope {
		if request.AuthorScope.Kind != runtimeauthoractivity.ScopeBundle ||
			strings.TrimSpace(request.AuthorScope.RuntimeInstanceID) == "" ||
			strings.TrimSpace(request.AuthorScope.BundleHash) == "" {
			return ctx, fmt.Errorf("selected-fork author scope requires exact runtime and bundle identity")
		}
		ctx = runtimeauthoractivity.WithScope(ctx, request.AuthorScope)
	} else if request.AuthorScope.Kind != "" || strings.TrimSpace(request.AuthorScope.RuntimeInstanceID) != "" || strings.TrimSpace(request.AuthorScope.BundleHash) != "" {
		return ctx, fmt.Errorf("selected-fork author scope facts require explicit presence")
	}
	ctx = runtimeauthoractivity.WithoutResolvedEventDescriptor(ctx)
	if !request.HasAuthorDescriptor {
		return ctx, nil
	}
	if !request.HasAuthorScope {
		return ctx, fmt.Errorf("selected-fork author descriptor requires exact author scope")
	}
	eventType := strings.TrimSpace(string(request.Commit.Event.Event().Type()))
	if strings.TrimSpace(request.AuthorDescriptor.EventType) != eventType {
		return ctx, fmt.Errorf("selected-fork author descriptor does not match event type")
	}
	if strings.TrimSpace(string(request.AuthorDescriptor.Disposition)) == "" {
		return ctx, fmt.Errorf("selected-fork author descriptor requires disposition")
	}
	return runtimeauthoractivity.WithResolvedEventDescriptor(ctx, request.AuthorScope, request.AuthorDescriptor)
}
