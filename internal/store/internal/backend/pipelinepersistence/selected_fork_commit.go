package pipelinepersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
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
	outcome := mutationprotocol.RunRetainedPostgresWithOptions(ctx, state.postgresLease.Session(), &sql.TxOptions{Isolation: sql.LevelSerializable}, mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, func(txctx context.Context, attempt *mutationprotocol.Attempt) (runtimebus.CommittedSelectedForkEvent, error) {
		return s.selectedFork.CommitSelectedForkTx(txctx, attempt, request)
	})
	result, acknowledged := outcome.Value()
	return finishSelectedForkPostgresCommit(result, acknowledged, outcome.Err())
}

func finishSelectedForkPostgresCommit(result runtimebus.CommittedSelectedForkEvent, acknowledged bool, err error) (runtimebus.CommittedSelectedForkEvent, error) {
	if !acknowledged {
		if postgresbackend.IsRolledBackSerializationConflict(err) {
			err = runtimefailures.MarkRunForkPublicationConflict(err)
		}
		return runtimebus.CommittedSelectedForkEvent{}, err
	}
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
	outcome := mutationprotocol.RunSQLite(ctx, s.backend, "sqlite selected-fork event commit", mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, func(txctx context.Context, attempt *mutationprotocol.Attempt) (runtimebus.CommittedSelectedForkEvent, error) {
		return s.selectedFork.CommitSelectedForkTx(txctx, attempt, request)
	})
	result, acknowledged := outcome.Value()
	if !acknowledged {
		return runtimebus.CommittedSelectedForkEvent{}, outcome.Err()
	}
	return result, outcome.Err()
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
