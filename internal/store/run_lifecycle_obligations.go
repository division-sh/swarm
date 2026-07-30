package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	runtimedecision "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimeentity "github.com/division-sh/swarm/internal/runtime/entityruntime"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	runtimesessions "github.com/division-sh/swarm/internal/runtime/sessions"
	runtimetimers "github.com/division-sh/swarm/internal/runtime/timerobligation"
)

type runCompletionOwnerSummaries struct {
	Delivery  runtimedelivery.RunSummary
	Pipeline  runtimepipelineobligation.RunSummary
	Timers    runtimetimers.RunSummary
	Sessions  runtimesessions.RunSummary
	Decisions runtimedecision.RunSummary
	Effects   runtimeeffects.RunSummary
	Entities  runtimeentity.RunSummary
}

func (s runCompletionOwnerSummaries) validate() error {
	if err := s.Delivery.Validate(); err != nil {
		return err
	}
	if err := s.Pipeline.Validate(); err != nil {
		return err
	}
	if err := s.Timers.Validate(); err != nil {
		return err
	}
	if err := s.Sessions.Validate(); err != nil {
		return err
	}
	if err := s.Decisions.Validate(); err != nil {
		return err
	}
	if err := s.Effects.Validate(); err != nil {
		return err
	}
	if err := s.Entities.Validate(); err != nil {
		return err
	}
	runID := strings.TrimSpace(s.Delivery.RunID)
	for owner, candidate := range map[string]string{
		"pipeline": s.Pipeline.RunID,
		"timer":    s.Timers.RunID,
		"session":  s.Sessions.RunID,
		"decision": s.Decisions.RunID,
		"effect":   s.Effects.RunID,
		"entity":   s.Entities.RunID,
	} {
		if strings.TrimSpace(candidate) != runID {
			return fmt.Errorf("%s run summary identity does not match delivery run %s", owner, runID)
		}
	}
	return nil
}

func (s runCompletionOwnerSummaries) blocksCompletion() bool {
	return !s.Delivery.Settled() ||
		s.Pipeline.BlocksCompletion() ||
		s.Timers.BlocksCompletion() ||
		s.Sessions.BlocksCompletion() ||
		s.Decisions.BlocksCompletion() ||
		s.Effects.BlocksCompletion() ||
		!s.Entities.ReadyForCompletion()
}

func loadPostgresRunCompletionOwnerSummaries(
	ctx context.Context,
	tx *sql.Tx,
	runID string,
	selectedNow time.Time,
	catalog runtimerunlifecycle.TerminalCatalog,
) (runCompletionOwnerSummaries, error) {
	delivery, err := postgresDeliveryAdapter.SummarizeRun(ctx, tx, runID)
	if err != nil {
		return runCompletionOwnerSummaries{}, fmt.Errorf("summarize delivery obligations: %w", err)
	}
	pipeline, err := summarizePipelineRun(ctx, tx, runID, true)
	if err != nil {
		return runCompletionOwnerSummaries{}, fmt.Errorf("summarize pipeline obligations: %w", err)
	}
	timers, err := summarizeTimerRun(ctx, tx, runID, selectedNow, true)
	if err != nil {
		return runCompletionOwnerSummaries{}, err
	}
	sessions, err := runtimesessions.ReadRunSummary(ctx, tx, runtimesessions.SummaryDialectPostgres, runID, selectedNow)
	if err != nil {
		return runCompletionOwnerSummaries{}, err
	}
	decisions, err := runtimedecision.ReadRunSummary(ctx, tx, runtimedecision.SummaryDialectPostgres, runID)
	if err != nil {
		return runCompletionOwnerSummaries{}, err
	}
	effects, err := runtimeeffects.ReadRunSummary(ctx, tx, runtimeeffects.SummaryDialectPostgres, runID)
	if err != nil {
		return runCompletionOwnerSummaries{}, err
	}
	entities, err := runtimeentity.ReadRunSummary(ctx, tx, runtimeentity.SummaryDialectPostgres, runID, catalog)
	if err != nil {
		return runCompletionOwnerSummaries{}, err
	}
	out := runCompletionOwnerSummaries{
		Delivery: delivery, Pipeline: pipeline, Timers: timers, Sessions: sessions,
		Decisions: decisions, Effects: effects, Entities: entities,
	}
	return out, out.validate()
}

func loadSQLiteRunCompletionOwnerSummaries(
	ctx context.Context,
	tx *sql.Tx,
	runID string,
	selectedNow time.Time,
	catalog runtimerunlifecycle.TerminalCatalog,
) (runCompletionOwnerSummaries, error) {
	delivery, err := sqliteDeliveryAdapter.SummarizeRun(ctx, tx, runID)
	if err != nil {
		return runCompletionOwnerSummaries{}, fmt.Errorf("summarize sqlite delivery obligations: %w", err)
	}
	pipeline, err := summarizePipelineRun(ctx, tx, runID, false)
	if err != nil {
		return runCompletionOwnerSummaries{}, fmt.Errorf("summarize sqlite pipeline obligations: %w", err)
	}
	timers, err := summarizeTimerRun(ctx, tx, runID, selectedNow, false)
	if err != nil {
		return runCompletionOwnerSummaries{}, err
	}
	sessions, err := runtimesessions.ReadRunSummary(ctx, tx, runtimesessions.SummaryDialectSQLite, runID, selectedNow)
	if err != nil {
		return runCompletionOwnerSummaries{}, err
	}
	decisions, err := runtimedecision.ReadRunSummary(ctx, tx, runtimedecision.SummaryDialectSQLite, runID)
	if err != nil {
		return runCompletionOwnerSummaries{}, err
	}
	effects, err := runtimeeffects.ReadRunSummary(ctx, tx, runtimeeffects.SummaryDialectSQLite, runID)
	if err != nil {
		return runCompletionOwnerSummaries{}, err
	}
	entities, err := runtimeentity.ReadRunSummary(ctx, tx, runtimeentity.SummaryDialectSQLite, runID, catalog)
	if err != nil {
		return runCompletionOwnerSummaries{}, err
	}
	out := runCompletionOwnerSummaries{
		Delivery: delivery, Pipeline: pipeline, Timers: timers, Sessions: sessions,
		Decisions: decisions, Effects: effects, Entities: entities,
	}
	return out, out.validate()
}

func summarizeTimerRun(ctx context.Context, tx *sql.Tx, runID string, selectedNow time.Time, postgres bool) (runtimetimers.RunSummary, error) {
	scope, err := runtimetimers.Run(runID)
	if err != nil {
		return runtimetimers.RunSummary{}, err
	}
	dialect := runtimetimers.DialectSQLite
	if postgres {
		dialect = runtimetimers.DialectPostgres
	}
	snapshot, err := runtimetimers.Read(ctx, tx, dialect, scope, selectedNow.UTC())
	if err != nil {
		return runtimetimers.RunSummary{}, err
	}
	obligations, ok := snapshot.Run(runID)
	if !ok {
		return runtimetimers.RunSummary{}, fmt.Errorf("timer owner omitted requested run %s", runID)
	}
	summary := obligations.Summary(snapshot.ObservedAt)
	return summary, summary.Validate()
}

func postgresRunSessionNextWakeTx(ctx context.Context, tx *sql.Tx, runID string, selectedNow time.Time) (*time.Time, error) {
	summary, err := runtimesessions.ReadRunSummary(ctx, tx, runtimesessions.SummaryDialectPostgres, runID, selectedNow)
	if err != nil {
		return nil, err
	}
	return optionalWake(summary.NextExpiry), nil
}
