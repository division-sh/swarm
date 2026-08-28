package runlifecycle

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
	runtimefanoutbarrier "github.com/division-sh/swarm/internal/runtime/fanoutbarrier"
	runtimefanout "github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	runtimesessions "github.com/division-sh/swarm/internal/runtime/sessions"
	runtimetimers "github.com/division-sh/swarm/internal/runtime/timerobligation"
	decisionstore "github.com/division-sh/swarm/internal/store/internal/backend/decisioncard"
	deliverystore "github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	effectstore "github.com/division-sh/swarm/internal/store/internal/backend/effects"
	entitystore "github.com/division-sh/swarm/internal/store/internal/backend/entityruntime"
	sessionstore "github.com/division-sh/swarm/internal/store/internal/backend/sessions"
	timerstore "github.com/division-sh/swarm/internal/store/internal/backend/timerobligation"
)

var (
	postgresDeliveryAdapter = mustDeliveryAdapter(deliverystore.DialectPostgres)
	sqliteDeliveryAdapter   = mustDeliveryAdapter(deliverystore.DialectSQLite)
)

func mustDeliveryAdapter(dialect deliverystore.Dialect) *deliverystore.Adapter {
	adapter, err := deliverystore.NewAdapter(dialect)
	if err != nil {
		panic(err)
	}
	return adapter
}

type runCompletionOwnerSummaries struct {
	Delivery  runtimedelivery.RunSummary
	Pipeline  runtimepipelineobligation.RunSummary
	FanOut    runtimefanout.RunSummary
	Barriers  runtimefanoutbarrier.RunSummary
	Timers    runtimetimers.RunSummary
	Sessions  runtimesessions.RunSummary
	Decisions runtimedecision.RunSummary
	Effects   runtimeeffects.RunSummary
	Entities  runtimeentity.RunSummary
}

type RunCompletionOwnerSummaries = runCompletionOwnerSummaries

func (s runCompletionOwnerSummaries) validate() error {
	if err := s.Delivery.Validate(); err != nil {
		return err
	}
	if err := s.Pipeline.Validate(); err != nil {
		return err
	}
	if err := s.FanOut.Validate(); err != nil {
		return err
	}
	if err := s.Barriers.Validate(); err != nil {
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
		"fan_out":  s.FanOut.RunID,
		"barrier":  s.Barriers.RunID,
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
		s.FanOut.BlocksCompletion() ||
		s.Barriers.BlocksCompletion() ||
		s.Timers.BlocksCompletion() ||
		s.Sessions.BlocksCompletion() ||
		s.Decisions.BlocksCompletion() ||
		s.Effects.BlocksCompletion() ||
		!s.Entities.ReadyForCompletion()
}

func (s *RunLifecyclePostgresOwner) loadPostgresRunCompletionOwnerSummaries(
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
	pipeline, err := s.pipeline.SummarizeRunTx(ctx, tx, runID)
	if err != nil {
		return runCompletionOwnerSummaries{}, fmt.Errorf("summarize pipeline obligations: %w", err)
	}
	fanOut, err := s.pipeline.SummarizeFanOutRunTx(ctx, tx, runID, selectedNow)
	if err != nil {
		return runCompletionOwnerSummaries{}, fmt.Errorf("summarize fan-out obligations: %w", err)
	}
	barriers, err := s.pipeline.SummarizeFanOutDeliveryBarriersRunTx(ctx, tx, runID)
	if err != nil {
		return runCompletionOwnerSummaries{}, fmt.Errorf("summarize fan-out delivery barriers: %w", err)
	}
	timers, err := summarizeTimerRun(ctx, tx, runID, selectedNow, true)
	if err != nil {
		return runCompletionOwnerSummaries{}, err
	}
	sessions, err := sessionstore.ReadRunSummary(ctx, tx, sessionstore.SummaryDialectPostgres, runID, selectedNow)
	if err != nil {
		return runCompletionOwnerSummaries{}, err
	}
	decisions, err := decisionstore.ReadRunSummary(ctx, tx, decisionstore.SummaryDialectPostgres, runID)
	if err != nil {
		return runCompletionOwnerSummaries{}, err
	}
	effects, err := effectstore.ReadRunSummary(ctx, tx, effectstore.SummaryDialectPostgres, runID)
	if err != nil {
		return runCompletionOwnerSummaries{}, err
	}
	entities, err := entitystore.ReadRunSummary(ctx, tx, entitystore.SummaryDialectPostgres, runID, catalog)
	if err != nil {
		return runCompletionOwnerSummaries{}, err
	}
	out := runCompletionOwnerSummaries{
		Delivery: delivery, Pipeline: pipeline, FanOut: fanOut, Barriers: barriers, Timers: timers, Sessions: sessions,
		Decisions: decisions, Effects: effects, Entities: entities,
	}
	return out, out.validate()
}

func (s *RunLifecycleSQLiteOwner) loadSQLiteRunCompletionOwnerSummaries(
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
	pipeline, err := s.pipeline.SummarizeRunTx(ctx, tx, runID)
	if err != nil {
		return runCompletionOwnerSummaries{}, fmt.Errorf("summarize sqlite pipeline obligations: %w", err)
	}
	fanOut, err := s.pipeline.SummarizeFanOutRunTx(ctx, tx, runID, selectedNow)
	if err != nil {
		return runCompletionOwnerSummaries{}, fmt.Errorf("summarize sqlite fan-out obligations: %w", err)
	}
	barriers, err := s.pipeline.SummarizeFanOutDeliveryBarriersRunTx(ctx, tx, runID)
	if err != nil {
		return runCompletionOwnerSummaries{}, fmt.Errorf("summarize sqlite fan-out delivery barriers: %w", err)
	}
	timers, err := summarizeTimerRun(ctx, tx, runID, selectedNow, false)
	if err != nil {
		return runCompletionOwnerSummaries{}, err
	}
	sessions, err := sessionstore.ReadRunSummary(ctx, tx, sessionstore.SummaryDialectSQLite, runID, selectedNow)
	if err != nil {
		return runCompletionOwnerSummaries{}, err
	}
	decisions, err := decisionstore.ReadRunSummary(ctx, tx, decisionstore.SummaryDialectSQLite, runID)
	if err != nil {
		return runCompletionOwnerSummaries{}, err
	}
	effects, err := effectstore.ReadRunSummary(ctx, tx, effectstore.SummaryDialectSQLite, runID)
	if err != nil {
		return runCompletionOwnerSummaries{}, err
	}
	entities, err := entitystore.ReadRunSummary(ctx, tx, entitystore.SummaryDialectSQLite, runID, catalog)
	if err != nil {
		return runCompletionOwnerSummaries{}, err
	}
	out := runCompletionOwnerSummaries{
		Delivery: delivery, Pipeline: pipeline, FanOut: fanOut, Barriers: barriers, Timers: timers, Sessions: sessions,
		Decisions: decisions, Effects: effects, Entities: entities,
	}
	return out, out.validate()
}

func summarizeTimerRun(ctx context.Context, tx *sql.Tx, runID string, selectedNow time.Time, postgres bool) (runtimetimers.RunSummary, error) {
	scope, err := runtimetimers.Run(runID)
	if err != nil {
		return runtimetimers.RunSummary{}, err
	}
	var snapshot runtimetimers.Snapshot
	if postgres {
		snapshot, err = timerstore.ReadPostgresTx(ctx, tx, scope, selectedNow.UTC())
	} else {
		snapshot, err = timerstore.ReadSQLiteTx(ctx, tx, scope, selectedNow.UTC())
	}
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
	summary, err := sessionstore.ReadRunSummary(ctx, tx, sessionstore.SummaryDialectPostgres, runID, selectedNow)
	if err != nil {
		return nil, err
	}
	return optionalWake(summary.NextExpiry), nil
}

func PostgresRunSessionNextWakeTx(ctx context.Context, tx *sql.Tx, runID string, selectedNow time.Time) (*time.Time, error) {
	return postgresRunSessionNextWakeTx(ctx, tx, runID, selectedNow)
}

func (s *RunLifecyclePostgresOwner) LoadRunCompletionOwnerSummariesTx(ctx context.Context, tx *sql.Tx, runID string, selectedNow time.Time, catalog runtimerunlifecycle.TerminalCatalog) (RunCompletionOwnerSummaries, error) {
	return s.loadPostgresRunCompletionOwnerSummaries(ctx, tx, runID, selectedNow, catalog)
}

func (s *RunLifecycleSQLiteOwner) LoadRunCompletionOwnerSummariesTx(ctx context.Context, tx *sql.Tx, runID string, selectedNow time.Time, catalog runtimerunlifecycle.TerminalCatalog) (RunCompletionOwnerSummaries, error) {
	return s.loadSQLiteRunCompletionOwnerSummaries(ctx, tx, runID, selectedNow, catalog)
}
