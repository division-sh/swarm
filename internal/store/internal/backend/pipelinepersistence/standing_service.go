package pipelinepersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimemutationlog "github.com/division-sh/swarm/internal/runtime/mutationlog"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	runtimetimercancellation "github.com/division-sh/swarm/internal/runtime/timercancellation"
	runtimetimerobligation "github.com/division-sh/swarm/internal/runtime/timerobligation"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	storeentity "github.com/division-sh/swarm/internal/store/internal/backend/entityruntime"
	storegenericschedule "github.com/division-sh/swarm/internal/store/internal/backend/genericschedule"
	privatemutationlog "github.com/division-sh/swarm/internal/store/internal/backend/mutationlog"
	privaterunforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	timerobligationstore "github.com/division-sh/swarm/internal/store/internal/backend/timerobligation"
	storeworkflowtimer "github.com/division-sh/swarm/internal/store/internal/backend/workflowtimer"
	"github.com/google/uuid"
)

const standingRestartAbandonReason = "server_restart_abandon"

type standingServiceAdapter struct {
	db                           eventReadQueryer
	postgres                     bool
	run                          func(context.Context, func(context.Context, *sql.Tx) error) error
	postgresStore                *PipelinePostgresOwner
	sqliteStore                  *PipelineSQLiteOwner
	story                        runtimeauthoractivity.Mutation
	handoff                      *runLifecycleCandidateHandoffReservation
	revisionEffects              *revisionEffects
	deliveryContinuationRequired bool
}

func newPostgresStandingServiceAdapter(store *PipelinePostgresOwner) *standingServiceAdapter {
	if store == nil || store.backend == nil {
		return nil
	}
	adapter := &standingServiceAdapter{
		db:            store.backend,
		postgres:      true,
		postgresStore: store,
	}
	adapter.run = func(ctx context.Context, fn func(context.Context, *sql.Tx) error) error {
		return withRunLifecycleCandidateHandoff(ctx, func(handoff *runLifecycleCandidateHandoffReservation) error {
			effects := newRevisionEffects()
			return store.runPrivateAuthorActivityMutation(ctx, effects, func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
				adapter.story = story
				adapter.handoff = handoff
				adapter.revisionEffects = effects
				defer func() {
					adapter.story = nil
					adapter.handoff = nil
					adapter.revisionEffects = nil
				}()
				return fn(txctx, tx)
			})
		})
	}
	return adapter
}

func newSQLiteStandingServiceAdapter(store *PipelineSQLiteOwner) *standingServiceAdapter {
	if store == nil || store.backend == nil {
		return nil
	}
	adapter := &standingServiceAdapter{
		db:          store.backend,
		sqliteStore: store,
	}
	adapter.run = func(ctx context.Context, fn func(context.Context, *sql.Tx) error) error {
		return withRunLifecycleCandidateHandoff(ctx, func(handoff *runLifecycleCandidateHandoffReservation) error {
			effects := newRevisionEffects()
			return store.runPrivateAuthorActivityMutation(ctx, "sqlite standing service mutation", effects, func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
				adapter.story = story
				adapter.handoff = handoff
				adapter.revisionEffects = effects
				defer func() {
					adapter.story = nil
					adapter.handoff = nil
					adapter.revisionEffects = nil
				}()
				return fn(txctx, tx)
			})
		})
	}
	return adapter
}

func (s *standingServiceAdapter) isSQLite() bool { return s != nil && !s.postgres }

func (s *standingServiceAdapter) runInPipelineTransaction(ctx context.Context, fn func(context.Context, *sql.Tx) error) error {
	if s == nil || s.run == nil {
		return errors.New("standing service transaction owner is required")
	}
	return s.run(ctx, fn)
}

func (s *standingServiceAdapter) queueDeliveryContinuationSignal(context.Context) error {
	if s == nil {
		return errors.New("standing service transaction owner is required")
	}
	s.deliveryContinuationRequired = true
	return nil
}

func (s *standingServiceAdapter) validRunLifecycleMutation(tx *sql.Tx) bool {
	return s != nil && tx != nil && s.story != nil && (s.postgresStore != nil || s.sqliteStore != nil)
}

func (s *standingServiceAdapter) requireActiveRunSource(ctx context.Context, tx *sql.Tx, runID string) (runtimecorrelation.BundleSourceFact, error) {
	if !s.validRunLifecycleMutation(tx) {
		return runtimecorrelation.BundleSourceFact{}, errors.New("standing run lifecycle mutation owner is required")
	}
	if s.postgresStore != nil {
		return s.postgresStore.RunLifecyclePostgresOwner.RequireActiveSourceTx(ctx, tx, runID)
	}
	return s.sqliteStore.RunLifecycleSQLiteOwner.RequireActiveSourceTx(ctx, tx, runID)
}

func (s *standingServiceAdapter) requirePresentRunSource(ctx context.Context, tx *sql.Tx, runID string) (runtimecorrelation.BundleSourceFact, error) {
	if !s.validRunLifecycleMutation(tx) {
		return runtimecorrelation.BundleSourceFact{}, errors.New("standing run lifecycle mutation owner is required")
	}
	if s.postgresStore != nil {
		return s.postgresStore.RunLifecyclePostgresOwner.RequirePresentSourceTx(ctx, tx, runID)
	}
	return s.sqliteStore.RunLifecycleSQLiteOwner.RequirePresentSourceTx(ctx, tx, runID)
}

func (s *standingServiceAdapter) createRun(ctx context.Context, tx *sql.Tx, request runtimerunlifecycle.CreateRequest) (runtimerunlifecycle.MutationDisposition, error) {
	if !s.validRunLifecycleMutation(tx) {
		return "", errors.New("standing run lifecycle mutation owner is required")
	}
	if s.postgresStore != nil {
		return s.postgresStore.RunLifecyclePostgresOwner.CreateRunTx(ctx, tx, s.story, request)
	}
	return s.sqliteStore.RunLifecycleSQLiteOwner.CreateRunTx(ctx, tx, s.story, request)
}

func (s *standingServiceAdapter) reviseRunSource(ctx context.Context, tx *sql.Tx, request runtimerunlifecycle.SourceRevisionRequest) (runtimerunlifecycle.MutationDisposition, error) {
	if !s.validRunLifecycleMutation(tx) {
		return "", errors.New("standing run lifecycle mutation owner is required")
	}
	if s.postgresStore != nil {
		return s.postgresStore.RunLifecyclePostgresOwner.ReviseSourceTx(ctx, tx, s.story, s.handoff, request)
	}
	return s.sqliteStore.RunLifecycleSQLiteOwner.ReviseSourceTx(ctx, tx, s.story, s.handoff, request)
}

func (s *standingServiceAdapter) transitionActiveRun(ctx context.Context, tx *sql.Tx, request runtimerunlifecycle.ActiveTransitionRequest) (runtimerunlifecycle.MutationDisposition, error) {
	if !s.validRunLifecycleMutation(tx) {
		return "", errors.New("standing run lifecycle mutation owner is required")
	}
	if s.postgresStore != nil {
		return s.postgresStore.RunLifecyclePostgresOwner.TransitionActiveTx(ctx, tx, s.story, s.handoff, request)
	}
	return s.sqliteStore.RunLifecycleSQLiteOwner.TransitionActiveTx(ctx, tx, s.story, s.handoff, request)
}

func (s *standingServiceAdapter) markTerminalRun(ctx context.Context, tx *sql.Tx, request runtimerunlifecycle.TerminalRequest) (runtimerunlifecycle.Snapshot, runtimerunlifecycle.MutationDisposition, error) {
	if !s.validRunLifecycleMutation(tx) {
		return runtimerunlifecycle.Snapshot{}, "", errors.New("standing run lifecycle mutation owner is required")
	}
	if s.postgresStore != nil {
		return s.postgresStore.RunLifecyclePostgresOwner.MarkTerminalTx(ctx, tx, s.story, s.revisionEffects, request)
	}
	return s.sqliteStore.RunLifecycleSQLiteOwner.MarkTerminalTx(ctx, tx, s.story, s.revisionEffects, request)
}

func standingServiceResultEvidence(result runtimepipeline.StandingServiceReconciliation, adapter *standingServiceAdapter, err error) (runtimepipeline.StandingServiceReconciliation, error) {
	if err == nil && adapter != nil {
		result.DeliveryContinuationRequired = adapter.deliveryContinuationRequired
	}
	return result, err
}

func standingServiceResultsEvidence(results []runtimepipeline.StandingServiceReconciliation, adapter *standingServiceAdapter, err error) ([]runtimepipeline.StandingServiceReconciliation, error) {
	if err == nil && adapter != nil && adapter.deliveryContinuationRequired && len(results) > 0 {
		results[0].DeliveryContinuationRequired = true
	}
	return results, err
}

func (s *PipelinePostgresOwner) ReconcileStandingService(ctx context.Context, candidate runtimepipeline.StandingServiceCandidate) (runtimepipeline.StandingServiceReconciliation, error) {
	adapter := newPostgresStandingServiceAdapter(s)
	result, err := adapter.ReconcileStandingService(ctx, candidate)
	return standingServiceResultEvidence(result, adapter, err)
}

func (s *PipelineSQLiteOwner) ReconcileStandingService(ctx context.Context, candidate runtimepipeline.StandingServiceCandidate) (runtimepipeline.StandingServiceReconciliation, error) {
	adapter := newSQLiteStandingServiceAdapter(s)
	result, err := adapter.ReconcileStandingService(ctx, candidate)
	return standingServiceResultEvidence(result, adapter, err)
}

func (s *PipelinePostgresOwner) LoadReconciledStandingService(ctx context.Context, candidate runtimepipeline.StandingServiceCandidate) (runtimepipeline.StandingServiceReconciliation, bool, error) {
	return newPostgresStandingServiceAdapter(s).LoadReconciledStandingService(ctx, candidate)
}

func (s *PipelineSQLiteOwner) LoadReconciledStandingService(ctx context.Context, candidate runtimepipeline.StandingServiceCandidate) (runtimepipeline.StandingServiceReconciliation, bool, error) {
	return newSQLiteStandingServiceAdapter(s).LoadReconciledStandingService(ctx, candidate)
}

func (s *PipelinePostgresOwner) ReconcileStandingServiceSet(ctx context.Context, candidates []runtimepipeline.StandingServiceCandidate) ([]runtimepipeline.StandingServiceReconciliation, error) {
	adapter := newPostgresStandingServiceAdapter(s)
	results, err := adapter.ReconcileStandingServiceSet(ctx, candidates)
	return standingServiceResultsEvidence(results, adapter, err)
}

func (s *PipelineSQLiteOwner) ReconcileStandingServiceSet(ctx context.Context, candidates []runtimepipeline.StandingServiceCandidate) ([]runtimepipeline.StandingServiceReconciliation, error) {
	adapter := newSQLiteStandingServiceAdapter(s)
	results, err := adapter.ReconcileStandingServiceSet(ctx, candidates)
	return standingServiceResultsEvidence(results, adapter, err)
}

func (s *PipelinePostgresOwner) ReconcileStandingServiceReplacement(ctx context.Context, previous, candidates []runtimepipeline.StandingServiceCandidate) ([]runtimepipeline.StandingServiceReconciliation, error) {
	adapter := newPostgresStandingServiceAdapter(s)
	results, err := adapter.ReconcileStandingServiceReplacement(ctx, previous, candidates)
	return standingServiceResultsEvidence(results, adapter, err)
}

func (s *PipelineSQLiteOwner) ReconcileStandingServiceReplacement(ctx context.Context, previous, candidates []runtimepipeline.StandingServiceCandidate) ([]runtimepipeline.StandingServiceReconciliation, error) {
	adapter := newSQLiteStandingServiceAdapter(s)
	results, err := adapter.ReconcileStandingServiceReplacement(ctx, previous, candidates)
	return standingServiceResultsEvidence(results, adapter, err)
}

func (s *PipelinePostgresOwner) SuspendStandingService(ctx context.Context, operation runtimepipeline.StandingServiceOperation) (runtimepipeline.StandingServiceReconciliation, error) {
	adapter := newPostgresStandingServiceAdapter(s)
	result, err := adapter.SuspendStandingService(ctx, operation)
	return standingServiceResultEvidence(result, adapter, err)
}

func (s *PipelineSQLiteOwner) SuspendStandingService(ctx context.Context, operation runtimepipeline.StandingServiceOperation) (runtimepipeline.StandingServiceReconciliation, error) {
	adapter := newSQLiteStandingServiceAdapter(s)
	result, err := adapter.SuspendStandingService(ctx, operation)
	return standingServiceResultEvidence(result, adapter, err)
}

func (s *PipelinePostgresOwner) ResumeStandingService(ctx context.Context, operation runtimepipeline.StandingServiceOperation) (runtimepipeline.StandingServiceReconciliation, error) {
	adapter := newPostgresStandingServiceAdapter(s)
	result, err := adapter.ResumeStandingService(ctx, operation)
	return standingServiceResultEvidence(result, adapter, err)
}

func (s *PipelineSQLiteOwner) ResumeStandingService(ctx context.Context, operation runtimepipeline.StandingServiceOperation) (runtimepipeline.StandingServiceReconciliation, error) {
	adapter := newSQLiteStandingServiceAdapter(s)
	result, err := adapter.ResumeStandingService(ctx, operation)
	return standingServiceResultEvidence(result, adapter, err)
}

func (s *PipelinePostgresOwner) ResetStandingService(ctx context.Context, operation runtimepipeline.StandingServiceOperation) (runtimepipeline.StandingServiceReconciliation, error) {
	adapter := newPostgresStandingServiceAdapter(s)
	result, err := adapter.ResetStandingService(ctx, operation)
	return standingServiceResultEvidence(result, adapter, err)
}

func (s *PipelineSQLiteOwner) ResetStandingService(ctx context.Context, operation runtimepipeline.StandingServiceOperation) (runtimepipeline.StandingServiceReconciliation, error) {
	adapter := newSQLiteStandingServiceAdapter(s)
	result, err := adapter.ResetStandingService(ctx, operation)
	return standingServiceResultEvidence(result, adapter, err)
}

func (s *PipelinePostgresOwner) AdmitStandingServiceRun(ctx context.Context, runID string, posture executionposture.Posture) error {
	return newPostgresStandingServiceAdapter(s).AdmitStandingServiceRun(ctx, runID, posture)
}

func (s *PipelineSQLiteOwner) AdmitStandingServiceRun(ctx context.Context, runID string, posture executionposture.Posture) error {
	return newSQLiteStandingServiceAdapter(s).AdmitStandingServiceRun(ctx, runID, posture)
}

func (s *PipelinePostgresOwner) PublishStandingService(ctx context.Context, serviceID, runID string, generation int64) (int64, error) {
	return newPostgresStandingServiceAdapter(s).PublishStandingService(ctx, serviceID, runID, generation)
}

func (s *PipelineSQLiteOwner) PublishStandingService(ctx context.Context, serviceID, runID string, generation int64) (int64, error) {
	return newSQLiteStandingServiceAdapter(s).PublishStandingService(ctx, serviceID, runID, generation)
}

func (s *PipelinePostgresOwner) StandingRunUsesIntrinsicRecovery(ctx context.Context, runID string) (bool, error) {
	return newPostgresStandingServiceAdapter(s).StandingRunUsesIntrinsicRecovery(ctx, runID)
}

func (s *PipelineSQLiteOwner) StandingRunUsesIntrinsicRecovery(ctx context.Context, runID string) (bool, error) {
	return newSQLiteStandingServiceAdapter(s).StandingRunUsesIntrinsicRecovery(ctx, runID)
}

func (s *PipelinePostgresOwner) ListStandingServiceStatuses(ctx context.Context) ([]runtimepipeline.StandingServiceStatus, error) {
	return newPostgresStandingServiceAdapter(s).ListStandingServiceStatuses(ctx)
}

func (s *PipelineSQLiteOwner) ListStandingServiceStatuses(ctx context.Context) ([]runtimepipeline.StandingServiceStatus, error) {
	return newSQLiteStandingServiceAdapter(s).ListStandingServiceStatuses(ctx)
}

type standingServiceRow struct {
	runtimepipeline.StandingServiceReconciliation
	DeclarationPresent bool
	OperatorOverride   string
	RevisionSequence   int64
	PublicationState   string
	RunStatus          string
	RunControlReason   string
}

func (s *standingServiceAdapter) requireStandingRunSourceTx(
	ctx context.Context,
	tx *sql.Tx,
	current standingServiceRow,
	requireActive bool,
) (runtimecorrelation.BundleSourceFact, error) {
	var (
		fact runtimecorrelation.BundleSourceFact
		err  error
	)
	if s == nil || !s.validRunLifecycleMutation(tx) {
		return runtimecorrelation.BundleSourceFact{}, errors.New("standing run lifecycle owner is required")
	}
	if requireActive {
		fact, err = s.requireActiveRunSource(ctx, tx, current.RunID)
	} else {
		fact, err = s.requirePresentRunSource(ctx, tx, current.RunID)
	}
	if err != nil {
		return runtimecorrelation.BundleSourceFact{}, err
	}
	bundleHash, bundleSource := fact.StorageValues()
	if current.BundleHash != bundleHash || current.BundleSource != bundleSource {
		return runtimecorrelation.BundleSourceFact{}, fmt.Errorf(
			"standing service %s provenance conflicts with run %s: standing bundle_hash=%s bundle_source=%s, run bundle_hash=%s bundle_source=%s",
			current.ServiceID, current.RunID, current.BundleHash, current.BundleSource, bundleHash, bundleSource,
		)
	}
	return fact, nil
}

func (s *standingServiceAdapter) ReconcileStandingService(ctx context.Context, candidate runtimepipeline.StandingServiceCandidate) (runtimepipeline.StandingServiceReconciliation, error) {
	if s == nil || s.db == nil {
		return runtimepipeline.StandingServiceReconciliation{}, fmt.Errorf("workflow instance store is required")
	}
	candidate = candidate.Normalized()
	if err := candidate.Validate(); err != nil {
		return runtimepipeline.StandingServiceReconciliation{}, err
	}
	var result runtimepipeline.StandingServiceReconciliation
	err := s.runInPipelineTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		result, err = s.reconcileStandingServiceTx(txctx, tx, candidate)
		return err
	})
	return result, err
}

func (s *standingServiceAdapter) LoadReconciledStandingService(ctx context.Context, candidate runtimepipeline.StandingServiceCandidate) (runtimepipeline.StandingServiceReconciliation, bool, error) {
	candidate = candidate.Normalized()
	if err := candidate.Validate(); err != nil {
		return runtimepipeline.StandingServiceReconciliation{}, false, err
	}
	var result runtimepipeline.StandingServiceReconciliation
	var found bool
	err := s.runInPipelineTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		current, exists, err := s.loadStandingServiceTx(txctx, tx, candidate.ServiceID)
		if err != nil || !exists {
			return err
		}
		if current.PackageKey != candidate.PackageKey || current.FlowID != candidate.FlowID || current.InstanceID != candidate.InstanceID || current.EntityID != candidate.EntityID {
			return fmt.Errorf("standing service identity conflict for %s", candidate.ServiceID)
		}
		bundleHash, bundleSource := candidate.Source.StorageValues()
		if !current.DeclarationPresent || current.BundleHash != bundleHash || current.BundleSource != bundleSource {
			return nil
		}
		if _, err := s.requireStandingRunSourceTx(txctx, tx, current, true); err != nil {
			return err
		}
		transition := "resumed"
		reason := ""
		var journalErr error
		if s.isSQLite() {
			journalErr = tx.QueryRowContext(txctx, `SELECT transition, COALESCE(reason, '') FROM standing_service_journal WHERE service_id = ? ORDER BY sequence DESC LIMIT 1`, candidate.ServiceID).Scan(&transition, &reason)
		} else {
			journalErr = tx.QueryRowContext(txctx, `SELECT transition, COALESCE(reason, '') FROM standing_service_journal WHERE service_id = $1::uuid ORDER BY sequence DESC LIMIT 1`, candidate.ServiceID).Scan(&transition, &reason)
		}
		if journalErr != nil {
			return fmt.Errorf("load standing service %s latest journal entry: %w", candidate.ServiceID, journalErr)
		}
		result = standingResult(candidate, current.RunID, current.Generation, current.PublicationSequence, transition, current.EffectiveState, reason)
		found = true
		return nil
	})
	return result, found, err
}

// ReconcileStandingServiceSet is the startup desired-state owner. It validates
// the full declaration set before mutation and orphans persisted services that
// are no longer declared in that same selected-store transaction.
func (s *standingServiceAdapter) ReconcileStandingServiceSet(ctx context.Context, candidates []runtimepipeline.StandingServiceCandidate) ([]runtimepipeline.StandingServiceReconciliation, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("workflow instance store is required")
	}
	normalized, err := normalizeStandingServiceCandidates(candidates)
	if err != nil {
		return nil, err
	}

	results := make([]runtimepipeline.StandingServiceReconciliation, 0, len(normalized))
	signalQueued := false
	err = s.runInPipelineTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		persisted, err := s.loadAllStandingServicesTx(txctx, tx)
		if err != nil {
			return err
		}
		declared := make(map[string]struct{}, len(normalized))
		for _, candidate := range normalized {
			declared[candidate.ServiceID] = struct{}{}
			result, err := s.reconcileStandingServiceTx(txctx, tx, candidate)
			if err != nil {
				return err
			}
			results = append(results, result)
		}
		for _, current := range persisted {
			if _, ok := declared[current.ServiceID]; ok || !current.DeclarationPresent {
				continue
			}
			result, err := s.orphanStandingServiceTx(txctx, tx, current)
			if err != nil {
				return err
			}
			if !signalQueued {
				if err := s.queueDeliveryContinuationSignal(txctx); err != nil {
					return err
				}
				signalQueued = true
			}
			results = append(results, result)
		}
		return nil
	})
	return results, err
}

// ReconcileStandingServiceReplacement is the hot-reload desired-state owner.
// It mutates only the predecessor declaration set so independently loaded
// runtime contexts remain outside the replacement transaction.
func (s *standingServiceAdapter) ReconcileStandingServiceReplacement(ctx context.Context, previous, candidates []runtimepipeline.StandingServiceCandidate) ([]runtimepipeline.StandingServiceReconciliation, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("workflow instance store is required")
	}
	previous, err := normalizeStandingServiceCandidates(previous)
	if err != nil {
		return nil, fmt.Errorf("validate predecessor standing declarations: %w", err)
	}
	candidates, err = normalizeStandingServiceCandidates(candidates)
	if err != nil {
		return nil, fmt.Errorf("validate replacement standing declarations: %w", err)
	}

	retained := make(map[string]struct{}, len(candidates))
	results := make([]runtimepipeline.StandingServiceReconciliation, 0, len(previous)+len(candidates))
	signalQueued := false
	err = s.runInPipelineTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		for _, candidate := range candidates {
			retained[candidate.ServiceID] = struct{}{}
			result, err := s.reconcileStandingServiceTx(txctx, tx, candidate)
			if err != nil {
				return err
			}
			results = append(results, result)
		}
		for _, predecessor := range previous {
			if _, ok := retained[predecessor.ServiceID]; ok {
				continue
			}
			current, found, err := s.loadStandingServiceTx(txctx, tx, predecessor.ServiceID)
			if err != nil {
				return err
			}
			if !found {
				return fmt.Errorf("predecessor standing service %s is not persisted", predecessor.ServiceID)
			}
			if !current.DeclarationPresent {
				continue
			}
			result, err := s.orphanStandingServiceTx(txctx, tx, current)
			if err != nil {
				return err
			}
			if !signalQueued {
				if err := s.queueDeliveryContinuationSignal(txctx); err != nil {
					return err
				}
				signalQueued = true
			}
			results = append(results, result)
		}
		return nil
	})
	return results, err
}

func normalizeStandingServiceCandidates(candidates []runtimepipeline.StandingServiceCandidate) ([]runtimepipeline.StandingServiceCandidate, error) {
	normalized := make([]runtimepipeline.StandingServiceCandidate, 0, len(candidates))
	seenService := map[string]struct{}{}
	seenOwner := map[string]struct{}{}
	for _, raw := range candidates {
		candidate := raw.Normalized()
		if err := candidate.Validate(); err != nil {
			return nil, err
		}
		owner := candidate.PackageKey + "\x00" + candidate.FlowID
		if _, exists := seenService[candidate.ServiceID]; exists {
			return nil, fmt.Errorf("standing service %s is declared by more than one runtime context", candidate.ServiceID)
		}
		if _, exists := seenOwner[owner]; exists {
			return nil, fmt.Errorf("standing owner %s/%s is declared by more than one runtime context", candidate.PackageKey, candidate.FlowID)
		}
		seenService[candidate.ServiceID] = struct{}{}
		seenOwner[owner] = struct{}{}
		normalized = append(normalized, candidate)
	}
	sort.Slice(normalized, func(i, j int) bool { return normalized[i].ServiceID < normalized[j].ServiceID })
	return normalized, nil
}

func (s *standingServiceAdapter) SuspendStandingService(ctx context.Context, operation runtimepipeline.StandingServiceOperation) (runtimepipeline.StandingServiceReconciliation, error) {
	operation = operation.Normalized()
	if operation.ServiceID == "" {
		return runtimepipeline.StandingServiceReconciliation{}, fmt.Errorf("standing service_id is required")
	}
	if operation.Reason == "" {
		operation.Reason = "operator_suspend"
	}
	var result runtimepipeline.StandingServiceReconciliation
	err := s.runInPipelineTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		current, found, err := s.loadStandingServiceTx(txctx, tx, operation.ServiceID)
		if err != nil {
			return err
		}
		if !found {
			return &runtimepipeline.StandingServiceError{ServiceID: operation.ServiceID, Err: runtimepipeline.ErrStandingServiceNotFound}
		}
		if !current.DeclarationPresent {
			return fmt.Errorf("standing service %s is orphaned; restore its declaration before suspending it", operation.ServiceID)
		}
		if current.OperatorOverride == "suspended" && current.EffectiveState == "suspended" {
			result = current.StandingServiceReconciliation
			result.Transition = "suspended"
			return nil
		}
		if _, err := s.requireStandingRunSourceTx(txctx, tx, current, true); err != nil {
			return err
		}
		now := time.Now().UTC()
		cancellations, err := s.quiesceStandingRunTx(txctx, tx, current.RunID, current.BundleHash, "standing_suspended", "cancelled", now)
		if err != nil {
			return err
		}
		if err := s.queueDeliveryContinuationSignal(txctx); err != nil {
			return err
		}
		if err := s.setStandingRunPausedTx(txctx, tx, current.RunID, operation.Reason, operation.Actor, now); err != nil {
			return err
		}
		if s.isSQLite() {
			_, err = tx.ExecContext(txctx, `UPDATE standing_services SET operator_override = 'suspended', effective_state = 'suspended', override_actor = ?, override_reason = NULLIF(?, ''), override_at = ?, publication_state = 'pending', updated_at = ? WHERE service_id = ?`, operation.Actor, operation.Reason, now, now, current.ServiceID)
		} else {
			_, err = tx.ExecContext(txctx, `UPDATE standing_services SET operator_override = 'suspended', effective_state = 'suspended', override_actor = $2, override_reason = NULLIF($3, ''), override_at = $4, publication_state = 'pending', updated_at = $4 WHERE service_id = $1::uuid`, current.ServiceID, operation.Actor, operation.Reason, now)
		}
		if err != nil {
			return fmt.Errorf("suspend standing service: %w", err)
		}
		result = current.StandingServiceReconciliation
		result.Transition = "suspended"
		result.EffectiveState = "suspended"
		result.Reason = operation.Reason
		result.TimerCancellations = cancellations
		return s.insertStandingJournalTx(txctx, tx, result, current.EffectiveState, operation.Actor, now)
	})
	return result, err
}

func (s *standingServiceAdapter) ResumeStandingService(ctx context.Context, operation runtimepipeline.StandingServiceOperation) (runtimepipeline.StandingServiceReconciliation, error) {
	operation = operation.Normalized()
	if operation.ServiceID == "" {
		return runtimepipeline.StandingServiceReconciliation{}, fmt.Errorf("standing service_id is required")
	}
	if operation.Reason == "" {
		operation.Reason = "operator_resume"
	}
	var result runtimepipeline.StandingServiceReconciliation
	err := s.runInPipelineTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		current, found, err := s.loadStandingServiceTx(txctx, tx, operation.ServiceID)
		if err != nil {
			return err
		}
		if !found {
			return &runtimepipeline.StandingServiceError{ServiceID: operation.ServiceID, Err: runtimepipeline.ErrStandingServiceNotFound}
		}
		if !current.DeclarationPresent {
			return fmt.Errorf("standing service %s is orphaned; restore its declaration before running `swarm standing resume %s`", operation.ServiceID, operation.ServiceID)
		}
		if err := s.admitStandingServiceRunTx(txctx, tx, current.RunID, operation.ExecutionPosture); err != nil {
			return err
		}
		if current.OperatorOverride == "none" && current.EffectiveState == "active" {
			result = current.StandingServiceReconciliation
			result.Transition = "operator_resumed"
			return nil
		}
		if _, err := s.requireStandingRunSourceTx(txctx, tx, current, true); err != nil {
			return err
		}
		now := time.Now().UTC()
		if err := s.setStandingRunRunningTx(txctx, tx, current.RunID, operation.Reason, operation.Actor, now); err != nil {
			return err
		}
		if s.isSQLite() {
			_, err = tx.ExecContext(txctx, `UPDATE standing_services SET operator_override = 'none', effective_state = 'active', override_actor = NULL, override_reason = NULL, override_at = NULL, publication_state = 'pending', updated_at = ? WHERE service_id = ?`, now, current.ServiceID)
		} else {
			_, err = tx.ExecContext(txctx, `UPDATE standing_services SET operator_override = 'none', effective_state = 'active', override_actor = NULL, override_reason = NULL, override_at = NULL, publication_state = 'pending', updated_at = $2 WHERE service_id = $1::uuid`, current.ServiceID, now)
		}
		if err != nil {
			return fmt.Errorf("resume standing service: %w", err)
		}
		result = current.StandingServiceReconciliation
		result.Transition = "operator_resumed"
		result.EffectiveState = "active"
		result.Reason = operation.Reason
		return s.insertStandingJournalTx(txctx, tx, result, current.EffectiveState, operation.Actor, now)
	})
	return result, err
}

func (s *standingServiceAdapter) ResetStandingService(ctx context.Context, operation runtimepipeline.StandingServiceOperation) (runtimepipeline.StandingServiceReconciliation, error) {
	operation = operation.Normalized()
	if operation.ServiceID == "" {
		return runtimepipeline.StandingServiceReconciliation{}, fmt.Errorf("standing service_id is required")
	}
	if operation.Reason == "" {
		operation.Reason = "standing_reset"
	}
	var result runtimepipeline.StandingServiceReconciliation
	err := s.runInPipelineTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var cancellations []runtimetimercancellation.Ref
		current, found, err := s.loadStandingServiceTx(txctx, tx, operation.ServiceID)
		if err != nil {
			return err
		}
		if !found {
			return &runtimepipeline.StandingServiceError{ServiceID: operation.ServiceID, Err: runtimepipeline.ErrStandingServiceNotFound}
		}
		if !current.DeclarationPresent {
			return fmt.Errorf("standing service %s is orphaned; restore its declaration before resetting it", operation.ServiceID)
		}
		if err := s.admitStandingServiceRunTx(txctx, tx, current.RunID, operation.ExecutionPosture); err != nil {
			return err
		}
		source, err := s.requireStandingRunSourceTx(txctx, tx, current, false)
		if err != nil {
			return err
		}
		now := time.Now().UTC()
		currentState, err := runtimerunlifecycle.ParseState(current.RunStatus)
		if err != nil {
			return err
		}
		if currentState.Active() {
			cancellations, err = s.quiesceStandingRunTx(txctx, tx, current.RunID, current.BundleHash, "standing_reset", "cancelled", now)
			if err != nil {
				return err
			}
			if err := s.queueDeliveryContinuationSignal(txctx); err != nil {
				return err
			}
			if err := s.setStandingRunCancelledTx(txctx, tx, current.RunID, "standing_reset", operation.Actor, now); err != nil {
				return err
			}
		}
		nextGeneration := current.Generation + 1
		nextRunID := runtimeflowidentity.StandingGenerationRunID(current.ServiceID, nextGeneration)
		origin, err := runtimerunlifecycle.StandingGenerationRunOrigin(current.ServiceID, nextGeneration)
		if err != nil {
			return err
		}
		if _, err := s.createRun(txctx, tx, runtimerunlifecycle.CreateRequest{
			RunID: nextRunID, Origin: origin, Source: source, StartedAt: now,
		}); err != nil {
			return err
		}
		effectiveState := "active"
		if current.OperatorOverride == "suspended" {
			effectiveState = "suspended"
			if err := s.setStandingRunPausedTx(txctx, tx, nextRunID, "standing_reset_preserved_suspend", operation.Actor, now); err != nil {
				return err
			}
		}
		if s.isSQLite() {
			if _, err := tx.ExecContext(txctx, `UPDATE standing_service_generations SET retired_at = ?, retired_reason = 'standing_reset', retired_by = ? WHERE service_id = ? AND generation = ? AND retired_at IS NULL`, now, operation.Actor, current.ServiceID, current.Generation); err != nil {
				return err
			}
			if _, err := tx.ExecContext(txctx, `INSERT INTO standing_service_generations (service_id, generation, run_id, created_at) VALUES (?, ?, ?, ?)`, current.ServiceID, nextGeneration, nextRunID, now); err != nil {
				return err
			}
			_, err = tx.ExecContext(txctx, `UPDATE standing_services SET current_generation = ?, current_run_id = ?, effective_state = ?, publication_state = 'pending', updated_at = ? WHERE service_id = ?`, nextGeneration, nextRunID, effectiveState, now, current.ServiceID)
		} else {
			if _, err := tx.ExecContext(txctx, `UPDATE standing_service_generations SET retired_at = $3, retired_reason = 'standing_reset', retired_by = $4 WHERE service_id = $1::uuid AND generation = $2 AND retired_at IS NULL`, current.ServiceID, current.Generation, now, operation.Actor); err != nil {
				return err
			}
			if _, err := tx.ExecContext(txctx, `INSERT INTO standing_service_generations (service_id, generation, run_id, created_at) VALUES ($1::uuid, $2, $3::uuid, $4)`, current.ServiceID, nextGeneration, nextRunID, now); err != nil {
				return err
			}
			_, err = tx.ExecContext(txctx, `UPDATE standing_services SET current_generation = $2, current_run_id = $3::uuid, effective_state = $4, publication_state = 'pending', updated_at = $5 WHERE service_id = $1::uuid`, current.ServiceID, nextGeneration, nextRunID, effectiveState, now)
		}
		if err != nil {
			return fmt.Errorf("reset standing service: %w", err)
		}
		candidate := runtimepipeline.StandingServiceCandidate{ServiceID: current.ServiceID, PackageKey: current.PackageKey, FlowID: current.FlowID, InstanceID: current.InstanceID, EntityID: current.EntityID, Source: source}
		result = standingResult(candidate, nextRunID, nextGeneration, current.PublicationSequence, "reset", effectiveState, operation.Reason)
		result.TimerCancellations = cancellations
		return s.insertStandingJournalTx(txctx, tx, result, current.EffectiveState, operation.Actor, now)
	})
	return result, err
}

func (s *standingServiceAdapter) AdmitStandingServiceRun(ctx context.Context, runID string, posture executionposture.Posture) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("workflow instance store is required")
	}
	return s.runInPipelineTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		return s.admitStandingServiceRunTx(txctx, tx, runID, posture)
	})
}

func (s *standingServiceAdapter) admitStandingServiceRunTx(ctx context.Context, tx *sql.Tx, runID string, posture executionposture.Posture) error {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return fmt.Errorf("standing service posture admission requires run_id")
	}
	if err := posture.Admit(executionmode.Mock, "standing service lifecycle admission"); err != nil {
		return err
	}
	if posture == executionposture.Live {
		return nil
	}
	query := `SELECT EXISTS (
		SELECT 1 FROM event_deliveries d JOIN events e ON e.event_id = d.event_id AND e.run_id = d.run_id
		WHERE d.run_id = ? AND d.status IN ('pending', 'in_progress', 'failed') AND e.execution_mode = 'live'
		UNION ALL SELECT 1 FROM timers WHERE run_id = ? AND status = 'active' AND execution_mode = 'live'
		UNION ALL SELECT 1 FROM decision_cards WHERE run_id = ? AND status = 'pending' AND execution_mode = 'live'
		UNION ALL SELECT 1 FROM activity_attempts WHERE run_id = ? AND status IN ('started', 'uncertain') AND execution_mode = 'live'
		UNION ALL SELECT 1 FROM flow_instance_runtime_readiness
			WHERE run_id = ? AND (topology_ready_at IS NULL OR creation_event_emitted_at IS NULL)
			AND json_extract(plan, '$.execution_mode') = 'live'
		UNION ALL SELECT 1 FROM events e
			LEFT JOIN event_receipts receipt ON receipt.event_id = e.event_id
				AND receipt.subscriber_type = 'platform' AND receipt.subscriber_id = 'pipeline'
			WHERE e.run_id = ? AND e.execution_mode = 'live' AND receipt.event_id IS NULL
			AND NOT EXISTS (
				SELECT 1 FROM decision_card_route_obligations route
				WHERE route.event_id = e.event_id AND route.status <> 'completed'
			)
			AND ` + sqliteDiagnosticDirectReplayExclusionSQL("e") + `
		UNION ALL SELECT 1 FROM decision_card_route_obligations route
			JOIN events e ON e.event_id = route.event_id
			WHERE route.run_id = ? AND route.status = 'pending' AND e.execution_mode = 'live'
	)`
	args := []any{runID, runID, runID, runID, runID, runID}
	args = append(args, diagnosticDirectReplayEventArgs()...)
	args = append(args, runID)
	if !s.isSQLite() {
		query = `SELECT EXISTS (
			SELECT 1 FROM event_deliveries d JOIN events e ON e.event_id = d.event_id AND e.run_id = d.run_id
			WHERE d.run_id = $1::uuid AND d.status IN ('pending', 'in_progress', 'failed') AND e.execution_mode = 'live'
			UNION ALL SELECT 1 FROM timers WHERE run_id = $1::uuid AND status = 'active' AND execution_mode = 'live'
			UNION ALL SELECT 1 FROM decision_cards WHERE run_id = $1::uuid AND status = 'pending' AND execution_mode = 'live'
			UNION ALL SELECT 1 FROM activity_attempts WHERE run_id = $1::uuid AND status IN ('started', 'uncertain') AND execution_mode = 'live'
			UNION ALL SELECT 1 FROM flow_instance_runtime_readiness
				WHERE run_id = $1::uuid AND (topology_ready_at IS NULL OR creation_event_emitted_at IS NULL)
				AND plan->>'execution_mode' = 'live'
			UNION ALL SELECT 1 FROM events e
				LEFT JOIN event_receipts receipt ON receipt.event_id = e.event_id
					AND receipt.subscriber_type = 'platform' AND receipt.subscriber_id = 'pipeline'
				WHERE e.run_id = $1::uuid AND e.execution_mode = 'live' AND receipt.event_id IS NULL
				AND NOT EXISTS (
					SELECT 1 FROM decision_card_route_obligations route
					WHERE route.event_id = e.event_id AND route.status <> 'completed'
				)
				AND ` + postgresDiagnosticDirectReplayExclusionSQL("e", 2) + `
			UNION ALL SELECT 1 FROM decision_card_route_obligations route
				JOIN events e ON e.event_id = route.event_id
				WHERE route.run_id = $1::uuid AND route.status = 'pending' AND e.execution_mode = 'live'
		)`
		args = []any{runID}
		args = append(args, diagnosticDirectReplayEventArgs()...)
	}
	var blocked bool
	if err := tx.QueryRowContext(ctx, query, args...).Scan(&blocked); err != nil {
		return fmt.Errorf("inspect standing service execution posture: %w", err)
	}
	if blocked {
		return posture.Admit(executionmode.Live, "standing service activation or lifecycle mutation")
	}
	return nil
}

func (s *standingServiceAdapter) reconcileStandingServiceTx(ctx context.Context, tx *sql.Tx, candidate runtimepipeline.StandingServiceCandidate) (runtimepipeline.StandingServiceReconciliation, error) {
	current, found, err := s.loadStandingServiceTx(ctx, tx, candidate.ServiceID)
	if err != nil {
		return runtimepipeline.StandingServiceReconciliation{}, err
	}
	if !found {
		return s.createStandingServiceTx(ctx, tx, candidate)
	}
	if current.PackageKey != candidate.PackageKey || current.FlowID != candidate.FlowID || current.InstanceID != candidate.InstanceID || current.EntityID != candidate.EntityID {
		return runtimepipeline.StandingServiceReconciliation{}, fmt.Errorf("standing service identity conflict for %s", candidate.ServiceID)
	}
	currentState, err := runtimerunlifecycle.ParseState(current.RunStatus)
	if err != nil {
		return runtimepipeline.StandingServiceReconciliation{}, err
	}
	switch {
	case currentState.Active():
		return s.resumeStandingServiceTx(ctx, tx, current, candidate)
	case currentState == runtimerunlifecycle.StateCancelled:
		if current.RunControlReason != standingRestartAbandonReason {
			return runtimepipeline.StandingServiceReconciliation{}, standingResetRequiredError(current, "cancelled standing generation is not owned by restart abandonment")
		}
		if live, err := s.standingRunHasLiveWorkTx(ctx, tx, current.RunID, time.Now().UTC()); err != nil {
			return runtimepipeline.StandingServiceReconciliation{}, err
		} else if live {
			return runtimepipeline.StandingServiceReconciliation{}, standingResetRequiredError(current, "restart-abandoned generation still owns live work")
		}
		return s.repairStandingServiceTx(ctx, tx, current, candidate)
	default:
		return runtimepipeline.StandingServiceReconciliation{}, standingResetRequiredError(current, "standing generation is terminal")
	}
}

func (s *standingServiceAdapter) PublishStandingService(ctx context.Context, serviceID, runID string, generation int64) (int64, error) {
	serviceID = strings.TrimSpace(serviceID)
	runID = strings.TrimSpace(runID)
	if serviceID == "" || runID == "" || generation <= 0 {
		return 0, fmt.Errorf("standing publication requires service_id, run_id, and generation")
	}
	var sequence int64
	err := s.runInPipelineTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		if s.isSQLite() {
			result, err := tx.ExecContext(txctx, `
				UPDATE standing_services
				SET publication_state = 'published', publication_sequence = publication_sequence + 1, updated_at = ?
				WHERE service_id = ? AND current_run_id = ? AND current_generation = ? AND effective_state = 'active'
			`, time.Now().UTC(), serviceID, runID, generation)
			if err != nil {
				return err
			}
			if err := requireOneStandingRow(result); err != nil {
				return err
			}
			return tx.QueryRowContext(txctx, `SELECT publication_sequence FROM standing_services WHERE service_id = ?`, serviceID).Scan(&sequence)
		}
		return tx.QueryRowContext(txctx, `
			UPDATE standing_services
			SET publication_state = 'published', publication_sequence = publication_sequence + 1, updated_at = now()
			WHERE service_id = $1::uuid AND current_run_id = $2::uuid AND current_generation = $3 AND effective_state = 'active'
			RETURNING publication_sequence
		`, serviceID, runID, generation).Scan(&sequence)
	})
	if errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("standing service changed before ingress publication")
	}
	return sequence, err
}

func (s *standingServiceAdapter) StandingRunUsesIntrinsicRecovery(ctx context.Context, runID string) (bool, error) {
	runID = strings.TrimSpace(runID)
	if s == nil || s.db == nil || runID == "" {
		return false, nil
	}
	var active bool
	if s.isSQLite() {
		err := s.db.QueryRowContext(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM standing_services
				WHERE current_run_id = ? AND declaration_present = TRUE AND effective_state = 'active'
			)
		`, runID).Scan(&active)
		return active, err
	}
	err := s.db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM standing_services
			WHERE current_run_id = $1::uuid AND declaration_present = TRUE AND effective_state = 'active'
		)
	`, runID).Scan(&active)
	return active, err
}

func (s *standingServiceAdapter) ListStandingServiceStatuses(ctx context.Context) ([]runtimepipeline.StandingServiceStatus, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("workflow instance store is required")
	}
	query := `
		SELECT ss.service_id, ss.package_key, ss.flow_id, ss.instance_id, ss.entity_id,
		       ss.current_run_id, ss.current_generation, ss.publication_sequence,
		       ss.effective_state, ss.current_bundle_hash, ss.current_bundle_source,
		       ss.declaration_present, ss.operator_override, ss.publication_state,
		       COALESCE(ss.override_actor, ''), COALESCE(ss.override_reason, ''), ss.override_at,
		       COALESCE((SELECT transition FROM standing_service_journal j WHERE j.service_id = ss.service_id ORDER BY sequence DESC LIMIT 1), 'resumed'),
		       COALESCE((SELECT reason FROM standing_service_journal j WHERE j.service_id = ss.service_id ORDER BY sequence DESC LIMIT 1), '')
		FROM standing_services ss
		ORDER BY ss.package_key, ss.flow_id
	`
	if !s.isSQLite() {
		query = `
			SELECT ss.service_id::text, ss.package_key, ss.flow_id, ss.instance_id, ss.entity_id::text,
			       ss.current_run_id::text, ss.current_generation, ss.publication_sequence,
			       ss.effective_state, ss.current_bundle_hash, ss.current_bundle_source,
			       ss.declaration_present, ss.operator_override, ss.publication_state,
			       COALESCE(ss.override_actor, ''), COALESCE(ss.override_reason, ''), ss.override_at,
			       COALESCE((SELECT transition FROM standing_service_journal j WHERE j.service_id = ss.service_id ORDER BY sequence DESC LIMIT 1), 'resumed'),
			       COALESCE((SELECT reason FROM standing_service_journal j WHERE j.service_id = ss.service_id ORDER BY sequence DESC LIMIT 1), '')
			FROM standing_services ss
			ORDER BY ss.package_key, ss.flow_id
		`
	}
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("list standing service statuses: %w", err)
	}
	defer rows.Close()
	var out []runtimepipeline.StandingServiceStatus
	for rows.Next() {
		var status runtimepipeline.StandingServiceStatus
		var overrideAt any
		if err := rows.Scan(
			&status.ServiceID, &status.PackageKey, &status.FlowID, &status.InstanceID, &status.EntityID,
			&status.RunID, &status.Generation, &status.PublicationSequence,
			&status.EffectiveState, &status.BundleHash, &status.BundleSource,
			&status.DeclarationPresent, &status.OperatorOverride, &status.PublicationState,
			&status.OverrideActor, &status.OverrideReason, &overrideAt, &status.Transition, &status.Reason,
		); err != nil {
			return nil, fmt.Errorf("scan standing service status: %w", err)
		}
		if overrideAt != nil {
			switch value := overrideAt.(type) {
			case time.Time:
				status.OverrideAt = value.UTC()
			case string:
				parsed, err := parseStandingTimestamp(value)
				if err != nil {
					return nil, fmt.Errorf("parse standing override time: %w", err)
				}
				status.OverrideAt = parsed.UTC()
			case []byte:
				parsed, err := parseStandingTimestamp(string(value))
				if err != nil {
					return nil, fmt.Errorf("parse standing override time: %w", err)
				}
				status.OverrideAt = parsed.UTC()
			}
		}
		out = append(out, status)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read standing service statuses: %w", err)
	}
	return out, nil
}

func parseStandingTimestamp(raw string) (time.Time, error) {
	raw = strings.TrimSpace(raw)
	var lastErr error
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05.999999999 -0700 MST", "2006-01-02 15:04:05.999999999Z07:00"} {
		parsed, err := time.Parse(layout, raw)
		if err == nil {
			return parsed, nil
		}
		lastErr = err
	}
	return time.Time{}, lastErr
}

func (s *standingServiceAdapter) loadStandingServiceTx(ctx context.Context, tx *sql.Tx, serviceID string) (standingServiceRow, bool, error) {
	var row standingServiceRow
	var query string
	if s.isSQLite() {
		query = `
			SELECT ss.service_id, ss.package_key, ss.flow_id, ss.instance_id, ss.entity_id,
			       ss.current_run_id, ss.current_generation, ss.publication_sequence,
			       ss.declaration_present, ss.operator_override, ss.effective_state,
			       ss.current_bundle_hash, ss.current_bundle_source, ss.revision_sequence,
			       ss.publication_state, COALESCE(r.status, ''), COALESCE(rc.reason, '')
			FROM standing_services ss
			JOIN runs r ON r.run_id = ss.current_run_id
			LEFT JOIN run_control_state rc ON rc.run_id = ss.current_run_id
			WHERE ss.service_id = ?
		`
	} else {
		query = `
			SELECT ss.service_id::text, ss.package_key, ss.flow_id, ss.instance_id, ss.entity_id::text,
			       ss.current_run_id::text, ss.current_generation, ss.publication_sequence,
			       ss.declaration_present, ss.operator_override, ss.effective_state,
			       ss.current_bundle_hash, ss.current_bundle_source, ss.revision_sequence,
			       ss.publication_state, COALESCE(r.status, ''), COALESCE(rc.reason, '')
			FROM standing_services ss
			JOIN runs r ON r.run_id = ss.current_run_id
			LEFT JOIN run_control_state rc ON rc.run_id = ss.current_run_id
			WHERE ss.service_id = $1::uuid
			FOR UPDATE OF ss, r
		`
	}
	err := tx.QueryRowContext(ctx, query, serviceID).Scan(
		&row.ServiceID, &row.PackageKey, &row.FlowID, &row.InstanceID, &row.EntityID,
		&row.RunID, &row.Generation, &row.PublicationSequence,
		&row.DeclarationPresent, &row.OperatorOverride, &row.EffectiveState,
		&row.BundleHash, &row.BundleSource, &row.RevisionSequence,
		&row.PublicationState, &row.RunStatus, &row.RunControlReason,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return standingServiceRow{}, false, nil
	}
	if err != nil {
		return standingServiceRow{}, false, fmt.Errorf("load standing service %s: %w", serviceID, err)
	}
	return row, true, nil
}

func (s *standingServiceAdapter) loadAllStandingServicesTx(ctx context.Context, tx *sql.Tx) ([]standingServiceRow, error) {
	query := `
		SELECT ss.service_id, ss.package_key, ss.flow_id, ss.instance_id, ss.entity_id,
		       ss.current_run_id, ss.current_generation, ss.publication_sequence,
		       ss.declaration_present, ss.operator_override, ss.effective_state,
		       ss.current_bundle_hash, ss.current_bundle_source, ss.revision_sequence,
		       ss.publication_state, COALESCE(r.status, ''), COALESCE(rc.reason, '')
		FROM standing_services ss
		JOIN runs r ON r.run_id = ss.current_run_id
		LEFT JOIN run_control_state rc ON rc.run_id = ss.current_run_id
		ORDER BY ss.service_id
	`
	if !s.isSQLite() {
		query = `
			SELECT ss.service_id::text, ss.package_key, ss.flow_id, ss.instance_id, ss.entity_id::text,
			       ss.current_run_id::text, ss.current_generation, ss.publication_sequence,
			       ss.declaration_present, ss.operator_override, ss.effective_state,
			       ss.current_bundle_hash, ss.current_bundle_source, ss.revision_sequence,
			       ss.publication_state, COALESCE(r.status, ''), COALESCE(rc.reason, '')
			FROM standing_services ss
			JOIN runs r ON r.run_id = ss.current_run_id
			LEFT JOIN run_control_state rc ON rc.run_id = ss.current_run_id
			ORDER BY ss.service_id
			FOR UPDATE OF ss, r
		`
	}
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("load standing service set: %w", err)
	}
	defer rows.Close()
	var out []standingServiceRow
	for rows.Next() {
		var row standingServiceRow
		if err := rows.Scan(
			&row.ServiceID, &row.PackageKey, &row.FlowID, &row.InstanceID, &row.EntityID,
			&row.RunID, &row.Generation, &row.PublicationSequence,
			&row.DeclarationPresent, &row.OperatorOverride, &row.EffectiveState,
			&row.BundleHash, &row.BundleSource, &row.RevisionSequence,
			&row.PublicationState, &row.RunStatus, &row.RunControlReason,
		); err != nil {
			return nil, fmt.Errorf("scan standing service set: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read standing service set: %w", err)
	}
	return out, nil
}

func (s *standingServiceAdapter) createStandingServiceTx(ctx context.Context, tx *sql.Tx, candidate runtimepipeline.StandingServiceCandidate) (runtimepipeline.StandingServiceReconciliation, error) {
	generation := int64(1)
	runID := runtimeflowidentity.StandingGenerationRunID(candidate.ServiceID, generation)
	now := time.Now().UTC()
	origin, err := runtimerunlifecycle.StandingGenerationRunOrigin(candidate.ServiceID, generation)
	if err != nil {
		return runtimepipeline.StandingServiceReconciliation{}, err
	}
	if !s.validRunLifecycleMutation(tx) {
		return runtimepipeline.StandingServiceReconciliation{}, errors.New("standing run lifecycle owner is required")
	}
	if _, err := s.createRun(ctx, tx, runtimerunlifecycle.CreateRequest{
		RunID: runID, Origin: origin, Source: candidate.Source, StartedAt: now,
	}); err != nil {
		return runtimepipeline.StandingServiceReconciliation{}, err
	}
	bundleHash, bundleSource := candidate.Source.StorageValues()
	if s.isSQLite() {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO standing_services (
				service_id, package_key, flow_id, instance_id, entity_id, declaration_present,
				operator_override, effective_state, current_bundle_hash, current_bundle_source,
				revision_sequence, current_generation, current_run_id, publication_state,
				publication_sequence, created_at, updated_at
			) VALUES (?, ?, ?, ?, ?, TRUE, 'none', 'active', ?, ?, 1, ?, ?, 'pending', 0, ?, ?)
		`, candidate.ServiceID, candidate.PackageKey, candidate.FlowID, candidate.InstanceID, candidate.EntityID,
			bundleHash, bundleSource, generation, runID, now, now); err != nil {
			return runtimepipeline.StandingServiceReconciliation{}, fmt.Errorf("insert standing service: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO standing_service_generations (service_id, generation, run_id, created_at)
			VALUES (?, ?, ?, ?)
		`, candidate.ServiceID, generation, runID, now); err != nil {
			return runtimepipeline.StandingServiceReconciliation{}, fmt.Errorf("insert standing generation: %w", err)
		}
	} else {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO standing_services (
				service_id, package_key, flow_id, instance_id, entity_id, declaration_present,
				operator_override, effective_state, current_bundle_hash, current_bundle_source,
				revision_sequence, current_generation, current_run_id, publication_state,
				publication_sequence, created_at, updated_at
			) VALUES ($1::uuid, $2, $3, $4, $5::uuid, TRUE, 'none', 'active', $6, $7, 1, $8, $9::uuid, 'pending', 0, $10, $10)
		`, candidate.ServiceID, candidate.PackageKey, candidate.FlowID, candidate.InstanceID, candidate.EntityID,
			bundleHash, bundleSource, generation, runID, now); err != nil {
			return runtimepipeline.StandingServiceReconciliation{}, fmt.Errorf("insert standing service: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO standing_service_generations (service_id, generation, run_id, created_at)
			VALUES ($1::uuid, $2, $3::uuid, $4)
		`, candidate.ServiceID, generation, runID, now); err != nil {
			return runtimepipeline.StandingServiceReconciliation{}, fmt.Errorf("insert standing generation: %w", err)
		}
	}
	result := standingResult(candidate, runID, generation, 0, "created", "active", "")
	if err := s.insertStandingJournalTx(ctx, tx, result, "", "runtime", now); err != nil {
		return runtimepipeline.StandingServiceReconciliation{}, err
	}
	return result, nil
}

func (s *standingServiceAdapter) resumeStandingServiceTx(ctx context.Context, tx *sql.Tx, current standingServiceRow, candidate runtimepipeline.StandingServiceCandidate) (runtimepipeline.StandingServiceReconciliation, error) {
	if _, err := s.requireStandingRunSourceTx(ctx, tx, current, true); err != nil {
		return runtimepipeline.StandingServiceReconciliation{}, err
	}
	bundleHash, bundleSource := candidate.Source.StorageValues()
	transition := "resumed"
	if current.BundleHash != bundleHash || current.BundleSource != bundleSource {
		transition = "revised"
	}
	effectiveState := "active"
	if current.OperatorOverride == "suspended" {
		effectiveState = "suspended"
	}
	revisionSequence := current.RevisionSequence
	if transition == "revised" {
		revisionSequence++
		if _, err := s.reviseRunSource(ctx, tx, runtimerunlifecycle.SourceRevisionRequest{
			RunID: current.RunID, Source: candidate.Source,
		}); err != nil {
			return runtimepipeline.StandingServiceReconciliation{}, err
		}
	}
	now := time.Now().UTC()
	if effectiveState == "active" {
		if err := s.setStandingRunRunningTx(ctx, tx, current.RunID, "standing_reconcile", "runtime", now); err != nil {
			return runtimepipeline.StandingServiceReconciliation{}, err
		}
	} else if err := s.setStandingRunPausedTx(ctx, tx, current.RunID, "standing_suspended", "runtime", now); err != nil {
		return runtimepipeline.StandingServiceReconciliation{}, err
	}
	if s.isSQLite() {
		_, err := tx.ExecContext(ctx, `
			UPDATE standing_services
			SET declaration_present = TRUE, effective_state = ?, current_bundle_hash = ?, current_bundle_source = ?,
			    revision_sequence = ?, publication_state = 'pending', updated_at = ?
			WHERE service_id = ?
		`, effectiveState, bundleHash, bundleSource, revisionSequence, now, candidate.ServiceID)
		if err != nil {
			return runtimepipeline.StandingServiceReconciliation{}, err
		}
	} else {
		_, err := tx.ExecContext(ctx, `
			UPDATE standing_services
			SET declaration_present = TRUE, effective_state = $2, current_bundle_hash = $3, current_bundle_source = $4,
			    revision_sequence = $5, publication_state = 'pending', updated_at = $6
			WHERE service_id = $1::uuid
		`, candidate.ServiceID, effectiveState, bundleHash, bundleSource, revisionSequence, now)
		if err != nil {
			return runtimepipeline.StandingServiceReconciliation{}, err
		}
	}
	result := standingResult(candidate, current.RunID, current.Generation, current.PublicationSequence, transition, effectiveState, "")
	if err := s.insertStandingJournalTx(ctx, tx, result, current.EffectiveState, "runtime", now); err != nil {
		return runtimepipeline.StandingServiceReconciliation{}, err
	}
	return result, nil
}

func (s *standingServiceAdapter) repairStandingServiceTx(ctx context.Context, tx *sql.Tx, current standingServiceRow, candidate runtimepipeline.StandingServiceCandidate) (runtimepipeline.StandingServiceReconciliation, error) {
	if _, err := s.requireStandingRunSourceTx(ctx, tx, current, false); err != nil {
		return runtimepipeline.StandingServiceReconciliation{}, err
	}
	nextGeneration := current.Generation + 1
	nextRunID := runtimeflowidentity.StandingGenerationRunID(candidate.ServiceID, nextGeneration)
	now := time.Now().UTC()
	origin, err := runtimerunlifecycle.StandingGenerationRunOrigin(candidate.ServiceID, nextGeneration)
	if err != nil {
		return runtimepipeline.StandingServiceReconciliation{}, err
	}
	if !s.validRunLifecycleMutation(tx) {
		return runtimepipeline.StandingServiceReconciliation{}, errors.New("standing run lifecycle owner is required")
	}
	if _, err := s.createRun(ctx, tx, runtimerunlifecycle.CreateRequest{
		RunID: nextRunID, Origin: origin, Source: candidate.Source, StartedAt: now,
	}); err != nil {
		return runtimepipeline.StandingServiceReconciliation{}, err
	}
	bundleHash, bundleSource := candidate.Source.StorageValues()
	if err := s.copyStandingEntityStateTx(ctx, tx, current.RunID, nextRunID, candidate.EntityID, now); err != nil {
		return runtimepipeline.StandingServiceReconciliation{}, err
	}
	if s.isSQLite() {
		if _, err := tx.ExecContext(ctx, `UPDATE standing_service_generations SET retired_at = ?, retired_reason = ?, retired_by = 'runtime' WHERE service_id = ? AND generation = ? AND retired_at IS NULL`, now, standingRestartAbandonReason, candidate.ServiceID, current.Generation); err != nil {
			return runtimepipeline.StandingServiceReconciliation{}, err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO standing_service_generations (service_id, generation, run_id, created_at) VALUES (?, ?, ?, ?)`, candidate.ServiceID, nextGeneration, nextRunID, now); err != nil {
			return runtimepipeline.StandingServiceReconciliation{}, err
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE standing_services
			SET declaration_present = TRUE, effective_state = CASE WHEN operator_override = 'suspended' THEN 'suspended' ELSE 'active' END,
			    current_bundle_hash = ?, current_bundle_source = ?, revision_sequence = revision_sequence + 1,
			    current_generation = ?, current_run_id = ?, publication_state = 'pending', updated_at = ?
			WHERE service_id = ?
		`, bundleHash, bundleSource, nextGeneration, nextRunID, now, candidate.ServiceID); err != nil {
			return runtimepipeline.StandingServiceReconciliation{}, err
		}
	} else {
		if _, err := tx.ExecContext(ctx, `UPDATE standing_service_generations SET retired_at = $3, retired_reason = $4, retired_by = 'runtime' WHERE service_id = $1::uuid AND generation = $2 AND retired_at IS NULL`, candidate.ServiceID, current.Generation, now, standingRestartAbandonReason); err != nil {
			return runtimepipeline.StandingServiceReconciliation{}, err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO standing_service_generations (service_id, generation, run_id, created_at) VALUES ($1::uuid, $2, $3::uuid, $4)`, candidate.ServiceID, nextGeneration, nextRunID, now); err != nil {
			return runtimepipeline.StandingServiceReconciliation{}, err
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE standing_services
			SET declaration_present = TRUE, effective_state = CASE WHEN operator_override = 'suspended' THEN 'suspended' ELSE 'active' END,
			    current_bundle_hash = $2, current_bundle_source = $3, revision_sequence = revision_sequence + 1,
			    current_generation = $4, current_run_id = $5::uuid, publication_state = 'pending', updated_at = $6
			WHERE service_id = $1::uuid
		`, candidate.ServiceID, bundleHash, bundleSource, nextGeneration, nextRunID, now); err != nil {
			return runtimepipeline.StandingServiceReconciliation{}, err
		}
	}
	effectiveState := "active"
	if current.OperatorOverride == "suspended" {
		effectiveState = "suspended"
	}
	result := standingResult(candidate, nextRunID, nextGeneration, current.PublicationSequence, "repaired", effectiveState, standingRestartAbandonReason)
	if err := s.insertStandingJournalTx(ctx, tx, result, current.EffectiveState, "runtime", now); err != nil {
		return runtimepipeline.StandingServiceReconciliation{}, err
	}
	return result, nil
}

func (s *standingServiceAdapter) orphanStandingServiceTx(ctx context.Context, tx *sql.Tx, current standingServiceRow) (runtimepipeline.StandingServiceReconciliation, error) {
	currentState, err := runtimerunlifecycle.ParseState(current.RunStatus)
	if err != nil {
		return runtimepipeline.StandingServiceReconciliation{}, err
	}
	if !currentState.Active() {
		return runtimepipeline.StandingServiceReconciliation{}, standingResetRequiredError(current, "removed declaration points at a terminal generation")
	}
	now := time.Now().UTC()
	cancellations, err := s.quiesceStandingRunTx(ctx, tx, current.RunID, current.BundleHash, "standing_declaration_removed", "orphaned", now)
	if err != nil {
		return runtimepipeline.StandingServiceReconciliation{}, err
	}
	if err := s.setStandingRunPausedTx(ctx, tx, current.RunID, "standing_declaration_removed", "runtime", now); err != nil {
		return runtimepipeline.StandingServiceReconciliation{}, err
	}
	err = nil
	if s.isSQLite() {
		_, err = tx.ExecContext(ctx, `UPDATE standing_services SET declaration_present = FALSE, effective_state = 'orphaned', publication_state = 'pending', updated_at = ? WHERE service_id = ?`, now, current.ServiceID)
	} else {
		_, err = tx.ExecContext(ctx, `UPDATE standing_services SET declaration_present = FALSE, effective_state = 'orphaned', publication_state = 'pending', updated_at = $2 WHERE service_id = $1::uuid`, current.ServiceID, now)
	}
	if err != nil {
		return runtimepipeline.StandingServiceReconciliation{}, fmt.Errorf("orphan standing service: %w", err)
	}
	result := current.StandingServiceReconciliation
	result.Transition = "orphaned"
	result.EffectiveState = "orphaned"
	result.Reason = "standing_declaration_removed"
	result.TimerCancellations = cancellations
	if err := s.insertStandingJournalTx(ctx, tx, result, current.EffectiveState, "runtime", now); err != nil {
		return runtimepipeline.StandingServiceReconciliation{}, err
	}
	return result, nil
}

func (s *standingServiceAdapter) standingRunHasLiveWorkTx(ctx context.Context, tx *sql.Tx, runID string, observedAt time.Time) (bool, error) {
	var deliverySummary runtimedelivery.RunSummary
	var err error
	if s.isSQLite() {
		deliverySummary, err = s.sqliteStore.DeliverySQLiteOwner.SummarizeRunTx(ctx, tx, runID)
	} else {
		deliverySummary, err = s.postgresStore.DeliveryPostgresOwner.SummarizeRunTx(ctx, tx, runID)
	}
	if err != nil {
		return false, fmt.Errorf("inspect standing run delivery work: %w", err)
	}
	if !deliverySummary.Settled() {
		return true, nil
	}
	var pipelineSummary runtimepipelineobligation.RunSummary
	if s.isSQLite() {
		pipelineSummary, err = s.sqliteStore.SummarizeRunTx(ctx, tx, runID)
	} else {
		pipelineSummary, err = s.postgresStore.SummarizeRunTx(ctx, tx, runID)
	}
	if err != nil {
		return false, fmt.Errorf("inspect standing run pipeline work: %w", err)
	}
	if pipelineSummary.HasOpenWork() {
		return true, nil
	}
	query := `SELECT EXISTS (SELECT 1 FROM agent_sessions WHERE run_id = ? AND status IN ('active', 'suspended'))`
	args := []any{runID}
	if !s.isSQLite() {
		query = `SELECT EXISTS (SELECT 1 FROM agent_sessions WHERE run_id = $1::uuid AND status IN ('active', 'suspended'))`
		args = []any{runID}
	}
	var live bool
	if err := tx.QueryRowContext(ctx, query, args...).Scan(&live); err != nil {
		return false, fmt.Errorf("inspect standing run live work: %w", err)
	}
	if live {
		return true, nil
	}
	scope, err := runtimetimerobligation.Run(runID)
	if err != nil {
		return false, err
	}
	var snapshot runtimetimerobligation.Snapshot
	if s.isSQLite() {
		snapshot, err = timerobligationstore.ReadSQLiteTx(ctx, tx, scope, observedAt)
	} else {
		snapshot, err = timerobligationstore.ReadPostgresTx(ctx, tx, scope, observedAt)
	}
	if err != nil {
		return false, fmt.Errorf("inspect standing run timer work: %w", err)
	}
	run, ok := snapshot.Run(runID)
	if !ok {
		return false, fmt.Errorf("inspect standing run timer work: snapshot omitted requested run")
	}
	return run.Totals().ActiveCount > 0, nil
}

func (s *standingServiceAdapter) quiesceStandingRunTx(ctx context.Context, tx *sql.Tx, runID, bundleHash, reason, sessionReason string, now time.Time) ([]runtimetimercancellation.Ref, error) {
	if !s.validRunLifecycleMutation(tx) {
		return nil, fmt.Errorf("quiesce standing run: standing transaction owner is required")
	}
	scope, err := runtimeauthoractivity.BundleScopeForTarget(ctx, bundleHash)
	if err != nil {
		return nil, fmt.Errorf("quiesce standing run scope: %w", err)
	}
	ctx = runtimeauthoractivity.WithScope(ctx, scope)
	var deliveryErr error
	if s.isSQLite() {
		_, deliveryErr = s.sqliteStore.TerminalizeRunDeliveriesTx(ctx, tx, s.story, s.revisionEffects, runID, reason)
	} else {
		_, deliveryErr = s.postgresStore.TerminalizeRunDeliveriesTx(ctx, tx, s.story, s.revisionEffects, runID, reason)
	}
	if deliveryErr != nil {
		return nil, fmt.Errorf("terminalize standing deliveries: %w", deliveryErr)
	}
	disposition := runtimepipelineobligation.DeadLetter(reason, nil)
	var pipelineErr error
	if s.isSQLite() {
		_, pipelineErr = s.sqliteStore.TerminalizeRunTx(ctx, tx, s.revisionEffects, runID, disposition, now)
	} else {
		_, pipelineErr = s.postgresStore.TerminalizeRunTx(ctx, tx, s.revisionEffects, runID, disposition, now)
	}
	if pipelineErr != nil {
		return nil, fmt.Errorf("terminalize standing pipeline obligations: %w", pipelineErr)
	}
	if s.isSQLite() {
		result, err := tx.ExecContext(ctx, `UPDATE agent_sessions SET status = 'terminated', termination_reason = ?, termination_detail = ?, terminated_at = COALESCE(terminated_at, ?), lease_holder = NULL, lease_expires_at = NULL, updated_at = ? WHERE run_id = ? AND status IN ('active', 'suspended')`, sessionReason, reason, now, now, runID)
		if err != nil {
			return nil, fmt.Errorf("terminate sqlite standing sessions: %w", err)
		}
		if changed, err := result.RowsAffected(); err != nil {
			return nil, err
		} else if changed > 0 {
			if err := s.revisionEffects.Add(runID, privaterunforkrevision.FamilyAgentSessions); err != nil {
				return nil, err
			}
		}
		generic, err := storegenericschedule.CancelRunsTx(ctx, tx, false, s.revisionEffects, []string{runID}, reason, now)
		if err != nil {
			return nil, fmt.Errorf("cancel sqlite standing generic schedules: %w", err)
		}
		workflow, err := storeworkflowtimer.CancelRunsTx(ctx, tx, false, s.revisionEffects, []string{runID})
		if err != nil {
			return nil, fmt.Errorf("cancel sqlite standing workflow timers: %w", err)
		}
		return append(generic, workflow...), nil
	}
	result, err := tx.ExecContext(ctx, `UPDATE agent_sessions SET status = 'terminated', termination_reason = $2, termination_detail = $3, terminated_at = COALESCE(terminated_at, $4), lease_holder = NULL, lease_expires_at = NULL, updated_at = $4 WHERE run_id = $1::uuid AND status IN ('active', 'suspended')`, runID, sessionReason, reason, now)
	if err != nil {
		return nil, fmt.Errorf("terminate standing sessions: %w", err)
	}
	if changed, err := result.RowsAffected(); err != nil {
		return nil, err
	} else if changed > 0 {
		if err := s.revisionEffects.Add(runID, privaterunforkrevision.FamilyAgentSessions); err != nil {
			return nil, err
		}
	}
	generic, err := storegenericschedule.CancelRunsTx(ctx, tx, true, s.revisionEffects, []string{runID}, reason, now)
	if err != nil {
		return nil, fmt.Errorf("cancel standing generic schedules: %w", err)
	}
	workflow, err := storeworkflowtimer.CancelRunsTx(ctx, tx, true, s.revisionEffects, []string{runID})
	if err != nil {
		return nil, fmt.Errorf("cancel standing workflow timers: %w", err)
	}
	return append(generic, workflow...), nil
}

func (s *standingServiceAdapter) setStandingRunPausedTx(ctx context.Context, tx *sql.Tx, runID, reason, actor string, now time.Time) error {
	if !s.validRunLifecycleMutation(tx) {
		return errors.New("standing run pause requires run lifecycle owner")
	}
	if _, err := s.transitionActiveRun(ctx, tx, runtimerunlifecycle.ActiveTransitionRequest{
		RunID: runID,
		State: runtimerunlifecycle.StatePaused,
	}); err != nil {
		return err
	}
	if s.isSQLite() {
		_, err := tx.ExecContext(ctx, `INSERT INTO run_control_state (run_id, control_status, reason, controlled_by, updated_at, paused_at, stopped_at) VALUES (?, 'paused', ?, ?, ?, ?, NULL) ON CONFLICT(run_id) DO UPDATE SET control_status = 'paused', reason = excluded.reason, controlled_by = excluded.controlled_by, updated_at = excluded.updated_at, paused_at = COALESCE(run_control_state.paused_at, excluded.paused_at), stopped_at = NULL`, runID, reason, actor, now, now)
		return err
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO run_control_state (run_id, control_status, reason, controlled_by, updated_at, paused_at, stopped_at) VALUES ($1::uuid, 'paused', $2, $3, $4, $4, NULL) ON CONFLICT(run_id) DO UPDATE SET control_status = 'paused', reason = EXCLUDED.reason, controlled_by = EXCLUDED.controlled_by, updated_at = EXCLUDED.updated_at, paused_at = COALESCE(run_control_state.paused_at, EXCLUDED.paused_at), stopped_at = NULL`, runID, reason, actor, now)
	return err
}

func (s *standingServiceAdapter) setStandingRunRunningTx(ctx context.Context, tx *sql.Tx, runID, reason, actor string, now time.Time) error {
	if !s.validRunLifecycleMutation(tx) {
		return errors.New("standing run resume requires run lifecycle owner")
	}
	if _, err := s.transitionActiveRun(ctx, tx, runtimerunlifecycle.ActiveTransitionRequest{
		RunID: runID,
		State: runtimerunlifecycle.StateRunning,
	}); err != nil {
		return err
	}
	if s.isSQLite() {
		_, err := tx.ExecContext(ctx, `INSERT INTO run_control_state (run_id, control_status, reason, controlled_by, updated_at, paused_at, stopped_at) VALUES (?, 'running', ?, ?, ?, NULL, NULL) ON CONFLICT(run_id) DO UPDATE SET control_status = 'running', reason = excluded.reason, controlled_by = excluded.controlled_by, updated_at = excluded.updated_at, paused_at = NULL, stopped_at = NULL`, runID, reason, actor, now)
		return err
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO run_control_state (run_id, control_status, reason, controlled_by, updated_at, paused_at, stopped_at) VALUES ($1::uuid, 'running', $2, $3, $4, NULL, NULL) ON CONFLICT(run_id) DO UPDATE SET control_status = 'running', reason = EXCLUDED.reason, controlled_by = EXCLUDED.controlled_by, updated_at = EXCLUDED.updated_at, paused_at = NULL, stopped_at = NULL`, runID, reason, actor, now)
	return err
}

func (s *standingServiceAdapter) setStandingRunCancelledTx(ctx context.Context, tx *sql.Tx, runID, reason, actor string, now time.Time) error {
	if !s.validRunLifecycleMutation(tx) {
		return errors.New("standing run cancellation requires run lifecycle owner")
	}
	if _, _, err := s.markTerminalRun(ctx, tx, runtimerunlifecycle.TerminalRequest{
		RunID:   runID,
		State:   runtimerunlifecycle.StateCancelled,
		EndedAt: now.UTC(),
	}); err != nil {
		return err
	}
	if s.isSQLite() {
		_, err := tx.ExecContext(ctx, `INSERT INTO run_control_state (run_id, control_status, reason, controlled_by, updated_at, paused_at, stopped_at) VALUES (?, 'stopped', ?, ?, ?, NULL, ?) ON CONFLICT(run_id) DO UPDATE SET control_status = 'stopped', reason = excluded.reason, controlled_by = excluded.controlled_by, updated_at = excluded.updated_at, paused_at = NULL, stopped_at = excluded.stopped_at`, runID, reason, actor, now, now)
		return err
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO run_control_state (run_id, control_status, reason, controlled_by, updated_at, paused_at, stopped_at) VALUES ($1::uuid, 'stopped', $2, $3, $4, NULL, $4) ON CONFLICT(run_id) DO UPDATE SET control_status = 'stopped', reason = EXCLUDED.reason, controlled_by = EXCLUDED.controlled_by, updated_at = EXCLUDED.updated_at, paused_at = NULL, stopped_at = EXCLUDED.stopped_at`, runID, reason, actor, now)
	return err
}

func (s *standingServiceAdapter) copyStandingEntityStateTx(ctx context.Context, tx *sql.Tx, oldRunID, newRunID, entityID string, copiedAt time.Time) error {
	var result sql.Result
	var err error
	if s.isSQLite() {
		result, err = tx.ExecContext(ctx, `
			INSERT INTO entity_state (run_id, entity_id, flow_instance, entity_type, slug, name, current_state, gates, fields, bookkeeping, accumulator, revision, entered_state_at, created_at, updated_at)
			SELECT ?, entity_id, flow_instance, entity_type, slug, name, current_state, gates, fields, bookkeeping, accumulator, revision, entered_state_at, created_at, ?
			FROM entity_state WHERE run_id = ? AND entity_id = ?
		`, newRunID, copiedAt.UTC(), oldRunID, entityID)
	} else {
		result, err = tx.ExecContext(ctx, `
			INSERT INTO entity_state (run_id, entity_id, flow_instance, entity_type, slug, name, current_state, gates, fields, bookkeeping, accumulator, revision, entered_state_at, created_at, updated_at)
			SELECT $1::uuid, entity_id, flow_instance, entity_type, slug, name, current_state, gates, fields, bookkeeping, accumulator, revision, entered_state_at, created_at, now()
			FROM entity_state WHERE run_id = $2::uuid AND entity_id = $3::uuid
		`, newRunID, oldRunID, entityID)
	}
	if err != nil {
		return fmt.Errorf("copy standing entity state: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count > 1 {
		return fmt.Errorf("standing repair found multiple current entity rows")
	}
	if count == 0 {
		return nil
	}
	if err := s.revisionEffects.Add(newRunID, privaterunforkrevision.FamilyEntityMetadata); err != nil {
		return err
	}
	projection, err := s.loadStandingEntityStateProjectionTx(ctx, tx, newRunID, entityID)
	if err != nil {
		return err
	}
	fact, err := s.requireActiveRunSource(ctx, tx, newRunID)
	if err != nil {
		return err
	}
	scope, err := runtimeauthoractivity.BundleScopeForTarget(ctx, fact.BundleHash())
	if err != nil {
		return fmt.Errorf("derive standing repair mutation scope: %w", err)
	}
	mutationCtx := runtimecorrelation.WithBundleSourceFact(runtimecorrelation.WithRunID(ctx, newRunID), fact)
	mutationCtx = runtimeauthoractivity.WithScope(mutationCtx, scope)
	writer := runtimemutationlog.Writer{Type: "platform", ID: "standing_service", HandlerStep: "repair_generation"}
	if s.postgresStore != nil {
		return privatemutationlog.InsertEntityStateDiffWithStory(
			mutationCtx,
			tx,
			activeRunSourceOwnerFunc(func(ctx context.Context, runID string) (runtimecorrelation.BundleSourceFact, error) {
				return s.postgresStore.RunLifecyclePostgresOwner.RequireActiveSourceTx(ctx, tx, runID)
			}),
			s.story,
			s.revisionEffects,
			entityID,
			runtimemutationlog.EntityStateProjection{},
			projection,
			writer,
		)
	}
	return storeentity.InsertSQLiteEntityStateDiff(
		mutationCtx,
		s.story,
		tx,
		s.revisionEffects,
		newRunID,
		entityID,
		runtimemutationlog.EntityStateProjection{},
		projection,
		writer,
		copiedAt,
	)
}

func (s *standingServiceAdapter) loadStandingEntityStateProjectionTx(ctx context.Context, tx *sql.Tx, runID, entityID string) (runtimemutationlog.EntityStateProjection, error) {
	query := `SELECT current_state, gates, fields, bookkeeping, accumulator FROM entity_state WHERE run_id = ? AND entity_id = ?`
	if s.postgres {
		query = `SELECT current_state, gates, fields, bookkeeping, accumulator FROM entity_state WHERE run_id = $1::uuid AND entity_id = $2::uuid`
	}
	var currentState string
	var gatesRaw, fieldsRaw, bookkeepingRaw, accumulatorRaw any
	if err := tx.QueryRowContext(ctx, query, runID, entityID).Scan(&currentState, &gatesRaw, &fieldsRaw, &bookkeepingRaw, &accumulatorRaw); err != nil {
		return runtimemutationlog.EntityStateProjection{}, fmt.Errorf("load copied standing entity state: %w", err)
	}
	gates, err := storeentity.DecodeJSONMap(gatesRaw)
	if err != nil {
		return runtimemutationlog.EntityStateProjection{}, fmt.Errorf("decode copied standing entity gates: %w", err)
	}
	fields, err := storeentity.DecodeJSONMap(fieldsRaw)
	if err != nil {
		return runtimemutationlog.EntityStateProjection{}, fmt.Errorf("decode copied standing entity fields: %w", err)
	}
	bookkeeping, err := storeentity.DecodeJSONMap(bookkeepingRaw)
	if err != nil {
		return runtimemutationlog.EntityStateProjection{}, fmt.Errorf("decode copied standing entity bookkeeping: %w", err)
	}
	accumulator, err := storeentity.DecodeJSONMap(accumulatorRaw)
	if err != nil {
		return runtimemutationlog.EntityStateProjection{}, fmt.Errorf("decode copied standing entity accumulator: %w", err)
	}
	return runtimemutationlog.EntityStateProjection{
		CurrentState: strings.TrimSpace(currentState),
		Fields:       fields,
		Bookkeeping:  bookkeeping,
		Gates:        gates,
		Accumulator:  accumulator,
	}, nil
}

func (s *standingServiceAdapter) insertStandingJournalTx(ctx context.Context, tx *sql.Tx, result runtimepipeline.StandingServiceReconciliation, previousState, actor string, now time.Time) error {
	operationID := uuid.NewString()
	if s.isSQLite() {
		_, err := tx.ExecContext(ctx, `
			INSERT INTO standing_service_journal (
				service_id, sequence, operation_id, generation, run_id, transition,
				previous_effective_state, effective_state, actor, reason, poison_provenance, occurred_at
			) VALUES (?, (SELECT COALESCE(MAX(sequence), 0) + 1 FROM standing_service_journal WHERE service_id = ?), ?, ?, ?, ?, NULLIF(?, ''), ?, ?, NULLIF(?, ''), NULLIF(?, ''), ?)
		`, result.ServiceID, result.ServiceID, operationID, result.Generation, result.RunID, result.Transition,
			previousState, result.EffectiveState, actor, result.Reason, result.Reason, now)
		if err != nil {
			return fmt.Errorf("insert standing journal: %w", err)
		}
		return nil
	}
	_, err := tx.ExecContext(ctx, `
		INSERT INTO standing_service_journal (
			service_id, sequence, operation_id, generation, run_id, transition,
			previous_effective_state, effective_state, actor, reason, poison_provenance, occurred_at
		) VALUES ($1::uuid, (SELECT COALESCE(MAX(sequence), 0) + 1 FROM standing_service_journal WHERE service_id = $1::uuid), $2::uuid, $3, $4::uuid, $5, NULLIF($6, ''), $7, $8, NULLIF($9, ''), NULLIF($10, ''), $11)
	`, result.ServiceID, operationID, result.Generation, result.RunID, result.Transition,
		previousState, result.EffectiveState, actor, result.Reason, result.Reason, now)
	if err != nil {
		return fmt.Errorf("insert standing journal: %w", err)
	}
	return nil
}

func standingResult(candidate runtimepipeline.StandingServiceCandidate, runID string, generation, publicationSequence int64, transition, effectiveState, reason string) runtimepipeline.StandingServiceReconciliation {
	bundleHash, bundleSource := candidate.Source.StorageValues()
	return runtimepipeline.StandingServiceReconciliation{
		ServiceID: candidate.ServiceID, PackageKey: candidate.PackageKey, FlowID: candidate.FlowID,
		InstanceID: candidate.InstanceID, EntityID: candidate.EntityID, RunID: runID,
		Generation: generation, PublicationSequence: publicationSequence, Transition: transition,
		EffectiveState: effectiveState, BundleHash: bundleHash,
		BundleSource: bundleSource, Reason: reason,
	}
}

func standingResetRequiredError(current standingServiceRow, reason string) error {
	return fmt.Errorf("standing service %s (%s/%s) cannot reconcile run %s status %s: %s; run `swarm standing reset %s`",
		current.ServiceID, current.PackageKey, current.FlowID, current.RunID, current.RunStatus, reason, current.ServiceID)
}

func requireOneStandingRow(result sql.Result) error {
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return fmt.Errorf("standing service changed during mutation")
	}
	return nil
}
