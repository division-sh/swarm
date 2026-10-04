package selected

import (
	"context"
	"errors"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/runbundle"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/timerobligation"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/store"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	storeconstruction "github.com/division-sh/swarm/internal/store/construction"
)

type admissionInspectionPort interface {
	Ping(context.Context) error
	Close() error
	InspectSnapshot(context.Context, func(context.Context) error) error
	InspectSchema(context.Context, store.SchemaBootstrapRequest) (store.SchemaInspection, error)
	ProbePossession(context.Context) (startupownership.PossessionObservation, error)
	authorityInspectionPort
	ActiveNonStandingRunBundleAvailabilities(context.Context) ([]runbundle.Availability, error)
	LoadRunBundleAvailability(context.Context, string) (runbundle.Availability, error)
	GetSourceArtifact(context.Context, string) (sourceartifact.Persisted, error)
	InspectDeliveryRecovery(context.Context, correlation.SourceArtifactFact) (deliverylifecycle.RecoveryInventory, error)
	ReadTimerObligations(context.Context, timerobligation.Scope, time.Time) (timerobligation.Snapshot, error)
	ReadResetInventory(context.Context) (destructivereset.Inventory, error)
	InspectPendingResetOperations(context.Context) ([]destructivereset.Operation, error)
	ListSelectedForkRecoveryEntries(context.Context) ([]runfork.SelectedForkRecoveryEntry, error)
	InspectSelectedForkRecovery(context.Context, runfork.SelectedForkRecoveryEntry) (runfork.SelectedForkRecoveryInspection, error)
	ListSelectedContractRouteRecoveryRecords(context.Context) ([]manager.SelectedContractRouteRecoveryRecord, error)
	ListActiveAgentDescriptors(context.Context, string) ([]runtimebus.ActiveAgentDescriptor, error)
	StandingRunRestartDisposition(context.Context, string) (runlifecycle.StandingRestartDisposition, error)
	InspectDynamicFlowRuntimeReadinessForSource(context.Context, correlation.SourceArtifactFact) (pipeline.DynamicFlowRuntimeReadinessProjection, error)
	LoadAgents(context.Context) ([]manager.PersistedAgent, error)
	ListFlowInstanceRoutes(context.Context) ([]flowidentity.RunScopedFlowInstance, error)
	PipelineObligations() pipelineobligation.Store
	ObserveOrdinaryRunSource(context.Context, correlation.SourceArtifactFact, string) (bool, error)
	channelonboarding.RetainedActivationReader
	channelonboarding.PendingTeardownReader
}

// AdmissionInspection is a read purpose, not an unactivated runtime owner.
// Its reads are scoped to one consistent callback; momentary possession is a
// separate, joined observation outside that MVCC snapshot.
type AdmissionInspection struct {
	store admissionInspectionPort
}

func OpenAdmissionInspection(ctx context.Context, req AuthorityRequest) (*AdmissionInspection, error) {
	selected, err := openInspectionStore(ctx, req)
	if err != nil {
		return nil, err
	}
	return &AdmissionInspection{store: selected}, nil
}

func openInspectionStore(ctx context.Context, req AuthorityRequest) (admissionInspectionPort, error) {
	var selected admissionInspectionPort
	var err error
	switch req.Selection.Backend {
	case storebackend.BackendSQLite:
		selected, err = storeconstruction.OpenSQLiteRuntimeReadOnly(req.Selection.SQLitePath)
	case storebackend.BackendPostgres:
		selected, err = storeconstruction.OpenPostgresReadOnly(req.PostgresDSN)
	default:
		return nil, errors.New("selected store backend is required")
	}
	if err != nil {
		return nil, err
	}
	if err := selected.Ping(ctx); err != nil {
		return nil, errors.Join(err, selected.Close())
	}
	return selected, nil
}

func (s *AdmissionInspection) Inspect(ctx context.Context, request store.SchemaBootstrapRequest, observe func(*AdmissionSnapshot) error) (store.SchemaInspection, error) {
	if s == nil || s.store == nil || observe == nil {
		return store.SchemaInspection{}, errors.New("selected admission inspection and callback are required")
	}
	var schema store.SchemaInspection
	err := s.store.InspectSnapshot(ctx, func(ctx context.Context) error {
		var err error
		schema, err = s.store.InspectSchema(ctx, request)
		if err != nil {
			return err
		}
		scoped, cancel := context.WithCancel(ctx)
		defer cancel()
		return observe(&AdmissionSnapshot{ctx: scoped, store: s.store, fresh: schema.Fresh})
	})
	return schema, err
}

func (s *AdmissionInspection) Close() error {
	if s == nil || s.store == nil {
		return nil
	}
	return s.store.Close()
}

func (s *AdmissionInspection) ProbePossession(ctx context.Context) (startupownership.PossessionObservation, error) {
	if s == nil || s.store == nil || ctx == nil {
		return startupownership.PossessionObservation{}, errors.New("selected admission inspection and bounded context are required")
	}
	if err := ctx.Err(); err != nil {
		return startupownership.PossessionObservation{}, err
	}
	return s.store.ProbePossession(ctx)
}

// AdmissionSnapshot cannot expose writes, raw SQL, process possession or a
// runtime projection. Passing another context cannot escape its read snapshot.
type AdmissionSnapshot struct {
	ctx   context.Context
	store admissionInspectionPort
	fresh bool
}

func (s *AdmissionSnapshot) RequiresSchemaPreparation() (bool, error) {
	if s == nil || s.ctx == nil || s.store == nil {
		return false, errors.New("selected admission read snapshot is required")
	}
	if err := s.ctx.Err(); err != nil {
		return false, err
	}
	return s.fresh, nil
}

func (s *AdmissionSnapshot) readContext(caller context.Context) (context.Context, func(), error) {
	if s == nil || s.ctx == nil || s.store == nil || caller == nil {
		return nil, nil, errors.New("selected admission read snapshot is required")
	}
	if err := s.ctx.Err(); err != nil {
		return nil, nil, err
	}
	if err := caller.Err(); err != nil {
		return nil, nil, err
	}
	if s.fresh {
		return nil, nil, errors.New("fresh selected store requires boot schema preparation before domain reads")
	}
	var ctx context.Context
	var cancel context.CancelFunc
	if deadline, ok := caller.Deadline(); ok {
		ctx, cancel = context.WithDeadline(s.ctx, deadline)
	} else {
		ctx, cancel = context.WithCancel(s.ctx)
	}
	stop := context.AfterFunc(caller, cancel)
	return ctx, func() { stop(); cancel() }, nil
}

func (s *AdmissionSnapshot) InspectAuthority(ctx context.Context) (startupownership.AuthorityInspection, error) {
	ctx, done, err := s.readContext(ctx)
	if err != nil {
		return startupownership.AuthorityInspection{}, err
	}
	defer done()
	return s.store.InspectAuthority(ctx)
}

func (s *AdmissionSnapshot) ListChannelOnboardingOperations(ctx context.Context) ([]channelonboarding.Operation, error) {
	ctx, done, err := s.readContext(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	return s.store.ListChannelOnboardingOperations(ctx)
}

func (s *AdmissionSnapshot) ListCurrentConnectedChannelActivations(ctx context.Context) ([]channelonboarding.ConnectedChannelActivation, error) {
	ctx, done, err := s.readContext(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	return s.store.ListCurrentConnectedChannelActivations(ctx)
}

func (s *AdmissionSnapshot) ListChannelTeardowns(ctx context.Context) ([]channelonboarding.TeardownOperation, error) {
	ctx, done, err := s.readContext(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	return s.store.ListChannelTeardowns(ctx)
}

func (s *AdmissionSnapshot) ActiveNonStandingRunBundleAvailabilities(ctx context.Context) ([]runbundle.Availability, error) {
	ctx, done, err := s.readContext(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	return s.store.ActiveNonStandingRunBundleAvailabilities(ctx)
}

func (s *AdmissionSnapshot) GetSourceArtifact(ctx context.Context, hash string) (sourceartifact.Persisted, error) {
	ctx, done, err := s.readContext(ctx)
	if err != nil {
		return sourceartifact.Persisted{}, err
	}
	defer done()
	return s.store.GetSourceArtifact(ctx, hash)
}

func (s *AdmissionSnapshot) LoadRunBundleAvailability(ctx context.Context, runID string) (runbundle.Availability, error) {
	ctx, done, err := s.readContext(ctx)
	if err != nil {
		return runbundle.Availability{}, err
	}
	defer done()
	return s.store.LoadRunBundleAvailability(ctx, runID)
}

func (s *AdmissionSnapshot) InspectSelectedForkRecovery(ctx context.Context, entry runfork.SelectedForkRecoveryEntry) (runfork.SelectedForkRecoveryInspection, error) {
	ctx, done, err := s.readContext(ctx)
	if err != nil {
		return runfork.SelectedForkRecoveryInspection{}, err
	}
	defer done()
	return s.store.InspectSelectedForkRecovery(ctx, entry)
}

func (s *AdmissionSnapshot) InspectDeliveryRecovery(ctx context.Context, source correlation.SourceArtifactFact) (deliverylifecycle.RecoveryInventory, error) {
	ctx, done, err := s.readContext(ctx)
	if err != nil {
		return deliverylifecycle.RecoveryInventory{}, err
	}
	defer done()
	return s.store.InspectDeliveryRecovery(ctx, source)
}

func (s *AdmissionSnapshot) ReadTimerObligations(ctx context.Context, scope timerobligation.Scope, observedAt time.Time) (timerobligation.Snapshot, error) {
	ctx, done, err := s.readContext(ctx)
	if err != nil {
		return timerobligation.Snapshot{}, err
	}
	defer done()
	return s.store.ReadTimerObligations(ctx, scope, observedAt)
}

func (s *AdmissionSnapshot) ReadResetInventory(ctx context.Context) (destructivereset.Inventory, error) {
	ctx, done, err := s.readContext(ctx)
	if err != nil {
		return destructivereset.Inventory{}, err
	}
	defer done()
	return s.store.ReadResetInventory(ctx)
}

func (s *AdmissionSnapshot) PendingResetOperations(ctx context.Context) ([]destructivereset.Operation, error) {
	ctx, done, err := s.readContext(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	return s.store.InspectPendingResetOperations(ctx)
}

func (s *AdmissionSnapshot) ListSelectedForkRecoveryEntries(ctx context.Context) ([]runfork.SelectedForkRecoveryEntry, error) {
	ctx, done, err := s.readContext(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	return s.store.ListSelectedForkRecoveryEntries(ctx)
}

func (s *AdmissionSnapshot) ListSelectedContractRouteRecoveryRecords(ctx context.Context) ([]manager.SelectedContractRouteRecoveryRecord, error) {
	ctx, done, err := s.readContext(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	return s.store.ListSelectedContractRouteRecoveryRecords(ctx)
}

func (s *AdmissionSnapshot) ListActiveAgentDescriptors(ctx context.Context, runID string) ([]runtimebus.ActiveAgentDescriptor, error) {
	ctx, done, err := s.readContext(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	return s.store.ListActiveAgentDescriptors(ctx, runID)
}

func (s *AdmissionSnapshot) StandingRunRestartDisposition(ctx context.Context, runID string) (runlifecycle.StandingRestartDisposition, error) {
	ctx, done, err := s.readContext(ctx)
	if err != nil {
		return runlifecycle.StandingRestartDisposition{}, err
	}
	defer done()
	return s.store.StandingRunRestartDisposition(ctx, runID)
}

func (s *AdmissionSnapshot) InspectDynamicFlowRuntimeReadinessForSource(ctx context.Context, source correlation.SourceArtifactFact) (pipeline.DynamicFlowRuntimeReadinessProjection, error) {
	ctx, done, err := s.readContext(ctx)
	if err != nil {
		return pipeline.DynamicFlowRuntimeReadinessProjection{}, err
	}
	defer done()
	projection, err := s.store.InspectDynamicFlowRuntimeReadinessForSource(ctx, source)
	if err != nil {
		return pipeline.DynamicFlowRuntimeReadinessProjection{}, err
	}
	return manager.FilterDynamicFlowRuntimeReadiness(ctx, projection, s, func(ctx context.Context, runID string) (bool, error) {
		return s.store.ObserveOrdinaryRunSource(ctx, source, runID)
	})
}

func (s *AdmissionSnapshot) LoadAgents(ctx context.Context) ([]manager.PersistedAgent, error) {
	ctx, done, err := s.readContext(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	return s.store.LoadAgents(ctx)
}

func (s *AdmissionSnapshot) ObserveOrdinaryRunSource(ctx context.Context, source correlation.SourceArtifactFact, runID string) (bool, error) {
	ctx, done, err := s.readContext(ctx)
	if err != nil {
		return false, err
	}
	defer done()
	return s.store.ObserveOrdinaryRunSource(ctx, source, runID)
}

func (s *AdmissionSnapshot) ListFlowInstanceRoutes(ctx context.Context) ([]flowidentity.RunScopedFlowInstance, error) {
	ctx, done, err := s.readContext(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	return s.store.ListFlowInstanceRoutes(ctx)
}

func (s *AdmissionSnapshot) GlobalWorkPresence(ctx context.Context) (pipelineobligation.GlobalWorkPresence, error) {
	ctx, done, err := s.readContext(ctx)
	if err != nil {
		return pipelineobligation.GlobalWorkPresence{}, err
	}
	defer done()
	owner := s.store.PipelineObligations()
	if owner == nil {
		return pipelineobligation.GlobalWorkPresence{}, errors.New("selected pipeline obligation reader is required")
	}
	return owner.GlobalWorkPresence(ctx)
}
