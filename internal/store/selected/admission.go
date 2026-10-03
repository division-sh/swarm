package selected

import (
	"context"
	"errors"
	"time"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/runbundle"
	"github.com/division-sh/swarm/internal/runtime/runfork"
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
	authorityInspectionPort
	ActiveNonStandingRunBundleAvailabilities(context.Context) ([]runbundle.Availability, error)
	GetSourceArtifact(context.Context, string) (sourceartifact.Persisted, error)
	InspectDeliveryRecovery(context.Context, correlation.SourceArtifactFact) (deliverylifecycle.RecoveryInventory, error)
	ReadTimerObligations(context.Context, timerobligation.Scope, time.Time) (timerobligation.Snapshot, error)
	ReadResetInventory(context.Context) (destructivereset.Inventory, error)
	ListSelectedForkRecoveryEntries(context.Context) ([]runfork.SelectedForkRecoveryEntry, error)
	ListSelectedContractRouteRecoveryRecords(context.Context) ([]manager.SelectedContractRouteRecoveryRecord, error)
	ListActiveAgentDescriptors(context.Context, string) ([]runtimebus.ActiveAgentDescriptor, error)
}

// AdmissionInspection is a read purpose, not an unactivated runtime owner.
// Its only read surfaces are scoped to one consistent inspection callback.
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
		selected, _, err = storeconstruction.OpenPostgres(req.PostgresDSN)
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

// AdmissionSnapshot cannot expose writes, raw SQL, process possession or a
// runtime projection. Passing another context cannot escape its read snapshot.
type AdmissionSnapshot struct {
	ctx   context.Context
	store admissionInspectionPort
	fresh bool
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
