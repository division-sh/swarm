package pipelinepersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// The historical owner selects provenance from durable lineage and a fixed cut,
// before the native reader checks any constructor or readiness companions.
type HistoricalFlowInstanceReader interface {
	ReadHistoricalFlowInstanceConstructionTx(context.Context, *sql.Tx, semanticview.Source, runlifecycle.Snapshot, pipeline.WorkflowInstance) (pipeline.HistoricalFlowInstanceConstruction, bool, error)
}

func (s *PipelinePostgresOwner) BindHistoricalFlowInstanceReader(owner HistoricalFlowInstanceReader) error {
	if s == nil || owner == nil || s.historicalInstances != nil {
		return fmt.Errorf("PostgreSQL historical instance reader requires one binding")
	}
	s.historicalInstances = owner
	return nil
}

func (s *PipelineSQLiteOwner) BindHistoricalFlowInstanceReader(owner HistoricalFlowInstanceReader) error {
	if s == nil || owner == nil || s.historicalInstances != nil {
		return fmt.Errorf("SQLite historical instance reader requires one binding")
	}
	s.historicalInstances = owner
	return nil
}

func (s *PipelinePostgresOwner) LookupFlowInstance(ctx context.Context, request pipeline.FlowInstanceLookupRequest) (pipeline.FlowInstanceObservation, bool, error) {
	if s == nil || s.backend == nil || !request.Valid() {
		return pipeline.FlowInstanceObservation{}, false, fmt.Errorf("PostgreSQL instance lookup requires its admitted request and owner")
	}
	if err := s.requireCurrent(); err != nil {
		return pipeline.FlowInstanceObservation{}, false, err
	}
	var observation pipeline.FlowInstanceObservation
	var found bool
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		run, err := s.LoadSnapshotTx(ctx, tx, request.RunID(), false)
		if err != nil {
			return err
		}
		snapshot, err := newFlowInstanceIndexSnapshot(ctx, tx, true, request.Source(), request.SourceFact().BundleHash(), run, s.historicalInstances)
		if err != nil {
			return err
		}
		observation, found, err = snapshot.lookup(ctx, request)
		return err
	})
	if err != nil {
		return pipeline.FlowInstanceObservation{}, false, err
	}
	return observation, found, nil
}

func (s *PipelineSQLiteOwner) LookupFlowInstance(ctx context.Context, request pipeline.FlowInstanceLookupRequest) (pipeline.FlowInstanceObservation, bool, error) {
	if s == nil || s.backend == nil || !request.Valid() {
		return pipeline.FlowInstanceObservation{}, false, fmt.Errorf("SQLite instance lookup requires its admitted request and owner")
	}
	if err := s.requireCurrent(); err != nil {
		return pipeline.FlowInstanceObservation{}, false, err
	}
	var observation pipeline.FlowInstanceObservation
	var found bool
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		run, err := s.LoadSnapshotTx(ctx, tx, request.RunID())
		if err != nil {
			return err
		}
		snapshot, err := newFlowInstanceIndexSnapshot(ctx, tx, false, request.Source(), request.SourceFact().BundleHash(), run, s.historicalInstances)
		if err != nil {
			return err
		}
		observation, found, err = snapshot.lookup(ctx, request)
		return err
	})
	if err != nil {
		return pipeline.FlowInstanceObservation{}, false, err
	}
	return observation, found, nil
}

func (s *PipelinePostgresOwner) ListFlowInstances(ctx context.Context, scope pipeline.FlowInstanceLookupScope) ([]pipeline.FlowInstanceObservation, error) {
	if s == nil || s.backend == nil || !scope.Valid() {
		return nil, fmt.Errorf("PostgreSQL instance inventory requires its admitted scope and owner")
	}
	if err := s.requireCurrent(); err != nil {
		return nil, err
	}
	var observations []pipeline.FlowInstanceObservation
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		run, err := s.LoadSnapshotTx(ctx, tx, scope.RunID(), false)
		if err != nil {
			return err
		}
		snapshot, err := newFlowInstanceIndexSnapshot(ctx, tx, true, scope.Source(), scope.SourceFact().BundleHash(), run, s.historicalInstances)
		if err != nil {
			return err
		}
		observations, err = snapshot.list(ctx, scope)
		return err
	})
	if err != nil {
		return nil, err
	}
	return observations, nil
}

func (s *PipelineSQLiteOwner) ListFlowInstances(ctx context.Context, scope pipeline.FlowInstanceLookupScope) ([]pipeline.FlowInstanceObservation, error) {
	if s == nil || s.backend == nil || !scope.Valid() {
		return nil, fmt.Errorf("SQLite instance inventory requires its admitted scope and owner")
	}
	if err := s.requireCurrent(); err != nil {
		return nil, err
	}
	var observations []pipeline.FlowInstanceObservation
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		run, err := s.LoadSnapshotTx(ctx, tx, scope.RunID())
		if err != nil {
			return err
		}
		snapshot, err := newFlowInstanceIndexSnapshot(ctx, tx, false, scope.Source(), scope.SourceFact().BundleHash(), run, s.historicalInstances)
		if err != nil {
			return err
		}
		observations, err = snapshot.list(ctx, scope)
		return err
	})
	if err != nil {
		return nil, err
	}
	return observations, nil
}

// This memo belongs to one repeatable read, never to a process or a later plan.
type flowInstanceIndexSnapshot struct {
	tx         *sql.Tx
	postgres   bool
	source     semanticview.Source
	run        runlifecycle.Snapshot
	revision   int64
	historical HistoricalFlowInstanceReader
	observed   map[string]pipeline.FlowInstanceObservation
}

func newFlowInstanceIndexSnapshot(ctx context.Context, tx *sql.Tx, postgres bool, source semanticview.Source, hash string, run runlifecycle.Snapshot, historical HistoricalFlowInstanceReader) (*flowInstanceIndexSnapshot, error) {
	if tx == nil || source == nil || historical == nil || run.Validate() != nil || run.BundleHash != hash {
		return nil, fmt.Errorf("instance snapshot requires its exact admitted source, lineage and read owner")
	}
	var revision int64
	if err := tx.QueryRowContext(ctx, `SELECT last_revision FROM run_fork_revision_heads WHERE run_id=$1`, run.RunID).Scan(&revision); err != nil {
		return nil, fmt.Errorf("read instance snapshot revision: %w", err)
	}
	if revision <= 0 {
		return nil, fmt.Errorf("instance snapshot requires a recorded revision")
	}
	return &flowInstanceIndexSnapshot{tx: tx, postgres: postgres, source: source, run: run, revision: revision, historical: historical, observed: make(map[string]pipeline.FlowInstanceObservation)}, nil
}

func (s *flowInstanceIndexSnapshot) lookup(ctx context.Context, request pipeline.FlowInstanceLookupRequest) (pipeline.FlowInstanceObservation, bool, error) {
	if request.DeclaredSelection() && request.ParentInstance() != "" {
		if err := s.requireParent(ctx, request, request.ParentIdentity()); err != nil {
			return pipeline.FlowInstanceObservation{}, false, err
		}
	}
	predicate := `fi.instance_path=$2`
	args := []any{s.run.RunID, request.ExactPath()}
	if request.ExactPath() == "" {
		predicate = `fi.flow_template=$2 AND COALESCE(fi.parent_instance,'')=$3 AND COALESCE(fi.instance_key,'')=$4`
		args = []any{s.run.RunID, request.FlowID(), request.ParentInstance(), request.InstanceKey()}
	}
	headers, err := s.headers(ctx, predicate+` LIMIT 2`, args...)
	if err != nil {
		return pipeline.FlowInstanceObservation{}, false, err
	}
	if len(headers) > 1 {
		return pipeline.FlowInstanceObservation{}, false, &pipeline.FlowInstanceConstructionCorruption{RunID: request.RunID(), FlowID: request.FlowID(), Cause: fmt.Errorf("instance selector has ambiguous stored receivers")}
	}
	if len(headers) == 0 {
		if request.DeclaredSelection() && request.ExactPath() == "" {
			if err := s.validateDeclaredAbsence(ctx, request); err != nil {
				return pipeline.FlowInstanceObservation{}, false, err
			}
		}
		if err := s.requireAbsentCoordinate(ctx, request.ExactPath()); err != nil {
			return pipeline.FlowInstanceObservation{}, false, err
		}
		return pipeline.FlowInstanceObservation{}, false, nil
	}
	observation, err := s.observe(ctx, request, headers[0])
	return observation, err == nil, err
}

func (s *flowInstanceIndexSnapshot) validateDeclaredAbsence(ctx context.Context, request pipeline.FlowInstanceLookupRequest) error {
	scope, err := pipeline.NewFlowInstanceLookupScope(s.source, request.SourceFact(), s.run.RunID, []string{request.FlowID()}, nil)
	if err != nil {
		return err
	}
	_, err = s.observeDeclaration(ctx, scope, request.FlowID())
	return err
}

func (s *flowInstanceIndexSnapshot) headers(ctx context.Context, predicate string, args ...any) ([]pipeline.WorkflowInstance, error) {
	return readFlowInstanceIndexHeaders(ctx, s.tx, s.postgres, predicate, args...)
}

func readFlowInstanceIndexHeaders(ctx context.Context, tx *sql.Tx, postgres bool, predicate string, args ...any) ([]pipeline.WorkflowInstance, error) {
	query := sqliteWorkflowInstanceSelect
	if postgres {
		query = postgresWorkflowInstanceSelect
	}
	rows, err := tx.QueryContext(ctx, query+` WHERE fi.run_id=$1 AND `+predicate, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var headers []pipeline.WorkflowInstance
	if postgres {
		headers, err = scanPostgresWorkflowInstances(rows)
	} else {
		headers, err = scanSQLiteWorkflowInstances(rows)
	}
	if err != nil {
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	return headers, nil
}

func requireFlowInstanceConstructionSelector(ctx context.Context, tx *sql.Tx, postgres bool, record pipeline.FlowInstanceActivationRecord) error {
	headers, err := readFlowInstanceIndexHeaders(ctx, tx, postgres,
		`fi.flow_template=$2 AND COALESCE(fi.parent_instance,'')=$3 AND COALESCE(fi.instance_key,'')=$4 LIMIT 2`,
		record.Identity.RunID, record.WorkflowName, record.State.ParentInstance, record.State.InstanceKey)
	if err != nil {
		return err
	}
	if len(headers) > 1 {
		return &pipeline.FlowInstanceConstructionCorruption{RunID: record.Identity.RunID, FlowID: record.WorkflowName, Cause: fmt.Errorf("construction selector has ambiguous occupancy")}
	}
	if len(headers) == 0 {
		return nil
	}
	actual := headers[0]
	if actual.StorageRef != record.Identity.Route.InstancePath || actual.EntityID != record.EntityID {
		return &pipeline.FlowInstanceActivationConflict{Owner: record.Identity, Cause: failures.New(
			failures.ClassConflictingDuplicate, "flow_instance_already_exists", "flow-instance-activation", "commit",
			map[string]any{"flow_instance": actual.StorageRef, "entity_id": actual.EntityID},
		)}
	}
	return nil
}

func (s *flowInstanceIndexSnapshot) observe(ctx context.Context, request pipeline.FlowInstanceLookupRequest, header pipeline.WorkflowInstance) (pipeline.FlowInstanceObservation, error) {
	historical, projected, err := s.historical.ReadHistoricalFlowInstanceConstructionTx(ctx, s.tx, s.source, s.run, header)
	if err != nil {
		return pipeline.FlowInstanceObservation{}, err
	}
	route := flowidentity.StoredRoute(flowidentity.ScopeKey(s.source, header.WorkflowName), header.InstanceID, header.StorageRef)
	readiness, readyFound, err := loadDynamicFlowRuntimeReadiness(ctx, s.tx, s.postgres, s.run.RunID, route, false)
	if err != nil {
		return pipeline.FlowInstanceObservation{}, err
	}
	var observation pipeline.FlowInstanceObservation
	if projected {
		var desired *pipeline.DynamicFlowRuntimeReadiness
		if readyFound {
			desired = &readiness
		}
		observation, err = pipeline.AdmitHistoricalFlowInstanceObservation(request, header, s.run, s.revision, historical, desired)
	} else {
		if !readyFound {
			return pipeline.FlowInstanceObservation{}, &pipeline.FlowInstanceConstructionCorruption{RunID: request.RunID(), FlowID: request.FlowID(), InstancePath: header.StorageRef, Cause: fmt.Errorf("native instance lacks desired attachment evidence")}
		}
		receipt, readErr := ReadFlowConstructionPublicationTx(ctx, s.tx, flowidentity.RunScopedFlowInstance{RunID: s.run.RunID, Route: route}, header.EntityID)
		if readErr != nil {
			if errors.Is(readErr, sql.ErrNoRows) {
				return pipeline.FlowInstanceObservation{}, &pipeline.FlowInstanceConstructionCorruption{RunID: request.RunID(), FlowID: request.FlowID(), InstancePath: header.StorageRef, Cause: readErr}
			}
			return pipeline.FlowInstanceObservation{}, readErr
		}
		observation, err = pipeline.AdmitNativeFlowInstanceObservation(request, header, s.run, s.revision, receipt, readiness)
	}
	if err != nil {
		return pipeline.FlowInstanceObservation{}, &pipeline.FlowInstanceConstructionCorruption{RunID: request.RunID(), FlowID: request.FlowID(), InstancePath: header.StorageRef, Cause: err}
	}
	identity := observation.Identity()
	if !identity.ParentRoute.Empty() {
		parent := flowidentity.Instance{TemplateID: identity.ParentRoute.FlowID, ScopeKey: flowidentity.ScopeKey(s.source, identity.ParentRoute.FlowID), InstancePath: identity.ParentRoute.FlowInstance, InstanceID: flowidentity.LogicalInstanceID(identity.ParentRoute.FlowInstance), EntityID: identity.ParentEntityID, HasStoredPath: true}
		if err := s.requireParent(ctx, request, parent); err != nil {
			return pipeline.FlowInstanceObservation{}, err
		}
	}
	s.observed[header.StorageRef] = observation
	return observation, nil
}

func (s *flowInstanceIndexSnapshot) requireParent(ctx context.Context, request pipeline.FlowInstanceLookupRequest, parent flowidentity.Instance) error {
	owner := flowidentity.RunScopedFlowInstance{RunID: s.run.RunID, Route: parent.Route()}
	observed, found := s.observed[parent.InstancePath]
	if !found {
		parentRequest, err := pipeline.NewExactFlowInstanceLookup(s.source, request.SourceFact(), owner)
		if err != nil {
			return err
		}
		observed, found, err = s.lookup(ctx, parentRequest)
		if err != nil {
			return err
		}
	}
	actual := observed.Identity()
	if !found || actual.TemplateID != parent.TemplateID || actual.InstancePath != parent.InstancePath || actual.EntityID != parent.EntityID {
		return fmt.Errorf("instance selector contradicts its stored structural parent")
	}
	if request.ParentIdentity() != (flowidentity.Instance{}) && actual != request.ParentIdentity() {
		return fmt.Errorf("instance selector carries a stale structural parent")
	}
	return nil
}

func (s *flowInstanceIndexSnapshot) requireAbsentCoordinate(ctx context.Context, path string) error {
	if path == "" {
		return nil
	}
	var occupied bool
	err := s.tx.QueryRowContext(ctx, `SELECT
		EXISTS(SELECT 1 FROM entity_state WHERE run_id=$1 AND flow_instance=$2) OR
		EXISTS(SELECT 1 FROM workflow_instance_initial_materializations WHERE run_id=$1 AND instance_path=$2) OR
		EXISTS(SELECT 1 FROM flow_instance_runtime_readiness WHERE run_id=$1 AND instance_path=$2)`, s.run.RunID, path).Scan(&occupied)
	if err != nil {
		return err
	}
	if occupied {
		return &pipeline.FlowInstanceConstructionCorruption{RunID: s.run.RunID, InstancePath: path, Cause: fmt.Errorf("instance coordinate has incomplete native construction")}
	}
	return nil
}

func (s *flowInstanceIndexSnapshot) list(ctx context.Context, scope pipeline.FlowInstanceLookupScope) ([]pipeline.FlowInstanceObservation, error) {
	selected := make(map[string]pipeline.FlowInstanceObservation)
	for _, flowID := range scope.FlowIDs() {
		observations, err := s.observeDeclaration(ctx, scope, flowID)
		if err != nil {
			return nil, err
		}
		for _, observation := range observations {
			selected[observation.Owner().Key()] = observation
		}
	}
	for _, owner := range scope.Coordinates() {
		request, err := pipeline.NewExactFlowInstanceLookup(s.source, scope.SourceFact(), owner)
		if err != nil {
			return nil, err
		}
		if observed, found := selected[owner.Key()]; found {
			if err := observed.ValidateSelection(request); err != nil {
				return nil, err
			}
			continue
		}
		observation, found, err := s.lookup(ctx, request)
		if err != nil {
			return nil, err
		}
		if found {
			selected[owner.Key()] = observation
		}
	}
	observations := make([]pipeline.FlowInstanceObservation, 0, len(selected))
	for _, observation := range selected {
		observations = append(observations, observation)
	}
	slices.SortFunc(observations, func(a, b pipeline.FlowInstanceObservation) int {
		return strings.Compare(a.Owner().Key(), b.Owner().Key())
	})
	return observations, nil
}

func (s *flowInstanceIndexSnapshot) observeDeclaration(ctx context.Context, scope pipeline.FlowInstanceLookupScope, flowID string) ([]pipeline.FlowInstanceObservation, error) {
	headers, err := s.headers(ctx, `fi.flow_template=$2 ORDER BY fi.instance_path`, scope.RunID(), flowID)
	if err != nil {
		return nil, err
	}
	observations := make([]pipeline.FlowInstanceObservation, 0, len(headers))
	for _, header := range headers {
		owner := flowidentity.RunScopedFlowInstance{RunID: s.run.RunID, Route: flowidentity.StoredRoute(flowidentity.ScopeKey(s.source, flowID), header.InstanceID, header.StorageRef)}
		request, err := pipeline.NewExactFlowInstanceLookup(s.source, scope.SourceFact(), owner)
		if err != nil {
			return nil, err
		}
		observation, err := s.observe(ctx, request, header)
		if err != nil {
			return nil, err
		}
		observations = append(observations, observation)
	}
	return observations, nil
}
