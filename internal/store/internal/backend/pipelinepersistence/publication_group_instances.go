package pipelinepersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"slices"

	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type flowInstancePlanningRead struct {
	lookup       *pipeline.FlowInstanceLookupRequest
	scope        *pipeline.FlowInstanceLookupScope
	observations []pipeline.FlowInstanceObservation
}

// Only detached requests/results survive preparation. The committing owner
// repeats these canonical reads under its existing mutation-order fence.
type flowInstancePlanningReadSet struct {
	source semanticview.Source
	fact   correlation.SourceArtifactFact
	runID  string
	reads  []flowInstancePlanningRead
}

type groupInstancePlanningReader struct {
	snapshot *flowInstanceIndexSnapshot
	readSet  *flowInstancePlanningReadSet
}

func (r *groupInstancePlanningReader) LookupFlowInstance(ctx context.Context, request pipeline.FlowInstanceLookupRequest) (pipeline.FlowInstanceObservation, bool, error) {
	if err := r.requireScope(ctx, request.SourceFact(), request.RunID()); err != nil {
		return pipeline.FlowInstanceObservation{}, false, err
	}
	if !request.Valid() {
		return pipeline.FlowInstanceObservation{}, false, errors.New("group instance planning requires an admitted lookup")
	}
	observation, found, err := r.snapshot.lookup(ctx, request)
	if err == nil {
		read := flowInstancePlanningRead{lookup: &request}
		if found {
			read.observations = []pipeline.FlowInstanceObservation{observation}
		}
		r.readSet.record(read)
	}
	return observation, found, err
}

func (r *groupInstancePlanningReader) ListFlowInstances(ctx context.Context, scope pipeline.FlowInstanceLookupScope) ([]pipeline.FlowInstanceObservation, error) {
	if err := r.requireScope(ctx, scope.SourceFact(), scope.RunID()); err != nil {
		return nil, err
	}
	if !scope.Valid() {
		return nil, errors.New("group instance planning requires an admitted scope")
	}
	observations, err := r.snapshot.list(ctx, scope)
	if err == nil {
		r.readSet.record(flowInstancePlanningRead{scope: &scope, observations: slices.Clone(observations)})
	}
	return observations, err
}

func (r *groupInstancePlanningReader) requireScope(ctx context.Context, fact correlation.SourceArtifactFact, runID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if r.snapshot == nil || r.readSet == nil {
		return errors.New("group instance planning read lifetime has ended")
	}
	if runID != r.readSet.runID || !fact.Matches(r.readSet.fact) {
		return errors.New("group instance planning crosses its admitted source or run")
	}
	return nil
}

func (s *flowInstancePlanningReadSet) record(read flowInstancePlanningRead) {
	for _, previous := range s.reads {
		if sameFlowInstancePlanningRequest(previous, read) {
			return
		}
	}
	s.reads = append(s.reads, read)
}

func sameFlowInstancePlanningRequest(a, b flowInstancePlanningRead) bool {
	if a.lookup != nil && b.lookup != nil {
		x, y := *a.lookup, *b.lookup
		return x.FlowID() == y.FlowID() && x.ExactPath() == y.ExactPath() && x.DeclaredSelection() == y.DeclaredSelection() &&
			x.ParentIdentity() == y.ParentIdentity() && x.InstanceKey() == y.InstanceKey() && reflect.DeepEqual(x, y)
	}
	return a.scope != nil && b.scope != nil && slices.Equal(a.scope.FlowIDs(), b.scope.FlowIDs()) && slices.Equal(a.scope.Coordinates(), b.scope.Coordinates())
}

func (s *flowInstancePlanningReadSet) reobserve(ctx context.Context, snapshot *flowInstanceIndexSnapshot) error {
	for _, read := range s.reads {
		var current []pipeline.FlowInstanceObservation
		var err error
		if read.lookup != nil {
			var observation pipeline.FlowInstanceObservation
			var found bool
			observation, found, err = snapshot.lookup(ctx, *read.lookup)
			if found {
				current = []pipeline.FlowInstanceObservation{observation}
			}
		} else {
			current, err = snapshot.list(ctx, *read.scope)
		}
		// Corruption, cancellation and independent read failures keep their
		// original classification; only a valid changed view is retryable.
		if err != nil {
			return err
		}
		equal, err := sameFlowInstancePlanningObservations(read.observations, current)
		if err != nil {
			return err
		}
		if !equal {
			return failures.New(failures.ClassDependencyUnavailable, "flow_instance_planning_stale", "fan-out-publication", "reobserve_instances", map[string]any{"run_id": s.runID})
		}
	}
	return nil
}

func sameFlowInstancePlanningObservations(a, b []pipeline.FlowInstanceObservation) (bool, error) {
	if len(a) != len(b) {
		return false, nil
	}
	for i := range a {
		if a[i].Identity() != b[i].Identity() || a[i].RunState() != b[i].RunState() || !a[i].SourceFact().Matches(b[i].SourceFact()) {
			return false, nil
		}
		x, err := captureFlowInstancePlanningDependencies(a[i])
		if err != nil {
			return false, err
		}
		y, err := captureFlowInstancePlanningDependencies(b[i])
		if err != nil {
			return false, err
		}
		if !reflect.DeepEqual(x, y) {
			return false, nil
		}
	}
	return true, nil
}

type flowInstancePlanningDependencies struct {
	instance                               pipeline.WorkflowInstance
	native                                 pipeline.FlowConstructionPublicationEvidence
	historical                             pipeline.HistoricalFlowInstanceConstruction
	readiness                              pipeline.DynamicFlowRuntimeReadiness
	nativeSet, historicalSet, readinessSet bool
}

func captureFlowInstancePlanningDependencies(observation pipeline.FlowInstanceObservation) (flowInstancePlanningDependencies, error) {
	var out flowInstancePlanningDependencies
	var err error
	instance, err := observation.WorkflowInstance()
	if err != nil {
		return out, err
	}
	// Selection reads identity, availability and captured construction, not
	// live business fields, workflow counters or settlement bookkeeping. The
	// canonical re-read still validates the complete header and companions.
	out.instance = pipeline.WorkflowInstance{
		WorkflowName: instance.WorkflowName, WorkflowVersion: instance.WorkflowVersion,
		EntityType: instance.EntityType, Mode: instance.Mode, InstanceKey: instance.InstanceKey,
		Status: instance.Status, TerminatedAt: instance.TerminatedAt,
		CurrentState: instance.CurrentState, StageDefined: instance.StageDefined,
	}
	out.native, out.nativeSet, err = observation.NativeConstruction()
	if err != nil {
		return out, err
	}
	out.historical, out.historicalSet = observation.HistoricalConstruction()
	out.readiness, out.readinessSet = observation.Readiness()
	return out, nil
}

func (g *publicationGroup) WithFlowInstancePlanningSnapshot(ctx context.Context, source semanticview.Source, prepare func(context.Context, pipeline.FlowInstanceIndexReader) error) error {
	g.mu.Lock()
	if g.closed || g.sealed || g.planningActive || prepare == nil {
		g.mu.Unlock()
		return errors.New("publication group instance planning requires one open preparation")
	}
	g.planningActive = true
	g.mu.Unlock()
	defer func() {
		g.mu.Lock()
		g.planningActive = false
		g.mu.Unlock()
	}()
	reads := &flowInstancePlanningReadSet{source: source, runID: g.claim.Key.RunID}
	fact, found := correlation.SourceArtifactFactFromContext(ctx)
	if !found {
		return errors.New("publication group instance planning requires its admitted source")
	}
	reads.fact = fact
	operation := func(ctx context.Context, tx *sql.Tx) error {
		return g.withInstancePlanningSnapshot(ctx, tx, reads, func(snapshot *flowInstanceIndexSnapshot) error {
			reader := &groupInstancePlanningReader{snapshot: snapshot, readSet: reads}
			defer func() { reader.snapshot, reader.readSet = nil, nil }()
			return prepare(ctx, reader)
		})
	}
	var err error
	if g.postgres != nil {
		err = g.postgres.backend.RunReadTransaction(ctx, operation)
	} else {
		err = g.sqlite.backend.RunReadTransaction(ctx, operation)
	}
	if err != nil {
		return err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed || g.sealed {
		return errors.New("publication group closed during instance planning")
	}
	if len(g.planningReads) != 0 {
		original := g.planningReads[0]
		if original.runID != reads.runID || !original.fact.Matches(reads.fact) || original.source.WorkflowVersion() != reads.source.WorkflowVersion() {
			return errors.New("publication group preparation changed its admitted source or run")
		}
	}
	g.planningReads = append(g.planningReads, reads)
	return nil
}

func (g *publicationGroup) withInstancePlanningSnapshot(ctx context.Context, tx *sql.Tx, reads *flowInstancePlanningReadSet, read func(*flowInstanceIndexSnapshot) error) error {
	snapshot := flowInstanceIndexSnapshot{tx: tx, source: reads.source, postgres: g.postgres != nil, observed: make(map[string]pipeline.FlowInstanceObservation)}
	var err error
	if g.postgres != nil {
		snapshot.run, err = g.postgres.LoadSnapshotTx(ctx, tx, reads.runID, false)
		snapshot.historical = g.postgres.historicalInstances
	} else {
		snapshot.run, err = g.sqlite.LoadSnapshotTx(ctx, tx, reads.runID)
		snapshot.historical = g.sqlite.historicalInstances
	}
	if err != nil {
		return err
	}
	if err := validateFlowInstanceIndexSnapshot(reads.source, reads.fact.BundleHash(), snapshot.run, snapshot.historical); err != nil {
		return fmt.Errorf("admit publication group instance snapshot: %w", err)
	}
	return read(&snapshot)
}

func (g *publicationGroup) reobserveInstances(ctx context.Context, tx *sql.Tx) error {
	if len(g.planningReads) == 0 {
		return nil
	}
	return g.withInstancePlanningSnapshot(ctx, tx, g.planningReads[0], func(snapshot *flowInstanceIndexSnapshot) error {
		for _, reads := range g.planningReads {
			if err := reads.reobserve(ctx, snapshot); err != nil {
				return err
			}
		}
		return nil
	})
}
