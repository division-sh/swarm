package pipelinepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/platform"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/internal/backend/generationauthority"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle"
)

func (s *PipelinePostgresOwner) ReadRetainedAgentInstanceOwnershipTx(ctx context.Context, tx *sql.Tx, actor agentidentity.Identity, hash string) (events.InstanceOwnership, error) {
	source, fact, err := retainedTurnSourceTx(ctx, tx, actor, hash, false)
	if err != nil {
		return events.InstanceOwnership{}, err
	}
	return s.ReadAgentInstanceOwnershipTx(ctx, tx, source, fact, actor)
}

func (s *PipelineSQLiteOwner) ReadRetainedAgentInstanceOwnershipTx(ctx context.Context, tx *sql.Tx, actor agentidentity.Identity, hash string) (events.InstanceOwnership, error) {
	source, fact, err := retainedTurnSourceTx(ctx, tx, actor, hash, true)
	if err != nil {
		return events.InstanceOwnership{}, err
	}
	return s.ReadAgentInstanceOwnershipTx(ctx, tx, source, fact, actor)
}

// The outer owner already holds every actor lock. This is retained-source
// observation, never a demand that an obsolete execution token become current.
func retainedTurnSourceTx(ctx context.Context, tx *sql.Tx, actor agentidentity.Identity, hash string, sqlite bool) (semanticview.Source, correlation.SourceArtifactFact, error) {
	if err := generationauthority.FenceMutation(ctx, tx, sqlite); err != nil {
		return nil, correlation.SourceArtifactFact{}, err
	}
	if _, _, err := generationauthority.ReadSourceSetTx(ctx, tx, sqlite, true); err != nil {
		return nil, correlation.SourceArtifactFact{}, err
	}
	query := `SELECT bundle_hash FROM runs WHERE run_id=$1`
	if !sqlite {
		query += ` FOR SHARE`
	}
	var current string
	if err := tx.QueryRowContext(ctx, query, actor.RunID).Scan(&current); err != nil {
		return nil, correlation.SourceArtifactFact{}, err
	}
	if current != hash {
		return nil, correlation.SourceArtifactFact{}, fmt.Errorf("retained turn crosses its exact native run source")
	}
	rooted, err := storerunlifecycle.LoadRunRootedSourceTx(ctx, tx, actor.RunID, !sqlite)
	if err != nil {
		return nil, correlation.SourceArtifactFact{}, err
	}
	spec, err := platform.MaterializePlatformSpecFile()
	if err != nil {
		return nil, correlation.SourceArtifactFact{}, err
	}
	bundle, err := contracts.LoadWorkflowContractBundleFromRootedSource("", rooted, spec, contracts.WorkflowContractLoadOptions{AdmitPackInventory: packadmission.AdmitInventory})
	if err != nil {
		return nil, correlation.SourceArtifactFact{}, err
	}
	fact, err := correlation.DecodeSourceArtifactFact(hash)
	return semanticview.Wrap(bundle), fact, err
}

// The admission owner holds the actor/source/run prefix for this transaction.
// The returned observation is data; it cannot acquire or renew execution.
func (s *PipelinePostgresOwner) ReadAgentInstanceOwnershipTx(ctx context.Context, tx *sql.Tx, source semanticview.Source, fact correlation.SourceArtifactFact, actor agentidentity.Identity) (events.InstanceOwnership, error) {
	run, err := s.LoadSnapshotTx(ctx, tx, actor.RunID, false)
	if err != nil {
		return events.InstanceOwnership{}, err
	}
	snapshot := &flowInstanceIndexSnapshot{tx: tx, postgres: true, source: source, run: run, historical: s.historicalInstances, observed: make(map[string]pipeline.FlowInstanceObservation)}
	return snapshot.readAgentOwnership(ctx, fact, actor)
}

func (s *PipelineSQLiteOwner) ReadAgentInstanceOwnershipTx(ctx context.Context, tx *sql.Tx, source semanticview.Source, fact correlation.SourceArtifactFact, actor agentidentity.Identity) (events.InstanceOwnership, error) {
	run, err := s.LoadSnapshotTx(ctx, tx, actor.RunID)
	if err != nil {
		return events.InstanceOwnership{}, err
	}
	snapshot := &flowInstanceIndexSnapshot{tx: tx, source: source, run: run, historical: s.historicalInstances, observed: make(map[string]pipeline.FlowInstanceObservation)}
	return snapshot.readAgentOwnership(ctx, fact, actor)
}

func (s *flowInstanceIndexSnapshot) readAgentOwnership(ctx context.Context, fact correlation.SourceArtifactFact, actor agentidentity.Identity) (ownership events.InstanceOwnership, err error) {
	if actor.Validate() != nil || actor != actor.Normalize() || actor.RunID != s.run.RunID {
		return ownership, fmt.Errorf("turn ownership requires its exact admitted actor")
	}
	if err := validateFlowInstanceIndexSnapshot(s.source, fact.BundleHash(), s.run, s.historical); err != nil {
		return ownership, err
	}
	scope, instance, path, err := actor.ExecutionCoordinates()
	if err != nil {
		return ownership, err
	}
	if actor.Route.Presence == agentidentity.RouteRoot {
		scope = flowidentity.ScopeKey(s.source, semanticview.RootExecutionFlowID(s.source))
	}
	owner := flowidentity.RunScopedFlowInstance{RunID: actor.RunID, Route: flowidentity.StoredRoute(scope, instance, path)}
	// Runtime-owned actors can have no declared instance coordinate. Establish
	// native absence, including companions, before attempting declared decoding.
	headers, err := s.headers(ctx, `fi.instance_path=$2 LIMIT 2`, s.run.RunID, path)
	if err != nil {
		return ownership, err
	}
	if len(headers) == 0 {
		if err := s.requireAbsentCoordinate(ctx, path); err != nil {
			return ownership, err
		}
		return events.InstanceOwnership{RunOwned: true}, nil
	}
	request, err := pipeline.NewExactFlowInstanceLookup(s.source, fact, owner)
	if err != nil {
		return ownership, err
	}
	err = s.withHistoricalReconstruction(ctx, func(ctx context.Context) error {
		observed, found, err := s.lookup(ctx, request)
		if err != nil {
			return err
		}
		if !found {
			ownership.RunOwned = true
			return nil
		}
		_, complete := observed.Readiness()
		if !complete {
			return fmt.Errorf("turn ownership requires its complete source-bound actor roster")
		}
		constructed := observed.Identity()
		if observed.Owner().RunID != actor.RunID {
			return fmt.Errorf("turn ownership construction crosses its exact run")
		}
		if err := constructed.ValidateConstruction(s.source, actor.RunID); err != nil {
			return err
		}
		if bundle, available := semanticview.Bundle(s.source); !available || bundle == nil {
			return fmt.Errorf("turn ownership requires its complete declaration inventory")
		}
		flow, found := s.source.FlowScopeByID(constructed.TemplateID)
		if !found {
			return fmt.Errorf("turn ownership requires its exact declaration scope")
		}
		route, err := constructed.Route().AgentIdentityRoute()
		if err != nil {
			return err
		}
		member := false
		declared := 0
		for _, declaration := range semanticview.AgentDeclarations(s.source) {
			if strings.TrimSpace(declaration.OwnerFlowID) != strings.TrimSpace(flow.ID) {
				continue
			}
			declared++
			plan, err := semanticview.ScopedAgentNamePlan(s.source, declaration)
			if err != nil {
				return err
			}
			name, err := plan.Materialize()
			if err != nil {
				return err
			}
			member = member || name == actor.Name && route == actor.Route
		}
		if declared != len(flow.AgentURIs) {
			return fmt.Errorf("turn ownership declaration inventory is incomplete")
		}
		if member {
			instance, err := observed.WorkflowInstance()
			if err != nil {
				return err
			}
			ownership.Life = &events.InstanceLife{RunID: actor.RunID, FlowInstance: observed.Identity().InstancePath, Life: instance.Life}
			return ownership.Validate()
		}
		ownership.RunOwned = true
		return nil
	})
	return ownership, err
}
