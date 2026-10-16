package operatorsurface

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/platform"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/timerobligation"
	"github.com/division-sh/swarm/internal/sourceartifact"
	privatedelivery "github.com/division-sh/swarm/internal/store/internal/backend/delivery"
)

func loadRunOperationalSource(ctx context.Context, sources sourceartifact.Reader, header operatorread.RunHeader) (semanticview.Source, error) {
	stored, err := sources.GetSourceArtifact(ctx, header.BundleHash)
	if err != nil {
		return nil, err
	}
	if stored.BundleHash != header.BundleHash {
		return nil, fmt.Errorf("operational source contradicts exact run bundle")
	}
	artifact, err := stored.Decode()
	if err != nil {
		return nil, err
	}
	bundle, err := contracts.LoadWorkflowContractBundleFromArtifact("", artifact, platform.PlatformSpecDisplayPath, contracts.WorkflowContractLoadOptions{AdmitPackInventory: packadmission.AdmitInventory})
	if err != nil {
		return nil, err
	}
	return semanticview.Wrap(bundle), nil
}

func loadRunOperationalEvidence(ctx context.Context, q eventReadQueryer, adapter *privatedelivery.Adapter, sources sourceartifact.Reader, instances PipelineObligationSource, timers TimerObligationSource, header operatorread.RunHeader, work operatorread.RunTestQuiescence, observedAt time.Time) (*operatorread.RunOperationalFacts, error) {
	facts := &operatorread.RunOperationalFacts{ObservedAt: observedAt}
	// These existing summaries are run-scoped. Do not invent an instance owner
	// for work whose canonical readback does not provide one.
	facts.UnscopedWork.Pipeline = work.UnsettledPipelineEvents
	facts.UnscopedWork.FanOut = work.FanOutOwed + work.FanOutUnsettled
	facts.UnscopedWork.Barriers = work.FanOutBarriers
	var progress any
	if err := q.QueryRowContext(ctx, `SELECT MAX(created_at) FROM events WHERE run_id=$1 AND event_name <> 'platform.run_stalled'`, header.RunID).Scan(&progress); err != nil {
		return nil, err
	}
	var err error
	facts.LastProgressAt, _, err = StoreTimeValue(progress)
	if err != nil {
		return nil, err
	}
	source, err := loadRunOperationalSource(ctx, sources, header)
	if errors.Is(err, sourceartifact.ErrNotFound) {
		return facts, nil
	}
	if err != nil {
		return nil, err
	}
	byPath, err := loadRunOperationalInstances(ctx, instances, source, header, facts)
	if err != nil {
		return nil, err
	}
	deliveries, err := adapter.RunOperationalEvidence(ctx, q, header.RunID)
	if err != nil {
		return nil, err
	}
	for _, delivery := range deliveries {
		path := delivery.FlowInstance
		work := &facts.UnscopedWork
		if i, ok := byPath[path]; ok {
			work = &facts.Instances[i].Work
		}
		switch delivery.Status {
		case deliverylifecycle.StatusPending, deliverylifecycle.StatusInProgress:
			work.Deliveries++
		case deliverylifecycle.StatusFailed:
			work.Retries++
		}
	}
	timerScope, err := timerobligation.Run(header.RunID)
	if err != nil {
		return nil, err
	}
	timerFacts, err := timers.ReadTimerObligations(ctx, timerScope, observedAt)
	if err != nil {
		return nil, err
	}
	for _, timer := range timerFacts.Activations {
		if timer.Status != "active" {
			continue
		}
		if i, ok := byPath[timer.FlowInstance]; ok {
			facts.Instances[i].Work.Timers++
		} else {
			facts.UnscopedWork.Timers++
		}
	}
	if err := loadRunOperationalWaits(ctx, q, header.RunID, byPath, facts); err != nil {
		return nil, err
	}
	if err := loadRunOperationalSessions(ctx, q, header.RunID, byPath, facts, observedAt); err != nil {
		return nil, err
	}
	if err := loadRunTurnAttributions(ctx, q, adapter == operatorPostgresDelivery, header.RunID, instances, deliveries, byPath, facts); err != nil {
		return nil, err
	}
	facts.Complete = true
	return facts, nil
}

func loadRunOperationalInstances(ctx context.Context, instances PipelineObligationSource, source semanticview.Source, header operatorread.RunHeader, facts *operatorread.RunOperationalFacts) (map[string]int, error) {
	fact, err := correlation.DecodeSourceArtifactFact(header.BundleHash)
	if err != nil {
		return nil, err
	}
	flowIDs := []string{"."}
	for flowID := range source.FlowSchemaEntries() {
		if flowID != "." {
			flowIDs = append(flowIDs, flowID)
		}
	}
	slices.Sort(flowIDs)
	scope, err := pipeline.NewFlowInstanceLookupScope(source, fact, header.RunID, flowIDs, nil)
	if err != nil {
		return nil, err
	}
	observations, err := instances.ListFlowInstances(ctx, scope)
	if err != nil {
		return nil, err
	}
	byPath := make(map[string]int, len(observations))
	for _, observation := range observations {
		instance, err := observation.WorkflowInstance()
		if err != nil {
			return nil, err
		}
		graph, known := semanticview.WorkflowStageTopology(source, observation.Selection().FlowID())
		if !known {
			return nil, fmt.Errorf("constructed instance has no pinned stage catalog")
		}
		stage, err := graph.ResolveStoredStage(instance.CurrentState)
		if err != nil {
			return nil, err
		}
		path := observation.Owner().Route.InstancePath
		byPath[path] = len(facts.Instances)
		facts.Instances = append(facts.Instances, operatorread.RunInstanceFacts{
			FlowInstance: path, Stage: instance.CurrentState, Revision: instance.Revision,
			Final: stage.IsFinal(), Complete: true, Retired: instance.Status != "active",
		})
		if !instance.StageDefined {
			facts.Instances[len(facts.Instances)-1].Stage = ""
		}
	}
	return byPath, nil
}

func loadRunOperationalSessions(ctx context.Context, q eventReadQueryer, runID string, byPath map[string]int, facts *operatorread.RunOperationalFacts, observedAt time.Time) (err error) {
	rows, err := q.QueryContext(ctx, `SELECT flow_instance,agent_route_presence,lease_expires_at FROM agent_sessions WHERE run_id=$1 AND status='active' AND lease_holder IS NOT NULL AND lease_expires_at IS NOT NULL`, runID)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, rows.Close()) }()
	for rows.Next() {
		var path, presence string
		var raw any
		if err := rows.Scan(&path, &presence, &raw); err != nil {
			return err
		}
		expires, found, err := StoreTimeValue(raw)
		if err != nil {
			return err
		}
		if !found || !expires.After(observedAt) {
			continue
		}
		if presence == string(agentidentity.RouteRoot) {
			path = runID
		}
		if i, ok := byPath[path]; ok {
			facts.Instances[i].Work.Sessions++
		} else {
			facts.UnscopedWork.Sessions++
		}
	}
	return rows.Err()
}

func loadRunOperationalWaits(ctx context.Context, q eventReadQueryer, runID string, byPath map[string]int, facts *operatorread.RunOperationalFacts) (err error) {
	rows, err := q.QueryContext(ctx, `SELECT COALESCE(requester_flow_instance,''),COUNT(*) FROM human_task_continuations WHERE run_id=$1 AND state IN ('pending','decision_committed') GROUP BY requester_flow_instance`, runID)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, rows.Close()) }()
	for rows.Next() {
		var path string
		var count int
		if err := rows.Scan(&path, &count); err != nil {
			return err
		}
		if i, ok := byPath[path]; ok {
			facts.Instances[i].Work.Await += count
		} else {
			facts.UnscopedWork.Await += count
		}
	}
	return rows.Err()
}
