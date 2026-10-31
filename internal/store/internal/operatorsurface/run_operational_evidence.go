package operatorsurface

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
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

// One entry per store read owner. The compiler, empty repository root, embedded
// platform/packs and canonical pack admission are fixed for this owner's lifetime.
// Only the exact retained row varies; no ambient bundle or compiler options enter.
type runOperationalSourceMemo struct {
	gate   chan struct{}
	stored sourceartifact.Persisted
	root   sourceartifact.RootAdmissionReference
	source semanticview.Source
}

type operationalRootedSourceReader interface {
	sourceartifact.Reader
	GetRootedSource(context.Context, sourceartifact.RootAdmissionReference) (sourceartifact.RootedSource, error)
}

func newRunOperationalSourceMemo() *runOperationalSourceMemo {
	return &runOperationalSourceMemo{gate: make(chan struct{}, 1)}
}

func (m *runOperationalSourceMemo) load(ctx context.Context, sources operationalRootedSourceReader, header operatorread.RunHeader, root sourceartifact.RootAdmissionReference) (semanticview.Source, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := root.Validate(); err != nil {
		return nil, err
	}
	if root.BundleHash != header.BundleHash {
		return nil, fmt.Errorf("operational source root contradicts exact run bundle")
	}
	// Always read the current retained row inside the caller's coherent report
	// snapshot. A warm memo cannot conceal deletion, corruption or reassociation.
	stored, err := sources.GetSourceArtifact(ctx, header.BundleHash)
	if err != nil {
		return nil, err
	}
	if stored.BundleHash != header.BundleHash {
		return nil, fmt.Errorf("operational source contradicts exact run bundle")
	}
	if _, err := stored.Decode(); err != nil {
		return nil, err
	}
	rooted, err := sources.GetRootedSource(ctx, root)
	if err != nil {
		return nil, err
	}
	if err := rooted.Validate(); err != nil {
		return nil, err
	}
	if rooted.Admission().Reference() != root || !bytes.Equal(rooted.Artifact().LogicalBlob(), stored.SourceBlob) {
		return nil, fmt.Errorf("operational source contradicts exact run root")
	}
	select {
	case m.gate <- struct{}{}:
		defer func() { <-m.gate }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if m.source != nil && root == m.root && stored.BundleHash == m.stored.BundleHash &&
		stored.MemberCount == m.stored.MemberCount && stored.TotalBytes == m.stored.TotalBytes &&
		stored.CreatedAt.Equal(m.stored.CreatedAt) && bytes.Equal(stored.SourceBlob, m.stored.SourceBlob) {
		return m.source, nil
	}
	bundle, err := contracts.LoadWorkflowContractBundleFromRootedSource("", rooted, platform.PlatformSpecDisplayPath, contracts.WorkflowContractLoadOptions{AdmitPackInventory: packadmission.AdmitInventory})
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.stored = stored
	m.root = root
	m.stored.SourceBlob = bytes.Clone(stored.SourceBlob)
	// This private source never escapes the read-only operational projection;
	// neither live facts nor a mutable bundle are exposed to report consumers.
	m.source = semanticview.Wrap(bundle)
	return m.source, nil
}

func loadRunOperationalEvidence(ctx context.Context, q eventReadQueryer, adapter *privatedelivery.Adapter, sources operationalRootedSourceReader, sourceMemo *runOperationalSourceMemo, instances PipelineObligationSource, timers TimerObligationSource, header operatorread.RunHeader, root sourceartifact.RootAdmissionReference, work operatorread.RunTestQuiescence, observedAt time.Time) (*operatorread.RunOperationalFacts, error) {
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
	source, err := sourceMemo.load(ctx, sources, header, root)
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
		addRunOperationalTimer(facts, byPath, timer, observedAt)
	}
	if err := loadRunOperationalWaits(ctx, q, header.RunID, byPath, facts); err != nil {
		return nil, err
	}
	if err := loadRunOperationalSessions(ctx, q, header.RunID, byPath, facts, observedAt); err != nil {
		return nil, err
	}
	if err := loadRunRefusedEmits(ctx, q, header.RunID, adapter == operatorPostgresDelivery, byPath, facts); err != nil {
		return nil, err
	}
	if err := loadRunTurnAttributions(ctx, q, adapter == operatorPostgresDelivery, header.RunID, instances, deliveries, byPath, facts); err != nil {
		return nil, err
	}
	facts.Complete = true
	return facts, nil
}

func addRunOperationalTimer(facts *operatorread.RunOperationalFacts, byPath map[string]int, timer timerobligation.Activation, observedAt time.Time) {
	if facts.NextWakeAt == nil || timer.DueAt.Before(*facts.NextWakeAt) {
		due := timer.DueAt
		facts.NextWakeAt = &due
	}
	work := &facts.UnscopedWork
	if i, ok := byPath[timer.FlowInstance]; ok {
		instance := &facts.Instances[i]
		work = &instance.Work
		if instance.NextWakeAt == nil || timer.DueAt.Before(*instance.NextWakeAt) {
			due := timer.DueAt
			instance.NextWakeAt = &due
		}
	}
	work.Timers++
	if !timer.DueAt.After(observedAt) {
		work.DueTimers++
	}
}

func loadRunRefusedEmits(ctx context.Context, q eventReadQueryer, runID string, postgres bool, byPath map[string]int, facts *operatorread.RunOperationalFacts) error {
	field := func(path string) string { return "json_extract(payload, '$.details." + path + "')" }
	if postgres {
		field = func(path string) string {
			return "payload #>> '{details," + strings.ReplaceAll(path, ".", ",") + "}'"
		}
	}
	query := fmt.Sprintf(`SELECT %s, COALESCE(NULLIF(%s, ''), %s) FROM events
		WHERE run_id=$1 AND event_name='platform.runtime_log'
		AND %s='platform.unexpected_arrival' AND %s='no_route'
		GROUP BY 1,2`, field("failure.detail.attributes.flow_instance"), field("emitted_event_id"), field("event_id"), field("failure.class"), field("failure.detail.code"))
	rows, err := q.QueryContext(ctx, query, runID)
	if err != nil {
		return fmt.Errorf("read refused emit outcomes: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var path, eventID sql.NullString
		if err := rows.Scan(&path, &eventID); err != nil {
			return err
		}
		if i, known := byPath[path.String]; known && eventID.Valid && eventID.String != "" {
			facts.Instances[i].RefusedEmits++
		}
	}
	return rows.Err()
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
