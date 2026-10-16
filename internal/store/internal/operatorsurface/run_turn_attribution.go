package operatorsurface

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/core/toolcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/toolidentity"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	storecapability "github.com/division-sh/swarm/internal/store/internal/backend/managedcapability"
)

func loadRunTurnAttributions(ctx context.Context, q eventReadQueryer, postgres bool, runID string, publications PipelineObligationSource, deliveries []deliverylifecycle.Snapshot, byPath map[string]int, facts *operatorread.RunOperationalFacts) (err error) {
	byID := make(map[string]deliverylifecycle.Snapshot, len(deliveries))
	for _, delivery := range deliveries {
		byID[delivery.DeliveryID] = delivery
	}
	// The author-activity sequence is the existing commit-order witness. UUID
	// order and provider completion timestamps cannot choose a last business turn.
	rows, err := q.QueryContext(ctx, `SELECT source_identity FROM author_activity_occurrences
		WHERE run_id=$1 AND kind='delivery.lifecycle' AND source_owner='event_deliveries'
		AND transition IN ('delivered','dead_letter','canceled') ORDER BY sequence DESC`, runID)
	if err != nil {
		return err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return errors.Join(err, rows.Close())
		}
		ids = append(ids, id)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return err
	}
	seen := map[string]bool{}
	seenDelivery := map[string]bool{}
	for _, id := range ids {
		if seenDelivery[id] {
			continue
		}
		seenDelivery[id] = true
		delivery, found := byID[id]
		if !found {
			return fmt.Errorf("settled origin activity has no owning delivery")
		}
		path := delivery.Route.Target.Route().FlowInstance
		if delivery.SubscriberClass == deliverylifecycle.SubscriberAgent {
			_, _, path, err = delivery.Route.AgentIdentity.ExecutionCoordinates()
			if err != nil {
				return err
			}
		}
		i, found := byPath[path]
		if !found {
			continue
		}
		if delivery.SubscriberClass != deliverylifecycle.SubscriberAgent {
			seen[path] = true
			continue
		}
		attribution, err := loadDeliveryTurnAttribution(ctx, q, postgres, publications, delivery, deliveries, path)
		if err != nil {
			return err
		}
		if attribution.Historical {
			facts.HistoricalTurns = append(facts.HistoricalTurns, attribution)
		} else if !seen[path] {
			facts.Instances[i].LastTurn = &attribution
		}
		seen[path] = true
	}
	return nil
}

func loadAttributionEvent(ctx context.Context, q eventReadQueryer, postgres bool, eventID string) (eventrecord.Record, bool, error) {
	if postgres {
		return loadPostgresEventIdentity(ctx, q, eventID)
	}
	return loadSQLiteEventIdentity(ctx, q, eventID)
}

func loadDeliveryTurnAttribution(ctx context.Context, q eventReadQueryer, postgres bool, publications PipelineObligationSource, delivery deliverylifecycle.Snapshot, deliveries []deliverylifecycle.Snapshot, path string) (operatorread.RunTurnAttribution, error) {
	out := operatorread.RunTurnAttribution{
		FlowInstance: path, AgentID: delivery.SubscriberID, EventID: delivery.EventID, DeliveryID: delivery.DeliveryID,
		Emission: operatorread.RunEmissionUnknown, ProviderTurns: []string{}, GrantedEmitTools: []string{}, EmittedEvents: []operatorread.RunAcceptedEmission{},
	}
	trigger, found, err := loadAttributionEvent(ctx, q, postgres, delivery.EventID)
	if err != nil {
		return out, err
	}
	if !found || trigger.RunID != delivery.RunID {
		return out, fmt.Errorf("settled turn trigger does not belong to its exact run")
	}
	out.EventType = trigger.EventName
	var logicalTurn sql.NullString
	if err := q.QueryRowContext(ctx, `SELECT CAST(turn_id AS TEXT) FROM runtime_agent_turn_lifetimes WHERE origin_kind='delivery' AND origin_id=$1 AND run_id=$2`, delivery.DeliveryID, delivery.RunID).Scan(&logicalTurn); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return out, err
	}
	out.LogicalTurnID = logicalTurn.String
	turns, complete, nativeCLI, err := loadDeliveryCompletionEvidence(ctx, q, delivery, &out)
	if err != nil {
		return out, err
	}
	accepted, ambiguous, err := loadDeliveryAcceptedEmissions(ctx, q, postgres, publications, delivery, deliveries, turns, nativeCLI)
	if err != nil {
		return out, err
	}
	out.EmittedEvents = accepted
	if delivery.Status != deliverylifecycle.StatusDelivered {
		out.EvidenceReason = delivery.ReasonCode
		if out.EvidenceReason == "" {
			out.EvidenceReason = "delivery_failed"
		}
		if len(accepted) > 0 {
			out.Emission = operatorread.RunEmissionAccepted
		}
		return out, nil
	}
	switch {
	case len(accepted) > 0:
		out.Emission = operatorread.RunEmissionAccepted
		if ambiguous {
			out.EvidenceReason = "additional_publication_origin_ambiguous"
		}
	case ambiguous:
		out.EvidenceReason = "publication_origin_ambiguous"
	case len(turns) > 0:
		out.EvidenceReason = "proposed_publication_unproven"
	case complete:
		out.Emission = operatorread.RunEmissionNone
	default:
		if out.EvidenceReason == "" {
			out.EvidenceReason = "logical_turn_evidence_incomplete"
		}
	}
	return out, nil
}

func loadDeliveryCompletionEvidence(ctx context.Context, q eventReadQueryer, delivery deliverylifecycle.Snapshot, out *operatorread.RunTurnAttribution) (_ map[string]string, complete bool, nativeCLI map[string]bool, err error) {
	rows, err := q.QueryContext(ctx, `SELECT CAST(t.turn_id AS TEXT),CAST(t.session_id AS TEXT),
		CAST(t.run_id AS TEXT),t.agent_id,t.agent_name_owner,t.agent_name_source,t.agent_route_presence,t.flow_scope_key,t.flow_instance_id,t.flow_instance,
		CAST(t.trigger_event_id AS TEXT),t.trigger_event_type,s.surface,
		CAST(a.operation_id AS TEXT),a.adapter,a.state,COALESCE(a.completion_projection_phase,''),a.completion_continuation_active,a.evidence,o.request_fingerprint,
		(o.authority_kind='normal_agent' AND o.effect_kind='provider_turn' AND o.agent_run_id=t.run_id AND o.agent_id=t.agent_id
		AND o.agent_name_owner=t.agent_name_owner AND o.agent_name_source=t.agent_name_source AND o.agent_route_presence=t.agent_route_presence
		AND o.flow_scope_key=t.flow_scope_key AND o.flow_instance_id=t.flow_instance_id AND o.flow_instance=t.flow_instance
		AND a.origin_run_id=t.run_id AND a.origin_route_identity=$2 AND a.origin_subscriber_type='agent' AND a.origin_subscriber_id=t.agent_id
		AND a.capability_surface_id=t.capability_surface_id),
		EXISTS(SELECT 1 FROM event_delivery_attempts d WHERE d.delivery_id=a.origin_delivery_id AND d.claim_version=a.origin_claim_version AND d.claim_token=a.origin_claim_token),
		EXISTS(SELECT 1 FROM runtime_provider_attempt_drains d WHERE d.attempt_id=a.attempt_id)
		FROM runtime_external_effect_attempts a JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id
		JOIN agent_turns t ON t.completion_attempt_id=a.attempt_id
		JOIN managed_agent_capability_surfaces s ON s.surface_id=t.capability_surface_id
		WHERE a.origin_kind='delivery' AND a.origin_delivery_id=$1`, delivery.DeliveryID, events.EncodeDeliveryRouteIdentity(delivery.RouteIdentity))
	if err != nil {
		return nil, false, nil, err
	}
	defer func() { err = errors.Join(err, rows.Close()) }()
	candidates := map[string]string{}
	nativeCLI = map[string]bool{}
	complete = true
	count := 0
	activeHeads := 0
	for rows.Next() {
		var row completionAttributionRow
		if err := rows.Scan(&row.turnID, &row.sessionID, &row.identity.RunID, &row.identity.AgentID, &row.identity.NameOwner, &row.identity.NameSource,
			&row.identity.RoutePresence, &row.identity.FlowScopeKey, &row.identity.FlowInstanceID, &row.identity.FlowInstancePath,
			&row.eventID, &row.eventType, &row.surface, &row.operationID, &row.adapter, &row.state, &row.phase, &row.active, &row.evidence, &row.fingerprint, &row.paired, &row.claimKnown, &row.drained); err != nil {
			return nil, false, nil, err
		}
		count++
		if row.active {
			activeHeads++
		}
		closed, err := row.addEvidence(delivery, out, candidates, nativeCLI)
		if err != nil {
			return nil, false, nil, err
		}
		complete = complete && closed
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, false, nil, err
	}
	var physicalCount int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_external_effect_attempts a JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id WHERE a.origin_kind='delivery' AND a.origin_delivery_id=$1 AND o.effect_kind='provider_turn'`, delivery.DeliveryID).Scan(&physicalCount); err != nil {
		return nil, false, nil, err
	}
	complete = complete && count > 0 && count == physicalCount && activeHeads == 1 && out.LogicalTurnID != ""
	slices.Sort(out.ProviderTurns)
	slices.Sort(out.GrantedEmitTools)
	return candidates, complete, nativeCLI, nil
}

type completionAttributionRow struct {
	turnID, sessionID, eventID, eventType, operationID, adapter, state, phase, fingerprint string
	identity                                                                               agentidentity.StorageFields
	surface, evidence                                                                      []byte
	active, paired, claimKnown, drained                                                    bool
}

func (r completionAttributionRow) deliverySurface(delivery deliverylifecycle.Snapshot, eventType string) (managedcapabilities.Surface, error) {
	identity, err := agentidentity.FromStorageFields(r.identity)
	if err != nil {
		return managedcapabilities.Surface{}, err
	}
	equal, err := agentidentity.Equal(identity, delivery.Route.AgentIdentity)
	if err != nil || !equal || !r.paired || !r.claimKnown || r.eventID != delivery.EventID || r.eventType != eventType {
		return managedcapabilities.Surface{}, fmt.Errorf("completion evidence contradicts exact delivery origin: %w", errors.Join(err, errors.New("origin mismatch")))
	}
	surface, err := storecapability.Decode(r.surface)
	if err != nil {
		return managedcapabilities.Surface{}, err
	}
	if err := storecapability.ValidateAgentTurn(surface, identity, r.sessionID, delivery.RunID); err != nil {
		return managedcapabilities.Surface{}, err
	}
	return surface, nil
}

func (r completionAttributionRow) addEvidence(delivery deliverylifecycle.Snapshot, out *operatorread.RunTurnAttribution, candidates map[string]string, nativeCLI map[string]bool) (bool, error) {
	surface, err := r.deliverySurface(delivery, out.EventType)
	if err != nil {
		return false, err
	}
	out.ProviderTurns = append(out.ProviderTurns, r.turnID)
	for _, tool := range surface.Tools {
		if tool.Capability.Kind == toolcapabilities.KindEmit && tool.EffectiveCallable {
			if !slices.Contains(out.GrantedEmitTools, tool.Name) {
				out.GrantedEmitTools = append(out.GrantedEmitTools, tool.Name)
			}
			if r.adapter == "claude_cli" {
				nativeCLI[toolidentity.CanonicalName(tool.Name)] = true
			}
		}
	}
	if r.drained {
		out.Historical = true
		out.EvidenceReason = "historical_drained_completion"
		return false, nil
	}
	closed := r.state == string(effects.StateSettled) && r.phase == string(effects.CompletionProjectionResponseConsumed)
	if r.state != string(effects.StateSettled) || r.phase == "" {
		return closed, nil
	}
	payload, err := effects.ReadCompletionContinuationPayload(json.RawMessage(r.evidence), r.fingerprint)
	if err != nil {
		return false, err
	}
	outputs, err := llm.ReadCompletionOutputCandidates(payload, r.adapter, r.operationID, surface)
	if err != nil {
		return false, err
	}
	for _, output := range outputs {
		candidates[output.EventID] = toolidentity.CanonicalName(output.ToolName)
	}
	return closed, nil
}

func loadDeliveryAcceptedEmissions(ctx context.Context, q eventReadQueryer, postgres bool, publications PipelineObligationSource, delivery deliverylifecycle.Snapshot, deliveries []deliverylifecycle.Snapshot, candidates map[string]string, nativeCLI map[string]bool) (_ []operatorread.RunAcceptedEmission, ambiguous bool, err error) {
	matchingOrigins := countDeliveryPublicationOrigins(delivery, deliveries)
	rows, err := q.QueryContext(ctx, `SELECT CAST(event_id AS TEXT) FROM events WHERE run_id=$1 AND source_event_id=$2 AND produced_by_type='agent' AND produced_by=$3`, delivery.RunID, delivery.EventID, delivery.SubscriberID)
	if err != nil {
		return nil, false, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, false, errors.Join(err, rows.Close())
		}
		ids = append(ids, id)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, false, err
	}
	out := make([]operatorread.RunAcceptedEmission, 0)
	for _, id := range ids {
		record, found, err := loadAttributionEvent(ctx, q, postgres, id)
		if err != nil {
			return nil, false, err
		}
		if !found {
			return nil, false, fmt.Errorf("committed emission disappeared within its snapshot")
		}
		admitted, err := record.Decode()
		if err != nil {
			return nil, false, err
		}
		request, matched, uncertain, err := matchDeliveryEmissionOrigin(admitted.Event(), delivery.Route.AgentIdentity, candidates, nativeCLI, matchingOrigins)
		if err != nil {
			return nil, false, err
		}
		if !matched {
			ambiguous = ambiguous || uncertain
			continue
		}
		receipt, found, err := publications.ReadWorkflowPublicationStages(ctx, id, request.Instance)
		if err != nil {
			return nil, false, err
		}
		if !found {
			ambiguous = true
			continue
		}
		if err := validateAttributedEmissionReceipt(receipt, id, request); err != nil {
			return nil, false, err
		}
		out = append(out, operatorread.RunAcceptedEmission{EventID: id, EventType: record.EventName})
	}
	slices.SortFunc(out, func(a, b operatorread.RunAcceptedEmission) int {
		if a.EventID < b.EventID {
			return -1
		}
		if a.EventID > b.EventID {
			return 1
		}
		return 0
	})
	return out, ambiguous, nil
}

func countDeliveryPublicationOrigins(delivery deliverylifecycle.Snapshot, deliveries []deliverylifecycle.Snapshot) int {
	matching := 0
	for _, sibling := range deliveries {
		if sibling.EventID == delivery.EventID && sibling.RouteIdentity == delivery.RouteIdentity {
			matching++
		}
	}
	return matching
}

func validateAttributedEmissionReceipt(receipt pipeline.WorkflowPublicationStageEvidence, eventID string, request pipeline.WorkflowPublicationStageRequest) error {
	if err := receipt.Validate(); err != nil {
		return err
	}
	// Entityless agents still publish from a constructed header. As in the
	// publication owner, only an authored entity binding constrains its ID.
	if receipt.Acceptance.EventID() != eventID || receipt.Acceptance.Stage().Instance != request.Instance || request.EntityID != "" && receipt.Acceptance.Stage().EntityID != request.EntityID {
		return fmt.Errorf("accepted emission contradicts its immutable publication receipt")
	}
	return nil
}

func matchDeliveryEmissionOrigin(event events.Event, identity agentidentity.Identity, candidates map[string]string, nativeCLI map[string]bool, matchingOrigins int) (request pipeline.WorkflowPublicationStageRequest, matched, ambiguous bool, err error) {
	_, _, path, err := identity.ExecutionCoordinates()
	if err != nil {
		return request, false, false, err
	}
	source := event.RoutingSource()
	if identity.Route.Presence == agentidentity.RouteRoot {
		if source.Kind() != events.RoutingSourceStaticFlow || source.Route().FlowID != "." || source.Route().FlowInstance != identity.RunID {
			return request, false, false, nil
		}
	} else if source.Route().FlowInstance != path || source.Route().FlowID != identity.Route.ScopeKey {
		return request, false, false, nil
	}
	declaration, err := pinrouting.PublicationDeclarationForSourceEvent(event.Type(), source)
	if err != nil {
		return request, false, false, err
	}
	toolName := tools.EmitToolName(declaration.Local())
	if candidates[event.ID()] != toolName && (!nativeCLI[toolName] || matchingOrigins != 1) {
		return request, false, true, nil
	}
	instance := flowidentity.RunScopedFlowInstance{RunID: identity.RunID, Route: flowidentity.Route{
		ScopeKey: identity.Route.ScopeKey, InstanceID: identity.Route.InstanceID, InstancePath: identity.Route.InstancePath,
	}}
	if identity.Route.Presence == agentidentity.RouteRoot {
		instance.Route = flowidentity.Route{ScopeKey: ".", InstanceID: identity.RunID, InstancePath: identity.RunID}
	}
	request = pipeline.WorkflowPublicationStageRequest{Instance: instance, EntityID: source.Route().EntityID}
	if err := request.ValidateEvent(event); err != nil {
		return request, false, false, err
	}
	return request, true, false, nil
}
