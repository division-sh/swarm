package fanoutorigin

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
)

// ValidateCommitted proves that readback has exactly one committed ordinal
// owner, not merely a plausible JSON origin attached to an event.
func ValidateCommitted(ctx context.Context, q Queryer, postgres bool, event events.Event) error {
	origin, inherited := event.InheritedFanOutOrigin()
	if !inherited {
		return nil
	}
	var runID, deliveryID, flow, family, path, bundle, digest string
	var ordinal, owners, cursor, cardinality int
	var capsuleRaw []byte
	err := q.QueryRowContext(ctx, `SELECT CAST(o.run_id AS TEXT), CAST(o.triggering_delivery_id AS TEXT),
		o.flow_path, o.declaration_family, o.semantic_path, o.ordinal, i.bundle_hash, i.semantic_digest, i.capsule, i.cursor, i.cardinality,
		COUNT(*) OVER ()
		FROM fan_out_outcomes o JOIN fan_out_intents i
		ON i.run_id=o.run_id AND i.triggering_delivery_id=o.triggering_delivery_id AND i.flow_path=o.flow_path
		AND i.declaration_family=o.declaration_family AND i.semantic_path=o.semantic_path
		WHERE o.event_id=$1 AND o.outcome_kind='committed'`, event.ID()).Scan(
		&runID, &deliveryID, &flow, &family, &path, &ordinal, &bundle, &digest, &capsuleRaw, &cursor, &cardinality, &owners)
	if err != nil {
		return fmt.Errorf("inherited fan-out event requires its committed ordinal owner: %w", err)
	}
	if owners != 1 || ordinal < 0 || ordinal >= cursor || cursor > cardinality {
		return fmt.Errorf("inherited fan-out event origin disagrees with its unique committed ordinal owner")
	}
	var capsule fanoutobligation.Capsule
	if err := json.Unmarshal(capsuleRaw, &capsule); err != nil {
		return err
	}
	element := contracts.FanOutElementRef{FlowPath: flow, Family: family, SemanticPath: path}
	key := fanoutobligation.IntentKey{RunID: runID, TriggeringDeliveryID: deliveryID, ElementRef: element}
	plan := contracts.FanOutPlanRef{ElementRef: element, BundleHash: bundle, SemanticDigest: digest}
	if err := fanoutobligation.ValidateCommittedOrdinalEvent(key, plan, capsule, ordinal, event); err != nil {
		return err
	}
	var triggerRunID, task, mode string
	if err := q.QueryRowContext(ctx, `SELECT CAST(run_id AS TEXT), COALESCE(task_id,''), execution_mode FROM events WHERE event_id=$1`, origin.TriggerEventID()).Scan(&triggerRunID, &task, &mode); err != nil {
		return fmt.Errorf("inherited fan-out origin trigger: %w", err)
	}
	if triggerRunID != origin.SourceRunID() || task != event.TaskID() || mode != string(event.ExecutionMode()) {
		return fmt.Errorf("inherited fan-out origin trigger contradicts lineage")
	}
	inLineage, err := SourceRunInLineage(ctx, q, postgres, event.RunID(), origin.SourceRunID())
	if err != nil {
		return err
	}
	if !inLineage {
		return fmt.Errorf("inherited fan-out origin is outside the destination fork lineage")
	}
	return nil
}
