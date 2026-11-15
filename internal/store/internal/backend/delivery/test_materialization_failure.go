package delivery

import (
	"context"
	"fmt"
	"strings"
)

const (
	flowMaterializationFailureFunction = "test_fail_event_delivery_after_flow_materialization"
	flowMaterializationFailureTrigger  = "test_event_delivery_after_flow_materialization"
)

// Refuse before injection unless the same transaction contains construction,
// declared fields and runtime readiness. The cleanup retains its original cut.
func (s *DeliveryPostgresOwner) InstallEventDeliveryFailureAfterFlowMaterializationForTest(ctx context.Context, invariant, reason, flowTemplate string) (func(), error) {
	if strings.TrimSpace(invariant) == "" || strings.TrimSpace(reason) == "" {
		return nil, fmt.Errorf("event delivery failure injection requires an invariant and reason")
	}
	flowTemplate = strings.TrimSpace(flowTemplate)
	if flowTemplate == "" {
		return nil, fmt.Errorf("event delivery failure injection requires a flow template")
	}
	quotedTemplate := strings.ReplaceAll(flowTemplate, "'", "''")
	functionSQL := fmt.Sprintf(`
		CREATE FUNCTION %s() RETURNS trigger AS $$
		DECLARE lifecycle_run UUID;
		DECLARE lifecycle_instance TEXT;
		BEGIN
			SELECT run_id, instance_path INTO lifecycle_run, lifecycle_instance
			FROM flow_instances
			WHERE run_id = NEW.run_id AND flow_template = '%s'
			ORDER BY created_at DESC, run_id DESC, instance_path DESC
			LIMIT 1;
			IF lifecycle_instance IS NULL THEN
				RAISE EXCEPTION 'event delivery failure injection reached before flow instance materialization';
			END IF;
			IF NOT EXISTS (SELECT 1 FROM entity_state WHERE run_id = lifecycle_run AND flow_instance = lifecycle_instance) THEN
				RAISE EXCEPTION 'event delivery failure injection reached before entity materialization';
			END IF;
			IF NOT EXISTS (
				SELECT 1 FROM flow_instances fi
				JOIN workflow_instance_initial_materializations m ON m.run_id = fi.run_id AND m.instance_path = fi.instance_path AND m.entity_id = fi.entity_id
				JOIN flow_instance_runtime_readiness r ON r.run_id = fi.run_id AND r.instance_path = fi.instance_path
				WHERE fi.run_id = lifecycle_run AND fi.instance_path = lifecycle_instance
			) THEN
				RAISE EXCEPTION 'event delivery failure injection reached before construction evidence';
			END IF;
			RAISE EXCEPTION 'injected delivery route persistence failure';
		END;
		$$ LANGUAGE plpgsql
	`, flowMaterializationFailureFunction, quotedTemplate)
	if _, err := s.backend.ExecContext(ctx, functionSQL); err != nil {
		return nil, fmt.Errorf("install event delivery failure function for %s (%s): %v", invariant, reason, err)
	}
	triggerSQL := fmt.Sprintf(`
		CREATE TRIGGER %s
		BEFORE INSERT ON event_deliveries
		FOR EACH ROW EXECUTE FUNCTION %s()
	`, flowMaterializationFailureTrigger, flowMaterializationFailureFunction)
	if _, err := s.backend.ExecContext(ctx, triggerSQL); err != nil {
		return nil, fmt.Errorf("install event delivery failure trigger for %s (%s): %v", invariant, reason, err)
	}
	return func() {
		_, _ = s.backend.ExecContext(context.Background(), "DROP TRIGGER IF EXISTS "+flowMaterializationFailureTrigger+" ON event_deliveries")
		_, _ = s.backend.ExecContext(context.Background(), "DROP FUNCTION IF EXISTS "+flowMaterializationFailureFunction+"()")
	}, nil
}

func (s *DeliverySQLiteOwner) InstallEventDeliveryFailureAfterFlowMaterializationForTest(ctx context.Context, invariant, reason, flowTemplate string) (func(), error) {
	if strings.TrimSpace(invariant) == "" || strings.TrimSpace(reason) == "" {
		return nil, fmt.Errorf("event delivery failure injection requires an invariant and reason")
	}
	flowTemplate = strings.TrimSpace(flowTemplate)
	if flowTemplate == "" {
		return nil, fmt.Errorf("event delivery failure injection requires a flow template")
	}
	quotedTemplate := strings.ReplaceAll(flowTemplate, "'", "''")
	triggerSQL := fmt.Sprintf(`
		CREATE TRIGGER %s
		BEFORE INSERT ON event_deliveries
		BEGIN
			SELECT CASE WHEN NOT EXISTS (
				SELECT 1 FROM flow_instances WHERE run_id = NEW.run_id AND flow_template = '%s'
			) THEN RAISE(ABORT, 'event delivery failure injection reached before flow instance materialization') END;
			SELECT CASE WHEN NOT EXISTS (
				SELECT 1 FROM entity_state es JOIN flow_instances fi ON fi.run_id = es.run_id AND fi.instance_path = es.flow_instance WHERE fi.run_id = NEW.run_id AND fi.flow_template = '%s'
			) THEN RAISE(ABORT, 'event delivery failure injection reached before entity materialization') END;
			SELECT CASE WHEN NOT EXISTS (
				SELECT 1 FROM flow_instances fi
				JOIN workflow_instance_initial_materializations m ON m.run_id = fi.run_id AND m.instance_path = fi.instance_path AND m.entity_id = fi.entity_id
				JOIN flow_instance_runtime_readiness r ON r.run_id = fi.run_id AND r.instance_path = fi.instance_path
				WHERE fi.run_id = NEW.run_id AND fi.flow_template = '%s'
			) THEN RAISE(ABORT, 'event delivery failure injection reached before construction evidence') END;
			SELECT RAISE(ABORT, 'injected delivery route persistence failure');
		END
	`, flowMaterializationFailureTrigger, quotedTemplate, quotedTemplate, quotedTemplate)
	if _, err := s.backend.ExecContext(ctx, triggerSQL); err != nil {
		return nil, fmt.Errorf("install SQLite event delivery failure trigger for %s (%s): %v", invariant, reason, err)
	}
	return func() {
		_, _ = s.backend.ExecContext(context.Background(), "DROP TRIGGER IF EXISTS "+flowMaterializationFailureTrigger)
	}, nil
}
