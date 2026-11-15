package pipelinepersistence

import (
	"context"
	"fmt"
	"strings"
)

const (
	replayScopeFailureFunction = "test_fail_replay_scope_after_delivery"
	replayScopeFailureTrigger  = "test_replay_scope_after_delivery"
)

func (s *PipelinePostgresOwner) InstallReplayScopeFailureAfterDeliveryForTest(ctx context.Context, invariant, reason, flowTemplate string) (func(), error) {
	if strings.TrimSpace(invariant) == "" || strings.TrimSpace(reason) == "" {
		return nil, fmt.Errorf("event failure injection requires an invariant and reason")
	}
	flowTemplate = strings.TrimSpace(flowTemplate)
	if flowTemplate == "" {
		return nil, fmt.Errorf("event failure injection requires a flow template")
	}
	quotedTemplate := strings.ReplaceAll(flowTemplate, "'", "''")
	functionSQL := fmt.Sprintf(`
		CREATE FUNCTION %s() RETURNS trigger AS $$
		BEGIN
			IF NOT EXISTS (SELECT 1 FROM events WHERE event_id = NEW.event_id) THEN
				RAISE EXCEPTION 'replay-scope failure injection reached before event persistence';
			END IF;
			IF NOT EXISTS (SELECT 1 FROM event_deliveries WHERE event_id = NEW.event_id) THEN
				RAISE EXCEPTION 'replay-scope failure injection reached before delivery persistence';
			END IF;
			IF NOT EXISTS (SELECT 1 FROM flow_instances WHERE run_id = (SELECT run_id FROM events WHERE event_id = NEW.event_id) AND flow_template = '%s') OR
			   NOT EXISTS (SELECT 1 FROM entity_state es JOIN flow_instances fi ON fi.run_id = es.run_id AND fi.instance_path = es.flow_instance WHERE fi.run_id = (SELECT run_id FROM events WHERE event_id = NEW.event_id) AND fi.flow_template = '%s') OR
			   NOT EXISTS (
				SELECT 1 FROM flow_instances fi
				JOIN workflow_instance_initial_materializations m ON m.run_id = fi.run_id AND m.instance_path = fi.instance_path AND m.entity_id = fi.entity_id
				JOIN flow_instance_runtime_readiness r ON r.run_id = fi.run_id AND r.instance_path = fi.instance_path
				WHERE fi.run_id = (SELECT run_id FROM events WHERE event_id = NEW.event_id) AND fi.flow_template = '%s'
			   ) THEN
				RAISE EXCEPTION 'replay-scope failure injection reached before lifecycle persistence';
			END IF;
			RAISE EXCEPTION 'injected committed replay-scope persistence failure';
		END;
		$$ LANGUAGE plpgsql
	`, replayScopeFailureFunction, quotedTemplate, quotedTemplate, quotedTemplate)
	if _, err := s.backend.ExecContext(ctx, functionSQL); err != nil {
		return nil, fmt.Errorf("install replay-scope failure function for %s (%s): %v", invariant, reason, err)
	}
	if _, err := s.backend.ExecContext(ctx, fmt.Sprintf(`CREATE TRIGGER %s BEFORE INSERT ON committed_replay_scopes FOR EACH ROW EXECUTE FUNCTION %s()`, replayScopeFailureTrigger, replayScopeFailureFunction)); err != nil {
		return nil, fmt.Errorf("install replay-scope failure trigger for %s (%s): %v", invariant, reason, err)
	}
	return func() {
		_, _ = s.backend.ExecContext(context.Background(), "DROP TRIGGER IF EXISTS "+replayScopeFailureTrigger+" ON committed_replay_scopes")
		_, _ = s.backend.ExecContext(context.Background(), "DROP FUNCTION IF EXISTS "+replayScopeFailureFunction+"()")
	}, nil
}

func (s *PipelineSQLiteOwner) InstallReplayScopeFailureAfterDeliveryForTest(ctx context.Context, invariant, reason, flowTemplate string) (func(), error) {
	if strings.TrimSpace(invariant) == "" || strings.TrimSpace(reason) == "" {
		return nil, fmt.Errorf("event failure injection requires an invariant and reason")
	}
	flowTemplate = strings.TrimSpace(flowTemplate)
	if flowTemplate == "" {
		return nil, fmt.Errorf("event failure injection requires a flow template")
	}
	quotedTemplate := strings.ReplaceAll(flowTemplate, "'", "''")
	triggerSQL := fmt.Sprintf(`
		CREATE TRIGGER %s BEFORE INSERT ON committed_replay_scopes BEGIN
			SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM events WHERE event_id = NEW.event_id)
				THEN RAISE(ABORT, 'replay-scope failure injection reached before event persistence') END;
			SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM event_deliveries WHERE event_id = NEW.event_id)
				THEN RAISE(ABORT, 'replay-scope failure injection reached before delivery persistence') END;
			SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM flow_instances WHERE run_id = (SELECT run_id FROM events WHERE event_id = NEW.event_id) AND flow_template = '%s') OR
				NOT EXISTS (SELECT 1 FROM entity_state es JOIN flow_instances fi ON fi.run_id = es.run_id AND fi.instance_path = es.flow_instance WHERE fi.run_id = (SELECT run_id FROM events WHERE event_id = NEW.event_id) AND fi.flow_template = '%s') OR
				NOT EXISTS (
					SELECT 1 FROM flow_instances fi
					JOIN workflow_instance_initial_materializations m ON m.run_id = fi.run_id AND m.instance_path = fi.instance_path AND m.entity_id = fi.entity_id
					JOIN flow_instance_runtime_readiness r ON r.run_id = fi.run_id AND r.instance_path = fi.instance_path
					WHERE fi.run_id = (SELECT run_id FROM events WHERE event_id = NEW.event_id) AND fi.flow_template = '%s'
				)
				THEN RAISE(ABORT, 'replay-scope failure injection reached before lifecycle persistence') END;
			SELECT RAISE(ABORT, 'injected committed replay-scope persistence failure');
		END
	`, replayScopeFailureTrigger, quotedTemplate, quotedTemplate, quotedTemplate)
	if _, err := s.backend.ExecContext(ctx, triggerSQL); err != nil {
		return nil, fmt.Errorf("install SQLite replay-scope failure trigger for %s (%s): %v", invariant, reason, err)
	}
	return func() {
		_, _ = s.backend.ExecContext(context.Background(), "DROP TRIGGER IF EXISTS "+replayScopeFailureTrigger)
	}, nil
}
