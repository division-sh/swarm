package apiidempotency

import (
	"context"
	"fmt"
	"strings"
)

const (
	apiCompletionFailureFunction = "test_fail_api_completion_after_publication"
	apiCompletionFailureTrigger  = "test_api_completion_after_publication"
)

func (s *PostgresOwner) InstallAPICompletionFailureAfterPublicationForTest(ctx context.Context, invariant, reason, flowTemplate string) (func(), error) {
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
		DECLARE publication_event UUID;
		BEGIN
			publication_event := NEW.resource_id::uuid;
			IF NOT EXISTS (SELECT 1 FROM events WHERE event_id = publication_event) OR
			   NOT EXISTS (SELECT 1 FROM event_deliveries WHERE event_id = publication_event) OR
			   NOT EXISTS (SELECT 1 FROM committed_replay_scopes WHERE event_id = publication_event) OR
			   NOT EXISTS (SELECT 1 FROM flow_instances WHERE run_id = (SELECT run_id FROM events WHERE event_id = publication_event) AND flow_template = '%s') OR
			   NOT EXISTS (SELECT 1 FROM entity_state es JOIN flow_instances fi ON fi.run_id = es.run_id AND fi.instance_path = es.flow_instance WHERE fi.run_id = (SELECT run_id FROM events WHERE event_id = publication_event) AND fi.flow_template = '%s') OR
			   NOT EXISTS (
				SELECT 1 FROM flow_instances fi
				JOIN workflow_instance_initial_materializations m ON m.run_id = fi.run_id AND m.instance_path = fi.instance_path AND m.entity_id = fi.entity_id
				JOIN flow_instance_runtime_readiness r ON r.run_id = fi.run_id AND r.instance_path = fi.instance_path
				WHERE fi.run_id = (SELECT run_id FROM events WHERE event_id = publication_event) AND fi.flow_template = '%s'
			   ) THEN
				RAISE EXCEPTION 'API completion failure injection reached before complete publication persistence';
			END IF;
			RAISE EXCEPTION 'injected API idempotency completion persistence failure';
		END;
		$$ LANGUAGE plpgsql
	`, apiCompletionFailureFunction, quotedTemplate, quotedTemplate, quotedTemplate)
	if _, err := s.backend.ExecContext(ctx, functionSQL); err != nil {
		return nil, fmt.Errorf("install API completion failure function for %s (%s): %v", invariant, reason, err)
	}
	if _, err := s.backend.ExecContext(ctx, fmt.Sprintf(`CREATE TRIGGER %s BEFORE INSERT ON api_idempotency FOR EACH ROW WHEN (NEW.method = 'event.publish') EXECUTE FUNCTION %s()`, apiCompletionFailureTrigger, apiCompletionFailureFunction)); err != nil {
		return nil, fmt.Errorf("install API completion failure trigger for %s (%s): %v", invariant, reason, err)
	}
	return func() {
		_, _ = s.backend.ExecContext(context.Background(), "DROP TRIGGER IF EXISTS "+apiCompletionFailureTrigger+" ON api_idempotency")
		_, _ = s.backend.ExecContext(context.Background(), "DROP FUNCTION IF EXISTS "+apiCompletionFailureFunction+"()")
	}, nil
}

func (s *SQLiteOwner) InstallAPICompletionFailureAfterPublicationForTest(ctx context.Context, invariant, reason, flowTemplate string) (func(), error) {
	if strings.TrimSpace(invariant) == "" || strings.TrimSpace(reason) == "" {
		return nil, fmt.Errorf("event failure injection requires an invariant and reason")
	}
	flowTemplate = strings.TrimSpace(flowTemplate)
	if flowTemplate == "" {
		return nil, fmt.Errorf("event failure injection requires a flow template")
	}
	quotedTemplate := strings.ReplaceAll(flowTemplate, "'", "''")
	triggerSQL := fmt.Sprintf(`
		CREATE TRIGGER %s BEFORE INSERT ON api_idempotency WHEN NEW.method = 'event.publish' BEGIN
			SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM events WHERE event_id = NEW.resource_id) OR
				NOT EXISTS (SELECT 1 FROM event_deliveries WHERE event_id = NEW.resource_id) OR
				NOT EXISTS (SELECT 1 FROM committed_replay_scopes WHERE event_id = NEW.resource_id) OR
				NOT EXISTS (SELECT 1 FROM flow_instances WHERE run_id = (SELECT run_id FROM events WHERE event_id = NEW.resource_id) AND flow_template = '%s') OR
				NOT EXISTS (SELECT 1 FROM entity_state es JOIN flow_instances fi ON fi.run_id = es.run_id AND fi.instance_path = es.flow_instance WHERE fi.run_id = (SELECT run_id FROM events WHERE event_id = NEW.resource_id) AND fi.flow_template = '%s') OR
				NOT EXISTS (
					SELECT 1 FROM flow_instances fi
					JOIN workflow_instance_initial_materializations m ON m.run_id = fi.run_id AND m.instance_path = fi.instance_path AND m.entity_id = fi.entity_id
					JOIN flow_instance_runtime_readiness r ON r.run_id = fi.run_id AND r.instance_path = fi.instance_path
					WHERE fi.run_id = (SELECT run_id FROM events WHERE event_id = NEW.resource_id) AND fi.flow_template = '%s'
				)
				THEN RAISE(ABORT, 'API completion failure injection reached before complete publication persistence') END;
			SELECT RAISE(ABORT, 'injected API idempotency completion persistence failure');
		END
	`, apiCompletionFailureTrigger, quotedTemplate, quotedTemplate, quotedTemplate)
	if _, err := s.backend.ExecContext(ctx, triggerSQL); err != nil {
		return nil, fmt.Errorf("install SQLite API completion failure trigger for %s (%s): %v", invariant, reason, err)
	}
	return func() {
		_, _ = s.backend.ExecContext(context.Background(), "DROP TRIGGER IF EXISTS "+apiCompletionFailureTrigger)
	}, nil
}
