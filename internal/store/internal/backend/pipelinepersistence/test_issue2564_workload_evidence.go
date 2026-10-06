package pipelinepersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

type H1FlowAccountingEvidence struct{ FieldlessRoots, RootEntities, ActiveHubs int }

func (s *PipelinePostgresOwner) ObserveH1FlowAccountingForTest(ctx context.Context, runID string) (H1FlowAccountingEvidence, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return H1FlowAccountingEvidence{}, err
	}
	var out H1FlowAccountingEvidence
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return observeH1FlowAccountingForTest(ctx, tx, runID, &out)
	})
	if err != nil {
		return H1FlowAccountingEvidence{}, err
	}
	return out, nil
}

func (s *PipelineSQLiteOwner) ObserveH1FlowAccountingForTest(ctx context.Context, runID string) (H1FlowAccountingEvidence, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return H1FlowAccountingEvidence{}, err
	}
	var out H1FlowAccountingEvidence
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return observeH1FlowAccountingForTest(ctx, tx, runID, &out)
	})
	if err != nil {
		return H1FlowAccountingEvidence{}, err
	}
	return out, nil
}

func observeH1FlowAccountingForTest(ctx context.Context, tx *sql.Tx, runID string, out *H1FlowAccountingEvidence) error {
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM flow_instances WHERE run_id=$1 AND flow_template='.' AND entity_type IS NULL`, runID).Scan(&out.FieldlessRoots); err != nil {
		return err
	}
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM flow_instances f JOIN entity_state e ON e.run_id=f.run_id AND e.entity_id=f.entity_id WHERE f.run_id=$1 AND f.flow_template='.'`, runID).Scan(&out.RootEntities); err != nil {
		return err
	}
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM flow_instances WHERE run_id=$1 AND flow_template='hub' AND mode='template' AND current_state='active' AND status='active'`, runID).Scan(&out.ActiveHubs); err != nil {
		return err
	}
	return nil
}

type H2ConstructionAccountingEvidence struct{ Constructions int }

func (s *PipelinePostgresOwner) ObserveH2ConstructionAccountingForTest(ctx context.Context, runID string) (H2ConstructionAccountingEvidence, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return H2ConstructionAccountingEvidence{}, err
	}
	var out H2ConstructionAccountingEvidence
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return observeH2ConstructionAccountingForTest(ctx, tx, runID, &out)
	})
	if err != nil {
		return H2ConstructionAccountingEvidence{}, err
	}
	return out, nil
}

func (s *PipelineSQLiteOwner) ObserveH2ConstructionAccountingForTest(ctx context.Context, runID string) (H2ConstructionAccountingEvidence, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return H2ConstructionAccountingEvidence{}, err
	}
	var out H2ConstructionAccountingEvidence
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return observeH2ConstructionAccountingForTest(ctx, tx, runID, &out)
	})
	if err != nil {
		return H2ConstructionAccountingEvidence{}, err
	}
	return out, nil
}

func observeH2ConstructionAccountingForTest(ctx context.Context, tx *sql.Tx, runID string, out *H2ConstructionAccountingEvidence) error {
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM workflow_instance_initial_materializations WHERE run_id=$1`, runID).Scan(&out.Constructions); err != nil {
		return err
	}
	return nil
}

type H1RunOverlapEvidence struct{ Bumps int }

func (s *PipelinePostgresOwner) ObserveH1RunOverlapForTest(ctx context.Context, runID string) (H1RunOverlapEvidence, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return H1RunOverlapEvidence{}, err
	}
	var out H1RunOverlapEvidence
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error { return observeH1RunOverlapForTest(ctx, tx, runID, &out) })
	if err != nil {
		return H1RunOverlapEvidence{}, err
	}
	return out, nil
}

func (s *PipelineSQLiteOwner) ObserveH1RunOverlapForTest(ctx context.Context, runID string) (H1RunOverlapEvidence, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return H1RunOverlapEvidence{}, err
	}
	var out H1RunOverlapEvidence
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error { return observeH1RunOverlapForTest(ctx, tx, runID, &out) })
	if err != nil {
		return H1RunOverlapEvidence{}, err
	}
	return out, nil
}

func observeH1RunOverlapForTest(ctx context.Context, tx *sql.Tx, runID string, out *H1RunOverlapEvidence) error {
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM entity_mutations m JOIN events e ON e.event_id=m.caused_by_event WHERE m.run_id=$1 AND m.path='count' AND e.event_name='hub.bump' AND m.created_at>=(SELECT MIN(created_at) FROM entity_mutations WHERE run_id=$1 AND writer_type='agent') AND m.created_at<=(SELECT MAX(created_at) FROM entity_mutations WHERE run_id=$1 AND writer_type='agent')`, runID).Scan(&out.Bumps); err != nil {
		return err
	}
	return nil
}

type H1HubFieldsEvidence struct {
	Entity   string
	Revision int
	Fields   string
}

func (s *PipelinePostgresOwner) ObserveH1HubFieldsForTest(ctx context.Context, runID string) ([]H1HubFieldsEvidence, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	var out []H1HubFieldsEvidence
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = observeH1HubFieldsForTest(ctx, tx, runID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *PipelineSQLiteOwner) ObserveH1HubFieldsForTest(ctx context.Context, runID string) ([]H1HubFieldsEvidence, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	var out []H1HubFieldsEvidence
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = observeH1HubFieldsForTest(ctx, tx, runID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func observeH1HubFieldsForTest(ctx context.Context, tx *sql.Tx, runID string) ([]H1HubFieldsEvidence, error) {
	rows, err := tx.QueryContext(ctx, `SELECT f.entity_id,f.revision,CAST(e.fields AS TEXT) FROM flow_instances f JOIN entity_state e ON e.run_id=f.run_id AND e.entity_id=f.entity_id WHERE f.run_id=$1 AND f.flow_template='hub'`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []H1HubFieldsEvidence
	for rows.Next() {
		var row H1HubFieldsEvidence
		if err := rows.Scan(&row.Entity, &row.Revision, &row.Fields); err != nil {
			return nil, errors.Join(err, rows.Close())
		}
		out = append(out, row)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	return out, nil
}

type H1AttributedMutationsEvidence struct {
	Entity string
	Field  string
	Kind   string
	Writer string
	Step   string
	Before string
	After  string
	Event  string
	Owner  string
}

func (s *PipelinePostgresOwner) ObserveH1AttributedMutationsForTest(ctx context.Context, runID, bundleHash string) ([]H1AttributedMutationsEvidence, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	var out []H1AttributedMutationsEvidence
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = observeH1AttributedMutationsForTest(ctx, tx, runID, bundleHash)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *PipelineSQLiteOwner) ObserveH1AttributedMutationsForTest(ctx context.Context, runID, bundleHash string) ([]H1AttributedMutationsEvidence, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	var out []H1AttributedMutationsEvidence
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = observeH1AttributedMutationsForTest(ctx, tx, runID, bundleHash)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func observeH1AttributedMutationsForTest(ctx context.Context, tx *sql.Tx, runID, bundleHash string) ([]H1AttributedMutationsEvidence, error) {
	rows, err := tx.QueryContext(ctx, `SELECT m.entity_id,m.path,m.writer_type,m.writer_id,m.handler_step,CAST(m.old_value AS TEXT),CAST(m.new_value AS TEXT),e.event_name,COALESCE(a.agent_name_owner,'') FROM entity_mutations m JOIN events e ON e.event_id=m.caused_by_event LEFT JOIN agents a ON a.run_id=m.run_id AND a.agent_id=m.writer_id AND a.entity_id=m.entity_id AND a.flow_scope_key='hub' AND a.agent_name_source='declared' AND a.agent_route_presence='present' AND a.lifecycle_bundle_hash=$2 WHERE m.run_id=$1 AND m.domain='authored_field' AND (m.path LIKE 'a%' OR m.path LIKE 'b%')`, runID, bundleHash)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []H1AttributedMutationsEvidence
	for rows.Next() {
		var row H1AttributedMutationsEvidence
		if err := rows.Scan(&row.Entity, &row.Field, &row.Kind, &row.Writer, &row.Step, &row.Before, &row.After, &row.Event, &row.Owner); err != nil {
			return nil, errors.Join(err, rows.Close())
		}
		out = append(out, row)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	return out, nil
}

type H1SameEntityOverlapEvidence struct {
	Entity string
	Route  string
	Fields string
	Count  int
}

func (s *PipelinePostgresOwner) ObserveH1SameEntityOverlapForTest(ctx context.Context, runID string) ([]H1SameEntityOverlapEvidence, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	var out []H1SameEntityOverlapEvidence
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = observeH1SameEntityOverlapForTest(ctx, tx, runID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *PipelineSQLiteOwner) ObserveH1SameEntityOverlapForTest(ctx context.Context, runID string) ([]H1SameEntityOverlapEvidence, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	var out []H1SameEntityOverlapEvidence
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = observeH1SameEntityOverlapForTest(ctx, tx, runID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func observeH1SameEntityOverlapForTest(ctx context.Context, tx *sql.Tx, runID string) ([]H1SameEntityOverlapEvidence, error) {
	rows, err := tx.QueryContext(ctx, `SELECT f.entity_id,f.instance_path,CAST(s.fields AS TEXT),
		(SELECT COUNT(*) FROM entity_mutations m JOIN events e ON e.event_id=m.caused_by_event
		 WHERE m.run_id=f.run_id AND m.entity_id=f.entity_id AND m.domain='authored_field' AND m.path='count' AND e.event_name='hub.bump'
		 AND m.created_at>=(SELECT MIN(a.created_at) FROM entity_mutations a WHERE a.run_id=f.run_id AND a.entity_id=f.entity_id AND a.domain='authored_field' AND a.writer_type='agent')
		 AND m.created_at<=(SELECT MAX(a.created_at) FROM entity_mutations a WHERE a.run_id=f.run_id AND a.entity_id=f.entity_id AND a.domain='authored_field' AND a.writer_type='agent'))
		FROM flow_instances f JOIN entity_state s ON s.run_id=f.run_id AND s.entity_id=f.entity_id
		WHERE f.run_id=$1 AND f.flow_template='hub' ORDER BY f.instance_path`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []H1SameEntityOverlapEvidence
	for rows.Next() {
		var row H1SameEntityOverlapEvidence
		if err := rows.Scan(&row.Entity, &row.Route, &row.Fields, &row.Count); err != nil {
			return nil, errors.Join(err, rows.Close())
		}
		out = append(out, row)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	return out, nil
}

type H1FailureMutationsEvidence struct {
	ID         string
	Entity     string
	Field      string
	Kind       string
	Writer     string
	Step       string
	Before     string
	After      string
	Event      string
	EventName  string
	RecordedAt string
}

func (s *PipelinePostgresOwner) ObserveH1FailureMutationsForTest(ctx context.Context, runID string) ([]H1FailureMutationsEvidence, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	var out []H1FailureMutationsEvidence
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = observeH1FailureMutationsForTest(ctx, tx, runID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *PipelineSQLiteOwner) ObserveH1FailureMutationsForTest(ctx context.Context, runID string) ([]H1FailureMutationsEvidence, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	var out []H1FailureMutationsEvidence
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = observeH1FailureMutationsForTest(ctx, tx, runID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func observeH1FailureMutationsForTest(ctx context.Context, tx *sql.Tx, runID string) ([]H1FailureMutationsEvidence, error) {
	rows, err := tx.QueryContext(ctx, `SELECT m.mutation_id,m.entity_id,m.path,m.writer_type,m.writer_id,COALESCE(m.handler_step,''),
		COALESCE(CAST(m.old_value AS TEXT),'null'),COALESCE(CAST(m.new_value AS TEXT),'null'),
		COALESCE(CAST(m.caused_by_event AS TEXT),''),COALESCE(e.event_name,''),CAST(m.created_at AS TEXT)
		FROM entity_mutations m LEFT JOIN events e ON e.event_id=m.caused_by_event
		WHERE m.run_id=$1 AND m.domain='authored_field' AND (m.path LIKE 'a%' OR m.path LIKE 'b%')
		ORDER BY m.entity_id,m.path,m.created_at,m.mutation_id`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []H1FailureMutationsEvidence
	for rows.Next() {
		var row H1FailureMutationsEvidence
		if err := rows.Scan(&row.ID, &row.Entity, &row.Field, &row.Kind, &row.Writer, &row.Step, &row.Before, &row.After, &row.Event, &row.EventName, &row.RecordedAt); err != nil {
			return nil, errors.Join(err, rows.Close())
		}
		out = append(out, row)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	return out, nil
}

type H1BumpHistoryEvidence struct {
	Entity string
	Event  string
	Before string
	After  string
}

func (s *PipelinePostgresOwner) ObserveH1BumpHistoryForTest(ctx context.Context, runID string) ([]H1BumpHistoryEvidence, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	var out []H1BumpHistoryEvidence
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = observeH1BumpHistoryForTest(ctx, tx, runID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *PipelineSQLiteOwner) ObserveH1BumpHistoryForTest(ctx context.Context, runID string) ([]H1BumpHistoryEvidence, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	var out []H1BumpHistoryEvidence
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = observeH1BumpHistoryForTest(ctx, tx, runID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func observeH1BumpHistoryForTest(ctx context.Context, tx *sql.Tx, runID string) ([]H1BumpHistoryEvidence, error) {
	rows, err := tx.QueryContext(ctx, `SELECT m.entity_id,m.caused_by_event,CAST(m.old_value AS TEXT),CAST(m.new_value AS TEXT) FROM entity_mutations m JOIN events e ON e.event_id=m.caused_by_event JOIN event_deliveries d ON d.event_id=e.event_id AND d.run_id=m.run_id AND d.subscriber_type='node' WHERE m.run_id=$1 AND m.domain='authored_field' AND m.path='count' AND e.event_name='hub.bump' AND d.status='delivered'`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []H1BumpHistoryEvidence
	for rows.Next() {
		var row H1BumpHistoryEvidence
		if err := rows.Scan(&row.Entity, &row.Event, &row.Before, &row.After); err != nil {
			return nil, errors.Join(err, rows.Close())
		}
		out = append(out, row)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	return out, nil
}

type H2HubsEvidence struct {
	Entity         string
	Instance       string
	Stage          string
	Fields         []byte
	Revision       int64
	HeaderState    string
	HeaderRevision int64
	Config         []byte
}

func (s *PipelinePostgresOwner) ObserveH2HubsForTest(ctx context.Context, runID string) ([]H2HubsEvidence, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	var out []H2HubsEvidence
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = observeH2HubsForTest(ctx, tx, runID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *PipelineSQLiteOwner) ObserveH2HubsForTest(ctx context.Context, runID string) ([]H2HubsEvidence, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	var out []H2HubsEvidence
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = observeH2HubsForTest(ctx, tx, runID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func observeH2HubsForTest(ctx context.Context, tx *sql.Tx, runID string) ([]H2HubsEvidence, error) {
	rows, err := tx.QueryContext(ctx, `SELECT s.entity_id,s.flow_instance,s.current_state,s.fields,s.revision,f.current_state,f.revision,f.config FROM entity_state s JOIN flow_instances f ON f.run_id=s.run_id AND f.entity_id=s.entity_id WHERE s.run_id=$1`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []H2HubsEvidence
	for rows.Next() {
		var row H2HubsEvidence
		if err := rows.Scan(&row.Entity, &row.Instance, &row.Stage, &row.Fields, &row.Revision, &row.HeaderState, &row.HeaderRevision, &row.Config); err != nil {
			return nil, errors.Join(err, rows.Close())
		}
		out = append(out, row)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	return out, nil
}

type H2OccurrencesEvidence struct {
	ID       string
	Task     string
	Instance string
	Outcome  string
	Reason   string
}

func (s *PipelinePostgresOwner) ObserveH2OccurrencesForTest(ctx context.Context, runID string) ([]H2OccurrencesEvidence, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	var out []H2OccurrencesEvidence
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = observeH2OccurrencesForTest(ctx, tx, runID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *PipelineSQLiteOwner) ObserveH2OccurrencesForTest(ctx context.Context, runID string) ([]H2OccurrencesEvidence, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	var out []H2OccurrencesEvidence
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = observeH2OccurrencesForTest(ctx, tx, runID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func observeH2OccurrencesForTest(ctx context.Context, tx *sql.Tx, runID string) ([]H2OccurrencesEvidence, error) {
	rows, err := tx.QueryContext(ctx, `SELECT e.event_id,e.task_id,e.flow_instance,COALESCE(r.outcome,''),COALESCE(r.reason_code,'') FROM events e LEFT JOIN event_receipts r ON r.event_id=e.event_id AND r.subscriber_type='platform' AND r.subscriber_id='pipeline' WHERE e.run_id=$1 AND e.event_name='platform.stage_timer'`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []H2OccurrencesEvidence
	for rows.Next() {
		var row H2OccurrencesEvidence
		if err := rows.Scan(&row.ID, &row.Task, &row.Instance, &row.Outcome, &row.Reason); err != nil {
			return nil, errors.Join(err, rows.Close())
		}
		out = append(out, row)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	return out, nil
}

type H2CounterMutationsEvidence struct {
	Entity string
	Path   string
	Before WorkloadNullableText
	After  WorkloadNullableText
	Cause  string
}

func (s *PipelinePostgresOwner) ObserveH2CounterMutationsForTest(ctx context.Context, runID string) ([]H2CounterMutationsEvidence, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	var out []H2CounterMutationsEvidence
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = observeH2CounterMutationsForTest(ctx, tx, runID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *PipelineSQLiteOwner) ObserveH2CounterMutationsForTest(ctx context.Context, runID string) ([]H2CounterMutationsEvidence, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	var out []H2CounterMutationsEvidence
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = observeH2CounterMutationsForTest(ctx, tx, runID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func observeH2CounterMutationsForTest(ctx context.Context, tx *sql.Tx, runID string) ([]H2CounterMutationsEvidence, error) {
	rows, err := tx.QueryContext(ctx, `SELECT entity_id,path,CAST(old_value AS TEXT),CAST(new_value AS TEXT),COALESCE(CAST(caused_by_event AS TEXT),'') FROM entity_mutations WHERE run_id=$1 AND domain='authored_field' AND path IN ('count','c1','c2')`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []H2CounterMutationsEvidence
	for rows.Next() {
		var row H2CounterMutationsEvidence
		var before sql.NullString
		var after sql.NullString
		if err := rows.Scan(&row.Entity, &row.Path, &before, &after, &row.Cause); err != nil {
			return nil, errors.Join(err, rows.Close())
		}
		row.Before = WorkloadNullableText{String: before.String, Valid: before.Valid}
		row.After = WorkloadNullableText{String: after.String, Valid: after.Valid}
		out = append(out, row)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	return out, nil
}

type WorkloadNullableText struct {
	String string
	Valid  bool
}

type H2TimersEvidence struct {
	ID, Name, Entity, Instance, Status string
	Created, Due, Fired                time.Time
}

func (s *PipelinePostgresOwner) ObserveH2TimersForTest(ctx context.Context, runID string) ([]H2TimersEvidence, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	var out []H2TimersEvidence
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = observeH2TimersForTest(ctx, tx, runID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}
func (s *PipelineSQLiteOwner) ObserveH2TimersForTest(ctx context.Context, runID string) ([]H2TimersEvidence, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	var out []H2TimersEvidence
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = observeH2TimersForTest(ctx, tx, runID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}
func observeH2TimersForTest(ctx context.Context, tx *sql.Tx, runID string) ([]H2TimersEvidence, error) {
	rows, err := tx.QueryContext(ctx, `SELECT timer_id,timer_name,entity_id,flow_instance,status,created_at,fire_at,fired_at FROM timers WHERE run_id=$1 AND task_type='workflow_timer'`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []H2TimersEvidence
	for rows.Next() {
		var row H2TimersEvidence
		var created, due, fired any
		if err := rows.Scan(&row.ID, &row.Name, &row.Entity, &row.Instance, &row.Status, &created, &due, &fired); err != nil {
			return nil, errors.Join(err, rows.Close())
		}
		if row.Created, err = issue2564H2TimeForTest(created); err != nil {
			return nil, errors.Join(err, rows.Close())
		}
		if row.Due, err = issue2564H2TimeForTest(due); err != nil {
			return nil, errors.Join(err, rows.Close())
		}
		if row.Fired, err = issue2564H2TimeForTest(fired); err != nil {
			return nil, errors.Join(err, rows.Close())
		}
		out = append(out, row)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	return out, nil
}

func issue2564H2TimeForTest(value any) (time.Time, error) {
	if value == nil {
		return time.Time{}, nil
	}
	if stamp, ok := value.(time.Time); ok {
		return stamp.UTC(), nil
	}
	if raw, ok := value.([]byte); ok {
		value = string(raw)
	}
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05.999999999-07:00", "2006-01-02 15:04:05.999999999+00", "2006-01-02 15:04:05.999999999", "2006-01-02 15:04:05.999999999 -0700 MST"} {
		if stamp, err := time.Parse(layout, fmt.Sprint(value)); err == nil {
			return stamp.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("H2 timestamp %T %q", value, value)
}
