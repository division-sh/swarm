package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimemutationlog "github.com/division-sh/swarm/internal/runtime/mutationlog"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
)

type ScenarioSetupRequest struct {
	RunID     string
	Entities  []ScenarioSetupEntityRequest
	CreatedAt time.Time
}

type ScenarioSetupEntityRequest struct {
	Alias        string
	EntityID     string
	FlowInstance string
	EntityType   string
	CurrentState string
	Fields       map[string]any
	Gates        map[string]bool
}

type ScenarioSetupResult struct {
	RunID    string
	Entities []ScenarioSetupEntityResult
}

type ScenarioSetupEntityResult struct {
	Alias        string
	EntityID     string
	FlowInstance string
	EntityType   string
	CurrentState string
}

func (s *PostgresStore) SetupScenarioEntities(ctx context.Context, req ScenarioSetupRequest) (ScenarioSetupResult, error) {
	if s == nil || s.DB == nil {
		return ScenarioSetupResult{}, fmt.Errorf("postgres scenario setup store is required")
	}
	req, err := normalizeScenarioSetupRequest(req)
	if err != nil {
		return ScenarioSetupResult{}, err
	}
	ctx = runtimecorrelation.WithRunID(ctx, req.RunID)
	if err := s.requireCurrentSchema(); err != nil {
		return ScenarioSetupResult{}, err
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return ScenarioSetupResult{}, fmt.Errorf("begin postgres scenario setup: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	ctx, err = runtimeauthoractivity.Begin(ctx, tx, runtimeauthoractivity.DialectPostgres)
	if err != nil {
		return ScenarioSetupResult{}, err
	}
	if err := s.ensureRunRow(ctx, tx, req.RunID, "", "test.setup_entities"); err != nil {
		return ScenarioSetupResult{}, err
	}
	for _, entity := range req.Entities {
		fieldsJSON, gatesJSON, fieldsAny, gatesAny, err := scenarioSetupEntityJSON(entity)
		if err != nil {
			return ScenarioSetupResult{}, err
		}
		res, err := tx.ExecContext(ctx, `
			INSERT INTO entity_state (
				run_id, entity_id, flow_instance, entity_type, name,
				current_state, gates, fields, accumulator, revision,
				entered_state_at, created_at, updated_at
			)
			VALUES (
				$1::uuid, $2::uuid, $3, $4, NULL,
				$5, $6::jsonb, $7::jsonb, '{}'::jsonb, 1,
				$8, $8, $8
			)
			ON CONFLICT (run_id, entity_id) DO NOTHING
		`, req.RunID, entity.EntityID, entity.FlowInstance, entity.EntityType, entity.CurrentState, string(gatesJSON), string(fieldsJSON), req.CreatedAt)
		if err != nil {
			return ScenarioSetupResult{}, fmt.Errorf("insert postgres scenario setup entity %s: %w", entity.Alias, err)
		}
		rows, err := res.RowsAffected()
		if err != nil {
			return ScenarioSetupResult{}, fmt.Errorf("inspect postgres scenario setup entity insert %s: %w", entity.Alias, err)
		}
		if rows == 0 {
			if err := validateExistingPostgresScenarioSetupEntity(ctx, tx, req.RunID, entity, fieldsJSON, gatesJSON); err != nil {
				return ScenarioSetupResult{}, err
			}
			continue
		}
		if err := runtimemutationlog.InsertEntityStateDiff(ctx, tx, entity.EntityID, runtimemutationlog.EntityStateProjection{}, runtimemutationlog.EntityStateProjection{
			CurrentState: entity.CurrentState,
			Fields:       fieldsAny,
			Gates:        gatesAny,
		}, scenarioSetupMutationWriter()); err != nil {
			return ScenarioSetupResult{}, fmt.Errorf("record postgres scenario setup entity mutation %s: %w", entity.Alias, err)
		}
	}
	if err := runtimepipeline.CapturePipelineRunForkRevisionChanges(ctx, tx); err != nil {
		return ScenarioSetupResult{}, fmt.Errorf("capture postgres scenario setup revisions: %w", err)
	}
	if err := runtimeauthoractivity.Finalize(ctx); err != nil {
		return ScenarioSetupResult{}, fmt.Errorf("finalize postgres scenario setup story: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return ScenarioSetupResult{}, fmt.Errorf("commit postgres scenario setup: %w", err)
	}
	committed = true
	return scenarioSetupResult(req), nil
}

func (s *SQLiteRuntimeStore) SetupScenarioEntities(ctx context.Context, req ScenarioSetupRequest) (ScenarioSetupResult, error) {
	if s == nil || s.DB == nil {
		return ScenarioSetupResult{}, fmt.Errorf("sqlite scenario setup store is required")
	}
	req, err := normalizeScenarioSetupRequest(req)
	if err != nil {
		return ScenarioSetupResult{}, err
	}
	ctx = runtimecorrelation.WithRunID(ctx, req.RunID)
	if err := s.runAuthorActivityMutation(ctx, "sqlite scenario setup", func(txctx context.Context, tx *sql.Tx) error {
		if err := sqliteEnsureActiveRunRow(txctx, tx, req.RunID, "", "test.setup_entities", req.CreatedAt); err != nil {
			return err
		}
		for _, entity := range req.Entities {
			fieldsJSON, gatesJSON, fieldsAny, gatesAny, err := scenarioSetupEntityJSON(entity)
			if err != nil {
				return err
			}
			res, err := tx.ExecContext(txctx, `
				INSERT INTO entity_state (
					run_id, entity_id, flow_instance, entity_type, name,
					current_state, gates, fields, accumulator, revision,
					entered_state_at, created_at, updated_at
				)
				VALUES (?, ?, ?, ?, NULL, ?, ?, ?, '{}', 1, ?, ?, ?)
				ON CONFLICT (run_id, entity_id) DO NOTHING
			`, req.RunID, entity.EntityID, entity.FlowInstance, entity.EntityType, entity.CurrentState,
				string(gatesJSON), string(fieldsJSON), req.CreatedAt.UTC(), req.CreatedAt.UTC(), req.CreatedAt.UTC())
			if err != nil {
				return fmt.Errorf("insert sqlite scenario setup entity %s: %w", entity.Alias, err)
			}
			rows, err := res.RowsAffected()
			if err != nil {
				return fmt.Errorf("inspect sqlite scenario setup entity insert %s: %w", entity.Alias, err)
			}
			if rows == 0 {
				if err := validateExistingSQLiteScenarioSetupEntity(txctx, tx, req.RunID, entity, fieldsJSON, gatesJSON); err != nil {
					return err
				}
				continue
			}
			if err := insertSQLiteEntityStateDiff(txctx, tx, req.RunID, entity.EntityID, runtimemutationlog.EntityStateProjection{}, runtimemutationlog.EntityStateProjection{
				CurrentState: entity.CurrentState,
				Fields:       fieldsAny,
				Gates:        gatesAny,
			}, scenarioSetupMutationWriter(), req.CreatedAt); err != nil {
				return fmt.Errorf("record sqlite scenario setup entity mutation %s: %w", entity.Alias, err)
			}
		}
		return nil
	}); err != nil {
		return ScenarioSetupResult{}, err
	}
	return scenarioSetupResult(req), nil
}

func normalizeScenarioSetupRequest(req ScenarioSetupRequest) (ScenarioSetupRequest, error) {
	req.RunID = nullUUIDString(req.RunID)
	if req.RunID == "" {
		return ScenarioSetupRequest{}, fmt.Errorf("run_id must be uuid")
	}
	if req.CreatedAt.IsZero() {
		req.CreatedAt = time.Now().UTC()
	} else {
		req.CreatedAt = req.CreatedAt.UTC()
	}
	if len(req.Entities) == 0 {
		return ScenarioSetupRequest{}, fmt.Errorf("entities is required")
	}
	aliases := map[string]struct{}{}
	ids := map[string]struct{}{}
	for i := range req.Entities {
		entity := &req.Entities[i]
		entity.Alias = strings.TrimSpace(entity.Alias)
		if entity.Alias == "" {
			return ScenarioSetupRequest{}, fmt.Errorf("entities[%d].alias is required", i)
		}
		if _, ok := aliases[entity.Alias]; ok {
			return ScenarioSetupRequest{}, fmt.Errorf("entities[%d].alias %q is duplicated", i, entity.Alias)
		}
		aliases[entity.Alias] = struct{}{}
		entity.EntityID = nullUUIDString(entity.EntityID)
		if entity.EntityID == "" {
			return ScenarioSetupRequest{}, fmt.Errorf("entities[%d].entity_id must be uuid", i)
		}
		if _, ok := ids[entity.EntityID]; ok {
			return ScenarioSetupRequest{}, fmt.Errorf("entities[%d].entity_id %q is duplicated", i, entity.EntityID)
		}
		ids[entity.EntityID] = struct{}{}
		entity.FlowInstance = strings.Trim(strings.TrimSpace(entity.FlowInstance), "/")
		entity.EntityType = strings.TrimSpace(entity.EntityType)
		entity.CurrentState = strings.TrimSpace(entity.CurrentState)
		if entity.EntityType == "" {
			return ScenarioSetupRequest{}, fmt.Errorf("entities[%d].entity_type is required", i)
		}
		if entity.CurrentState == "" {
			return ScenarioSetupRequest{}, fmt.Errorf("entities[%d].current_state is required", i)
		}
		if entity.Fields == nil {
			entity.Fields = map[string]any{}
		}
		if entity.Gates == nil {
			entity.Gates = map[string]bool{}
		}
	}
	return req, nil
}

func scenarioSetupEntityJSON(entity ScenarioSetupEntityRequest) (json.RawMessage, json.RawMessage, map[string]any, map[string]any, error) {
	fieldsJSON, err := json.Marshal(entity.Fields)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("marshal setup entity fields: %w", err)
	}
	gatesJSON, err := json.Marshal(entity.Gates)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("marshal setup entity gates: %w", err)
	}
	fieldsAny := make(map[string]any, len(entity.Fields))
	for key, value := range entity.Fields {
		fieldsAny[key] = value
	}
	gatesAny := make(map[string]any, len(entity.Gates))
	for key, value := range entity.Gates {
		gatesAny[key] = value
	}
	return fieldsJSON, gatesJSON, fieldsAny, gatesAny, nil
}

type scenarioSetupEntitySnapshot struct {
	FlowInstance string
	EntityType   string
	CurrentState string
	Fields       string
	Gates        string
	Accumulator  string
	Revision     int
}

func validateExistingPostgresScenarioSetupEntity(ctx context.Context, tx *sql.Tx, runID string, entity ScenarioSetupEntityRequest, fieldsJSON, gatesJSON json.RawMessage) error {
	var snapshot scenarioSetupEntitySnapshot
	err := tx.QueryRowContext(ctx, `
		SELECT flow_instance, entity_type, current_state, fields::text, gates::text, accumulator::text, revision
		FROM entity_state
		WHERE run_id = $1::uuid AND entity_id = $2::uuid
	`, runID, entity.EntityID).Scan(&snapshot.FlowInstance, &snapshot.EntityType, &snapshot.CurrentState, &snapshot.Fields, &snapshot.Gates, &snapshot.Accumulator, &snapshot.Revision)
	if err == sql.ErrNoRows {
		return fmt.Errorf("postgres scenario setup entity %s insert conflicted but no existing row was visible", entity.Alias)
	}
	if err != nil {
		return fmt.Errorf("load existing postgres scenario setup entity %s: %w", entity.Alias, err)
	}
	return validateExistingScenarioSetupEntity(snapshot, entity, fieldsJSON, gatesJSON)
}

func validateExistingSQLiteScenarioSetupEntity(ctx context.Context, tx *sql.Tx, runID string, entity ScenarioSetupEntityRequest, fieldsJSON, gatesJSON json.RawMessage) error {
	var snapshot scenarioSetupEntitySnapshot
	err := tx.QueryRowContext(ctx, `
		SELECT flow_instance, entity_type, current_state, fields, gates, accumulator, revision
		FROM entity_state
		WHERE run_id = ? AND entity_id = ?
	`, runID, entity.EntityID).Scan(&snapshot.FlowInstance, &snapshot.EntityType, &snapshot.CurrentState, &snapshot.Fields, &snapshot.Gates, &snapshot.Accumulator, &snapshot.Revision)
	if err == sql.ErrNoRows {
		return fmt.Errorf("sqlite scenario setup entity %s insert conflicted but no existing row was visible", entity.Alias)
	}
	if err != nil {
		return fmt.Errorf("load existing sqlite scenario setup entity %s: %w", entity.Alias, err)
	}
	return validateExistingScenarioSetupEntity(snapshot, entity, fieldsJSON, gatesJSON)
}

func validateExistingScenarioSetupEntity(snapshot scenarioSetupEntitySnapshot, entity ScenarioSetupEntityRequest, fieldsJSON, gatesJSON json.RawMessage) error {
	var mismatches []string
	if snapshot.FlowInstance != entity.FlowInstance {
		mismatches = append(mismatches, "flow_instance")
	}
	if snapshot.EntityType != entity.EntityType {
		mismatches = append(mismatches, "entity_type")
	}
	if snapshot.CurrentState != entity.CurrentState {
		mismatches = append(mismatches, "current_state")
	}
	if snapshot.Revision != 1 {
		mismatches = append(mismatches, "revision")
	}
	if !scenarioSetupJSONEqual(snapshot.Fields, fieldsJSON) {
		mismatches = append(mismatches, "fields")
	}
	if !scenarioSetupJSONEqual(snapshot.Gates, gatesJSON) {
		mismatches = append(mismatches, "gates")
	}
	if !scenarioSetupJSONEqual(snapshot.Accumulator, json.RawMessage(`{}`)) {
		mismatches = append(mismatches, "accumulator")
	}
	if len(mismatches) == 0 {
		return nil
	}
	return fmt.Errorf("scenario setup entity %s (%s) already exists with different %s", entity.Alias, entity.EntityID, strings.Join(mismatches, ", "))
}

func scenarioSetupJSONEqual(raw string, want json.RawMessage) bool {
	gotCanonical, err := canonicalScenarioSetupJSON(raw)
	if err != nil {
		return false
	}
	wantCanonical, err := canonicalScenarioSetupJSON(string(want))
	if err != nil {
		return false
	}
	return gotCanonical == wantCanonical
}

func canonicalScenarioSetupJSON(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		raw = "{}"
	}
	var decoded any
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		return "", err
	}
	canonical, err := json.Marshal(decoded)
	if err != nil {
		return "", err
	}
	return string(canonical), nil
}

func scenarioSetupResult(req ScenarioSetupRequest) ScenarioSetupResult {
	out := ScenarioSetupResult{RunID: req.RunID, Entities: make([]ScenarioSetupEntityResult, 0, len(req.Entities))}
	for _, entity := range req.Entities {
		out.Entities = append(out.Entities, ScenarioSetupEntityResult{
			Alias:        entity.Alias,
			EntityID:     entity.EntityID,
			FlowInstance: entity.FlowInstance,
			EntityType:   entity.EntityType,
			CurrentState: entity.CurrentState,
		})
	}
	return out
}

func scenarioSetupMutationWriter() runtimemutationlog.Writer {
	return mutationWriter(runtimetools.EntityMutationWriter{
		Type: "platform",
		ID:   "test.setup_entities",
	})
}

func (r ScenarioSetupResult) Normalized() ScenarioSetupResult {
	r.RunID = strings.TrimSpace(r.RunID)
	for i := range r.Entities {
		r.Entities[i].Alias = strings.TrimSpace(r.Entities[i].Alias)
		r.Entities[i].EntityID = strings.TrimSpace(r.Entities[i].EntityID)
		r.Entities[i].FlowInstance = strings.Trim(strings.TrimSpace(r.Entities[i].FlowInstance), "/")
		r.Entities[i].EntityType = strings.TrimSpace(r.Entities[i].EntityType)
		r.Entities[i].CurrentState = strings.TrimSpace(r.Entities[i].CurrentState)
	}
	return r
}
