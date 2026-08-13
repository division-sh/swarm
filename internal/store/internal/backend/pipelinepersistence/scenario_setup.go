package pipelinepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimemutationlog "github.com/division-sh/swarm/internal/runtime/mutationlog"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	storeentity "github.com/division-sh/swarm/internal/store/internal/backend/entityruntime"
	privatemutationlog "github.com/division-sh/swarm/internal/store/internal/backend/mutationlog"
	storescenarioexecution "github.com/division-sh/swarm/internal/store/internal/backend/scenarioexecutionpersistence"
)

func (s *PipelinePostgresOwner) SetupScenarioEntities(ctx context.Context, req runtimepipeline.ScenarioSetupRequest) (runtimepipeline.ScenarioSetupResult, error) {
	if s == nil || s.backend == nil {
		return runtimepipeline.ScenarioSetupResult{}, fmt.Errorf("postgres scenario setup store is required")
	}
	req, err := normalizeScenarioSetupRequest(req)
	if err != nil {
		return runtimepipeline.ScenarioSetupResult{}, err
	}
	ctx = runtimecorrelation.WithRunID(ctx, req.RunID)
	if err := s.runPrivateAuthorActivityMutation(ctx, func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
		fact, ok := runtimecorrelation.BundleSourceFactFromContext(txctx)
		if !ok {
			return fmt.Errorf("postgres scenario setup requires executable bundle source fact")
		}
		if _, err := s.RunLifecyclePostgresOwner.CreateRunTx(txctx, tx, story, runtimerunlifecycle.CreateRequest{
			RunID: req.RunID, Origin: runtimerunlifecycle.ScenarioSetupRunOrigin(),
			Source: fact, StartedAt: req.CreatedAt,
		}); err != nil {
			return err
		}
		if req.ScenarioExecutionProfile != nil {
			if err := storescenarioexecution.EnsurePostgres(txctx, tx, req.RunID, *req.ScenarioExecutionProfile, req.CreatedAt); err != nil {
				return err
			}
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
				VALUES (
					$1::uuid, $2::uuid, $3, $4, NULL,
					$5, $6::jsonb, $7::jsonb, '{}'::jsonb, 1,
					$8, $8, $8
				)
				ON CONFLICT (run_id, entity_id) DO NOTHING
			`, req.RunID, entity.EntityID, entity.FlowInstance, entity.EntityType, entity.CurrentState, string(gatesJSON), string(fieldsJSON), req.CreatedAt)
			if err != nil {
				return fmt.Errorf("insert postgres scenario setup entity %s: %w", entity.Alias, err)
			}
			rows, err := res.RowsAffected()
			if err != nil {
				return fmt.Errorf("inspect postgres scenario setup entity insert %s: %w", entity.Alias, err)
			}
			if rows == 0 {
				if err := validateExistingPostgresScenarioSetupEntity(txctx, tx, req.RunID, entity, fieldsJSON, gatesJSON); err != nil {
					return err
				}
				continue
			}
			if err := privatemutationlog.InsertEntityStateDiffWithStory(txctx, tx, activeRunSourceOwnerFunc(func(ctx context.Context, runID string) (runtimecorrelation.BundleSourceFact, error) {
				return s.RunLifecyclePostgresOwner.RequireActiveSourceTx(ctx, tx, runID)
			}), story, entity.EntityID, runtimemutationlog.EntityStateProjection{}, runtimemutationlog.EntityStateProjection{
				CurrentState: entity.CurrentState,
				Fields:       fieldsAny,
				Gates:        gatesAny,
			}, scenarioSetupMutationWriter()); err != nil {
				return fmt.Errorf("record postgres scenario setup entity mutation %s: %w", entity.Alias, err)
			}
		}
		return nil
	}); err != nil {
		return runtimepipeline.ScenarioSetupResult{}, err
	}
	return scenarioSetupResult(req), nil
}

func (s *PipelineSQLiteOwner) SetupScenarioEntities(ctx context.Context, req runtimepipeline.ScenarioSetupRequest) (runtimepipeline.ScenarioSetupResult, error) {
	if s == nil || s.backend == nil {
		return runtimepipeline.ScenarioSetupResult{}, fmt.Errorf("sqlite scenario setup store is required")
	}
	req, err := normalizeScenarioSetupRequest(req)
	if err != nil {
		return runtimepipeline.ScenarioSetupResult{}, err
	}
	ctx = runtimecorrelation.WithRunID(ctx, req.RunID)
	if err := s.runPrivateAuthorActivityMutation(ctx, "sqlite scenario setup", func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
		fact, ok := runtimecorrelation.BundleSourceFactFromContext(txctx)
		if !ok {
			return fmt.Errorf("sqlite scenario setup requires executable bundle source fact")
		}
		if _, err := s.RunLifecycleSQLiteOwner.CreateRunTx(txctx, tx, story, runtimerunlifecycle.CreateRequest{
			RunID: req.RunID, Origin: runtimerunlifecycle.ScenarioSetupRunOrigin(),
			Source: fact, StartedAt: req.CreatedAt,
		}); err != nil {
			return err
		}
		if req.ScenarioExecutionProfile != nil {
			if err := storescenarioexecution.EnsureSQLite(txctx, tx, req.RunID, *req.ScenarioExecutionProfile, req.CreatedAt); err != nil {
				return err
			}
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
			if err := storeentity.InsertSQLiteEntityStateDiff(txctx, story, tx, req.RunID, entity.EntityID, runtimemutationlog.EntityStateProjection{}, runtimemutationlog.EntityStateProjection{
				CurrentState: entity.CurrentState,
				Fields:       fieldsAny,
				Gates:        gatesAny,
			}, scenarioSetupMutationWriter(), req.CreatedAt); err != nil {
				return fmt.Errorf("record sqlite scenario setup entity mutation %s: %w", entity.Alias, err)
			}
		}
		return nil
	}); err != nil {
		return runtimepipeline.ScenarioSetupResult{}, err
	}
	return scenarioSetupResult(req), nil
}

func normalizeScenarioSetupRequest(req runtimepipeline.ScenarioSetupRequest) (runtimepipeline.ScenarioSetupRequest, error) {
	req.RunID = nullUUIDString(req.RunID)
	if req.RunID == "" {
		return runtimepipeline.ScenarioSetupRequest{}, fmt.Errorf("run_id must be uuid")
	}
	if req.CreatedAt.IsZero() {
		req.CreatedAt = time.Now().UTC()
	} else {
		req.CreatedAt = req.CreatedAt.UTC()
	}
	if len(req.Entities) == 0 {
		return runtimepipeline.ScenarioSetupRequest{}, fmt.Errorf("entities is required")
	}
	aliases := map[string]struct{}{}
	ids := map[string]struct{}{}
	for i := range req.Entities {
		entity := &req.Entities[i]
		entity.Alias = strings.TrimSpace(entity.Alias)
		if entity.Alias == "" {
			return runtimepipeline.ScenarioSetupRequest{}, fmt.Errorf("entities[%d].alias is required", i)
		}
		if _, ok := aliases[entity.Alias]; ok {
			return runtimepipeline.ScenarioSetupRequest{}, fmt.Errorf("entities[%d].alias %q is duplicated", i, entity.Alias)
		}
		aliases[entity.Alias] = struct{}{}
		entity.EntityID = nullUUIDString(entity.EntityID)
		if entity.EntityID == "" {
			return runtimepipeline.ScenarioSetupRequest{}, fmt.Errorf("entities[%d].entity_id must be uuid", i)
		}
		entity.FlowInstance = strings.Trim(strings.TrimSpace(entity.FlowInstance), "/")
		if entity.FlowInstance == "" {
			entity.FlowInstance = req.RunID
		}
		if _, ok := ids[entity.EntityID]; ok {
			return runtimepipeline.ScenarioSetupRequest{}, fmt.Errorf("entities[%d].entity_id %q is duplicated", i, entity.EntityID)
		}
		ids[entity.EntityID] = struct{}{}
		entity.FlowInstance = strings.Trim(strings.TrimSpace(entity.FlowInstance), "/")
		entity.EntityType = strings.TrimSpace(entity.EntityType)
		entity.CurrentState = strings.TrimSpace(entity.CurrentState)
		if entity.EntityType == "" {
			return runtimepipeline.ScenarioSetupRequest{}, fmt.Errorf("entities[%d].entity_type is required", i)
		}
		if entity.CurrentState == "" {
			return runtimepipeline.ScenarioSetupRequest{}, fmt.Errorf("entities[%d].current_state is required", i)
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

func scenarioSetupEntityJSON(entity runtimepipeline.ScenarioSetupEntityRequest) (json.RawMessage, json.RawMessage, map[string]any, map[string]any, error) {
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

func validateExistingPostgresScenarioSetupEntity(ctx context.Context, tx *sql.Tx, runID string, entity runtimepipeline.ScenarioSetupEntityRequest, fieldsJSON, gatesJSON json.RawMessage) error {
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

func validateExistingSQLiteScenarioSetupEntity(ctx context.Context, tx *sql.Tx, runID string, entity runtimepipeline.ScenarioSetupEntityRequest, fieldsJSON, gatesJSON json.RawMessage) error {
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

func validateExistingScenarioSetupEntity(snapshot scenarioSetupEntitySnapshot, entity runtimepipeline.ScenarioSetupEntityRequest, fieldsJSON, gatesJSON json.RawMessage) error {
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

func scenarioSetupResult(req runtimepipeline.ScenarioSetupRequest) runtimepipeline.ScenarioSetupResult {
	out := runtimepipeline.ScenarioSetupResult{RunID: req.RunID, Entities: make([]runtimepipeline.ScenarioSetupEntityResult, 0, len(req.Entities))}
	for _, entity := range req.Entities {
		out.Entities = append(out.Entities, runtimepipeline.ScenarioSetupEntityResult{
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
	return storeentity.MutationWriter(runtimetools.EntityMutationWriter{
		Type: "platform",
		ID:   "test.setup_entities",
	})
}
