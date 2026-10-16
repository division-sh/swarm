package pipelinepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimemutationlog "github.com/division-sh/swarm/internal/runtime/mutationlog"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
	privatemutationlog "github.com/division-sh/swarm/internal/store/internal/backend/mutationlog"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	privaterunforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	storescenarioexecution "github.com/division-sh/swarm/internal/store/internal/backend/scenarioexecutionpersistence"
)

func (s *PipelinePostgresOwner) SetupScenarioEntities(ctx context.Context, req runtimepipeline.ScenarioSetupRequest) (runtimepipeline.ScenarioSetupResult, error) {
	return s.CommitScenarioSetup(ctx, runtimebus.ScenarioSetupCommand{Setup: req})
}

func (s *PipelinePostgresOwner) CommitScenarioSetup(ctx context.Context, command runtimebus.ScenarioSetupCommand) (runtimepipeline.ScenarioSetupResult, error) {
	if s == nil || s.backend == nil {
		return runtimepipeline.ScenarioSetupResult{}, fmt.Errorf("postgres scenario setup store is required")
	}
	req, err := normalizeScenarioSetupRequest(command.Setup)
	if err != nil {
		return runtimepipeline.ScenarioSetupResult{}, err
	}
	ctx = runtimecorrelation.WithRunID(ctx, req.RunID)
	plans, err := validateScenarioConstruction(ctx, req, command.Activations)
	if err != nil {
		return runtimepipeline.ScenarioSetupResult{}, err
	}
	outcome := mutationprotocol.RunPostgres(ctx, s.backend, mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, func(txctx context.Context, attempt *mutationprotocol.Attempt) (runtimepipeline.ScenarioSetupResult, error) {
		result := scenarioSetupResult(req)
		err := attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
			fact, ok := runtimecorrelation.SourceArtifactFactFromContext(txctx)
			if !ok {
				return fmt.Errorf("postgres scenario setup requires executable bundle source fact")
			}
			if _, err := s.RunLifecyclePostgresOwner.CreateRunTx(txctx, attempt, runtimerunlifecycle.CreateRequest{
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
			if len(plans) != 0 {
				var err error
				result.Activations, err = s.CommitFlowInstanceActivationsTx(txctx, attempt, plans)
				if err != nil {
					return err
				}
			}
			for _, entity := range req.Entities {
				if len(plans) != 0 && entity.EntityID == plans[0].Identity.EntityID {
					continue
				}
				if err := requireScenarioImportWithoutHeader(txctx, tx, req.RunID, entity); err != nil {
					return err
				}
				fieldsJSON, _, fieldsAny, _, err := scenarioSetupEntityJSON(entity)
				if err != nil {
					return err
				}
				var storedRunID, storedEntityID string
				err = tx.QueryRowContext(txctx, `
				INSERT INTO entity_state (
					run_id, entity_id, flow_instance, entity_type, name, fields, created_at, updated_at
				)
				VALUES (
					$1::uuid, $2::uuid, $3, $4, NULL,
					$5::jsonb, $6, $6
				)
				ON CONFLICT (run_id, entity_id) DO NOTHING
				RETURNING run_id::text, entity_id::text
				`, req.RunID, entity.EntityID, entity.FlowInstance, entity.EntityType, string(fieldsJSON), req.CreatedAt).Scan(&storedRunID, &storedEntityID)
				if err == sql.ErrNoRows {
					if err := validateExistingPostgresScenarioSetupEntity(txctx, tx, req.RunID, entity, fieldsJSON); err != nil {
						return err
					}
					continue
				}
				if err != nil {
					return fmt.Errorf("insert postgres scenario setup entity %s: %w", entity.Alias, err)
				}
				if err := attempt.AddFact(storedRunID, privaterunforkrevision.FamilyEntityMetadata, storedEntityID); err != nil {
					return err
				}
				if err := privatemutationlog.InsertEntityStateDiff(txctx, attempt, activeRunSourceOwnerFunc(func(ctx context.Context, runID string) (runtimecorrelation.SourceArtifactFact, error) {
					return s.RunLifecyclePostgresOwner.RequireActiveSourceTx(ctx, tx, runID)
				}), storedEntityID, runtimemutationlog.EntityStateProjection{}, runtimemutationlog.EntityStateProjection{
					Fields: fieldsAny,
				}, scenarioSetupMutationWriter()); err != nil {
					return fmt.Errorf("record postgres scenario setup entity mutation %s: %w", entity.Alias, err)
				}
			}
			return nil
		})
		if err != nil {
			return runtimepipeline.ScenarioSetupResult{}, err
		}
		return result, nil
	})
	result, acknowledged := outcome.Value()
	if !acknowledged {
		return runtimepipeline.ScenarioSetupResult{}, outcome.Err()
	}
	return acknowledgeScenarioSetup(result), errors.Join(outcome.Err(), s.publishCardChanges(acknowledged, runtimepipeline.ChannelCardsChanged(result.Activations)))
}

func (s *PipelineSQLiteOwner) SetupScenarioEntities(ctx context.Context, req runtimepipeline.ScenarioSetupRequest) (runtimepipeline.ScenarioSetupResult, error) {
	return s.CommitScenarioSetup(ctx, runtimebus.ScenarioSetupCommand{Setup: req})
}

func (s *PipelineSQLiteOwner) CommitScenarioSetup(ctx context.Context, command runtimebus.ScenarioSetupCommand) (runtimepipeline.ScenarioSetupResult, error) {
	if s == nil || s.backend == nil {
		return runtimepipeline.ScenarioSetupResult{}, fmt.Errorf("sqlite scenario setup store is required")
	}
	req, err := normalizeScenarioSetupRequest(command.Setup)
	if err != nil {
		return runtimepipeline.ScenarioSetupResult{}, err
	}
	ctx = runtimecorrelation.WithRunID(ctx, req.RunID)
	plans, err := validateScenarioConstruction(ctx, req, command.Activations)
	if err != nil {
		return runtimepipeline.ScenarioSetupResult{}, err
	}
	outcome := mutationprotocol.RunSQLite(ctx, s.backend, "sqlite scenario setup", mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, func(txctx context.Context, attempt *mutationprotocol.Attempt) (runtimepipeline.ScenarioSetupResult, error) {
		result := scenarioSetupResult(req)
		err := attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
			fact, ok := runtimecorrelation.SourceArtifactFactFromContext(txctx)
			if !ok {
				return fmt.Errorf("sqlite scenario setup requires executable bundle source fact")
			}
			if _, err := s.RunLifecycleSQLiteOwner.CreateRunTx(txctx, attempt, runtimerunlifecycle.CreateRequest{
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
			if len(plans) != 0 {
				var err error
				result.Activations, err = s.CommitFlowInstanceActivationsTx(txctx, attempt, plans)
				if err != nil {
					return err
				}
			}
			for _, entity := range req.Entities {
				if len(plans) != 0 && entity.EntityID == plans[0].Identity.EntityID {
					continue
				}
				if err := requireScenarioImportWithoutHeader(txctx, tx, req.RunID, entity); err != nil {
					return err
				}
				fieldsJSON, _, fieldsAny, _, err := scenarioSetupEntityJSON(entity)
				if err != nil {
					return err
				}
				res, err := tx.ExecContext(txctx, `
				INSERT INTO entity_state (
					run_id, entity_id, flow_instance, entity_type, name, fields, created_at, updated_at
				)
				VALUES (?, ?, ?, ?, NULL, ?, ?, ?)
				ON CONFLICT (run_id, entity_id) DO NOTHING
				`, req.RunID, entity.EntityID, entity.FlowInstance, entity.EntityType, string(fieldsJSON), req.CreatedAt.UTC(), req.CreatedAt.UTC())
				if err != nil {
					return fmt.Errorf("insert sqlite scenario setup entity %s: %w", entity.Alias, err)
				}
				rows, err := res.RowsAffected()
				if err != nil {
					return fmt.Errorf("inspect sqlite scenario setup entity insert %s: %w", entity.Alias, err)
				}
				if rows == 0 {
					if err := validateExistingSQLiteScenarioSetupEntity(txctx, tx, req.RunID, entity, fieldsJSON); err != nil {
						return err
					}
					continue
				}
				if err := attempt.AddFact(req.RunID, privaterunforkrevision.FamilyEntityMetadata, entity.EntityID); err != nil {
					return err
				}
				if err := privatemutationlog.InsertSQLiteEntityStateDiff(txctx, attempt, activeRunSourceOwnerFunc(func(ctx context.Context, runID string) (runtimecorrelation.SourceArtifactFact, error) {
					return s.RunLifecycleSQLiteOwner.RequireActiveSourceTx(ctx, tx, runID)
				}), entity.EntityID, runtimemutationlog.EntityStateProjection{}, runtimemutationlog.EntityStateProjection{
					Fields: fieldsAny,
				}, scenarioSetupMutationWriter(), req.CreatedAt); err != nil {
					return fmt.Errorf("record sqlite scenario setup entity mutation %s: %w", entity.Alias, err)
				}
			}
			return nil
		})
		if err != nil {
			return runtimepipeline.ScenarioSetupResult{}, err
		}
		return result, nil
	})
	result, acknowledged := outcome.Value()
	if !acknowledged {
		return runtimepipeline.ScenarioSetupResult{}, outcome.Err()
	}
	return acknowledgeScenarioSetup(result), errors.Join(outcome.Err(), s.publishCardChanges(acknowledged, runtimepipeline.ChannelCardsChanged(result.Activations)))
}

func acknowledgeScenarioSetup(result runtimepipeline.ScenarioSetupResult) runtimepipeline.ScenarioSetupResult {
	result.Acknowledged = true
	for index, activation := range result.Activations {
		result.Activations[index] = activation.WithCommitAcknowledgment()
	}
	return result
}

func validateScenarioConstruction(ctx context.Context, req runtimepipeline.ScenarioSetupRequest, activations []runtimebus.FlowInstanceActivationCommand) ([]runtimepipeline.FlowInstanceActivationPlan, error) {
	for _, entity := range req.Entities {
		constructedRoot := len(activations) == 1 && entity.EntityID == req.RunID && entity.FlowInstance == req.RunID
		if !constructedRoot && (entity.CurrentState != nil || entity.Gates != nil) {
			return nil, fmt.Errorf("data-only entity %s cannot carry lifecycle inputs", entity.Alias)
		}
	}
	if len(activations) == 0 {
		return nil, nil
	}
	fact, found := runtimecorrelation.SourceArtifactFactFromContext(ctx)
	if !found || len(activations) != 1 {
		return nil, fmt.Errorf("scenario construction requires one selected root tree")
	}
	command := activations[0]
	if err := command.Validate(); err != nil {
		return nil, err
	}
	plan := command.Plan
	if plan.Readiness.RunID != req.RunID || plan.Readiness.BundleHash != fact.BundleHash() ||
		plan.Identity.InstanceID != req.RunID || plan.Identity.InstancePath != req.RunID || plan.Identity.EntityID != req.RunID ||
		plan.Identity.ParentEntityID != "" || plan.Identity.ParentRoute.FlowID != "" ||
		plan.CreatingInput != (runtimepipeline.FlowConstructionInput{}) || plan.StandingGenerationReplacement ||
		!plan.OccurredAt.Equal(req.CreatedAt) {
		return nil, fmt.Errorf("scenario construction lost its exact no-argument run owner")
	}
	for _, entity := range req.Entities {
		if entity.EntityID != req.RunID {
			continue
		}
		if err := validateScenarioConstructionSeed(entity, plan); err != nil {
			return nil, err
		}
		return []runtimepipeline.FlowInstanceActivationPlan{plan}, nil
	}
	return nil, fmt.Errorf("scenario construction has no exact root seed")
}

func validateScenarioConstructionSeed(entity runtimepipeline.ScenarioSetupEntityRequest, plan runtimepipeline.FlowInstanceActivationPlan) error {
	fields, gates, _, _, err := scenarioSetupEntityJSON(entity)
	if err != nil {
		return err
	}
	record, err := plan.PersistenceRecord()
	if err != nil {
		return err
	}
	matchesType := entity.EntityType == record.EntityType
	if record.EntityType == "" {
		matchesType = entity.EntityType == "default" && len(entity.Fields) == 0
	}
	if !matchesType || entity.CurrentState == nil || *entity.CurrentState != record.CurrentState ||
		!scenarioSetupJSONEqual(string(fields), record.Fields) || !scenarioSetupJSONEqual(string(gates), record.Gates) {
		return fmt.Errorf("scenario construction changed explicit imported fields, stage or gates")
	}
	return nil
}

func requireScenarioImportWithoutHeader(ctx context.Context, tx *sql.Tx, runID string, entity runtimepipeline.ScenarioSetupEntityRequest) error {
	var constructed bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM flow_instances WHERE run_id = $1 AND (entity_id = $2 OR instance_path = $3))`, runID, entity.EntityID, entity.FlowInstance).Scan(&constructed); err != nil {
		return fmt.Errorf("admit data-only scenario owner: %w", err)
	}
	if constructed {
		return fmt.Errorf("data-only entity %s conflicts with a constructed owner", entity.Alias)
	}
	return nil
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
		if entity.CurrentState != nil {
			state := strings.TrimSpace(*entity.CurrentState)
			entity.CurrentState = &state
		}
		if entity.EntityType == "" {
			return runtimepipeline.ScenarioSetupRequest{}, fmt.Errorf("entities[%d].entity_type is required", i)
		}
		if entity.Fields == nil {
			entity.Fields = map[string]any{}
		}
	}
	return req, nil
}

func scenarioSetupEntityJSON(entity runtimepipeline.ScenarioSetupEntityRequest) (json.RawMessage, json.RawMessage, map[string]any, map[string]any, error) {
	admitted, err := canonicaljson.FromGo(entity.Fields)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("admit setup entity fields: %w", err)
	}
	projected, err := workflowexpr.ProjectSemanticValue(admitted)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("project setup entity fields: %w", err)
	}
	fieldsJSON, err := canonicaljson.MarshalPreservingNumberKinds(projected)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("marshal setup entity fields: %w", err)
	}
	gates := entity.Gates
	if gates == nil {
		gates = map[string]bool{}
	}
	gatesJSON, err := json.Marshal(gates)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("marshal setup entity gates: %w", err)
	}
	fieldsAny := projected.(map[string]any)
	gatesAny := make(map[string]any, len(entity.Gates))
	for key, value := range entity.Gates {
		gatesAny[key] = value
	}
	return fieldsJSON, gatesJSON, fieldsAny, gatesAny, nil
}

type scenarioSetupEntitySnapshot struct {
	FlowInstance string
	EntityType   string
	Fields       string
}

func validateExistingPostgresScenarioSetupEntity(ctx context.Context, tx *sql.Tx, runID string, entity runtimepipeline.ScenarioSetupEntityRequest, fieldsJSON json.RawMessage) error {
	var snapshot scenarioSetupEntitySnapshot
	err := tx.QueryRowContext(ctx, `
		SELECT flow_instance, entity_type, fields::text
		FROM entity_state
		WHERE run_id = $1::uuid AND entity_id = $2::uuid
	`, runID, entity.EntityID).Scan(&snapshot.FlowInstance, &snapshot.EntityType, &snapshot.Fields)
	if err == sql.ErrNoRows {
		return fmt.Errorf("postgres scenario setup entity %s insert conflicted but no existing row was visible", entity.Alias)
	}
	if err != nil {
		return fmt.Errorf("load existing postgres scenario setup entity %s: %w", entity.Alias, err)
	}
	return validateExistingScenarioSetupEntity(snapshot, entity, fieldsJSON)
}

func validateExistingSQLiteScenarioSetupEntity(ctx context.Context, tx *sql.Tx, runID string, entity runtimepipeline.ScenarioSetupEntityRequest, fieldsJSON json.RawMessage) error {
	var snapshot scenarioSetupEntitySnapshot
	err := tx.QueryRowContext(ctx, `
		SELECT flow_instance, entity_type, fields
		FROM entity_state
		WHERE run_id = ? AND entity_id = ?
	`, runID, entity.EntityID).Scan(&snapshot.FlowInstance, &snapshot.EntityType, &snapshot.Fields)
	if err == sql.ErrNoRows {
		return fmt.Errorf("sqlite scenario setup entity %s insert conflicted but no existing row was visible", entity.Alias)
	}
	if err != nil {
		return fmt.Errorf("load existing sqlite scenario setup entity %s: %w", entity.Alias, err)
	}
	return validateExistingScenarioSetupEntity(snapshot, entity, fieldsJSON)
}

func validateExistingScenarioSetupEntity(snapshot scenarioSetupEntitySnapshot, entity runtimepipeline.ScenarioSetupEntityRequest, fieldsJSON json.RawMessage) error {
	var mismatches []string
	if snapshot.FlowInstance != entity.FlowInstance {
		mismatches = append(mismatches, "flow_instance")
	}
	if snapshot.EntityType != entity.EntityType {
		mismatches = append(mismatches, "entity_type")
	}
	if !scenarioSetupJSONEqual(snapshot.Fields, fieldsJSON) {
		mismatches = append(mismatches, "fields")
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
	canonical, err := canonicaljson.Canonicalize([]byte(raw))
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
	return runtimemutationlog.Writer{
		Type: "platform",
		ID:   "test.setup_entities",
	}
}
