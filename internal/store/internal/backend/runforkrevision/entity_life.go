package runforkrevision

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/mutationlog"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

type EntityMetadataFact struct {
	FirstRevision            int64                                          `json:"-"`
	Revision                 int64                                          `json:"-"`
	HistoricalConstruction   *runfork.RunForkCapturedHistoricalConstruction `json:"-"`
	InitialProjectionVersion int                                            `json:"initial_projection_version"`
	InitialMaterialization   json.RawMessage                                `json:"initial_materialization"`
	FlowConfig               json.RawMessage                                `json:"flow_config"`
	EntityID                 string                                         `json:"entity_id"`
	FlowInstance             string                                         `json:"flow_instance"`
	ParentInstance           string                                         `json:"parent_instance"`
	InstanceKey              string                                         `json:"instance_key"`
	EntityType               string                                         `json:"entity_type"`
	Slug                     string                                         `json:"slug"`
	Name                     string                                         `json:"name"`
	CreatedAt                time.Time                                      `json:"created_at"`
	ConstructionKind         string                                         `json:"construction_kind"`
	StageDefined             bool                                           `json:"stage_defined"`
	Life                     int64                                          `json:"life"`
	InstanceRevision         int64                                          `json:"instance_revision"`
	FlowTemplate             string                                         `json:"flow_template"`
	Mode                     string                                         `json:"mode"`
	Status                   string                                         `json:"status"`
	CurrentState             string                                         `json:"current_state"`
	EnteredStateAt           time.Time                                      `json:"entered_state_at"`
	UpdatedAt                time.Time                                      `json:"updated_at"`
	TerminatedAt             time.Time                                      `json:"terminated_at"`
	DataCreatedAt            *time.Time                                     `json:"data_created_at"`
	DataUpdatedAt            *time.Time                                     `json:"data_updated_at"`
}

type entityLifeQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

// ReadEntityLife uses the existing immutable revision and mutation rows. First
// selects the initialization cut for constructor replay; otherwise the last
// retained cut of that life is returned. It grants no execution authority.
func ReadEntityLife(ctx context.Context, q entityLifeQueryer, runID, entityID string, life int64, first bool) (EntityMetadataFact, mutationlog.EntityStateProjection, bool, error) {
	var empty mutationlog.EntityStateProjection
	if q == nil || life <= 0 {
		return EntityMetadataFact{}, empty, false, fmt.Errorf("entity life read requires a positive life and existing row owner")
	}
	if _, err := uuid.Parse(runID); err != nil {
		return EntityMetadataFact{}, empty, false, err
	}
	if _, err := uuid.Parse(entityID); err != nil {
		return EntityMetadataFact{}, empty, false, err
	}
	rows, err := q.QueryContext(ctx, `SELECT revision,fact,present FROM run_fork_fact_revisions
		WHERE run_id=$1 AND family=$2 AND fact_key=$3 ORDER BY revision`, runID, FamilyEntityMetadata, entityID)
	if err != nil {
		return EntityMetadataFact{}, empty, false, err
	}
	var selected EntityMetadataFact
	for rows.Next() {
		var revision int64
		var raw []byte
		var present bool
		if err := rows.Scan(&revision, &raw, &present); err != nil {
			rows.Close()
			return EntityMetadataFact{}, empty, false, err
		}
		var fact EntityMetadataFact
		if err := canonicaljson.DecodeInto(raw, &fact); err != nil {
			rows.Close()
			return EntityMetadataFact{}, empty, false, err
		}
		if fact.EntityID != entityID {
			rows.Close()
			return EntityMetadataFact{}, empty, false, fmt.Errorf("entity revision key disagrees with its retained identity")
		}
		if present && fact.ConstructionKind == "constructed" && fact.Life == life && (!first || selected.Revision == 0) {
			fact.Revision = revision
			selected = fact
		}
	}
	err = rows.Err()
	closeErr := rows.Close()
	if err != nil {
		return EntityMetadataFact{}, empty, false, err
	}
	if closeErr != nil || selected.Revision == 0 {
		return EntityMetadataFact{}, empty, false, closeErr
	}
	projection, err := readEntityProjectionAtRevision(ctx, q, runID, entityID, selected.Revision)
	if err != nil {
		return EntityMetadataFact{}, empty, false, err
	}
	if projection.CurrentState == nil || *projection.CurrentState != selected.CurrentState {
		return EntityMetadataFact{}, empty, false, fmt.Errorf("entity life metadata and retained mutation state disagree")
	}
	return selected, projection, true, nil
}

func readEntityProjectionAtRevision(ctx context.Context, q entityLifeQueryer, runID, entityID string, revision int64) (mutationlog.EntityStateProjection, error) {
	rows, err := q.QueryContext(ctx, `SELECT r.revision,r.fact_key,r.fact,r.present
		FROM run_fork_fact_revisions r WHERE r.run_id=$1 AND r.family=$2 AND r.revision<=$3
		AND r.fact_key IN (SELECT CAST(mutation_id AS TEXT) FROM entity_mutations WHERE run_id=$1 AND entity_id=$4)
		ORDER BY r.revision,r.fact_key`, runID, FamilyEntityMutations, revision, entityID)
	if err != nil {
		return mutationlog.EntityStateProjection{}, err
	}
	defer rows.Close()
	type retained struct {
		first int64
		value mutationlog.ProjectionMutation
	}
	mutations := map[string]retained{}
	for rows.Next() {
		var at int64
		var key string
		var raw []byte
		var present bool
		if err := rows.Scan(&at, &key, &raw, &present); err != nil {
			return mutationlog.EntityStateProjection{}, err
		}
		var fact struct {
			EntityID string             `json:"entity_id"`
			Domain   mutationlog.Domain `json:"domain"`
			Path     string             `json:"path"`
			NewValue json.RawMessage    `json:"new_value"`
		}
		if err := canonicaljson.DecodeInto(raw, &fact); err != nil {
			return mutationlog.EntityStateProjection{}, err
		}
		if fact.EntityID != entityID || !present {
			return mutationlog.EntityStateProjection{}, fmt.Errorf("entity life mutation has missing or foreign evidence")
		}
		var value any
		if err := canonicaljson.DecodePreservingNumberLexemes(fact.NewValue, &value); err != nil {
			return mutationlog.EntityStateProjection{}, err
		}
		value, err = canonicaljson.CloneRuntimeValue(value)
		if err != nil {
			return mutationlog.EntityStateProjection{}, err
		}
		first := at
		if previous, ok := mutations[key]; ok {
			first = previous.first
		}
		mutations[key] = retained{first, mutationlog.ProjectionMutation{Domain: fact.Domain, Path: fact.Path, NewValue: value}}
	}
	if err := rows.Err(); err != nil {
		return mutationlog.EntityStateProjection{}, err
	}
	keys := make([]string, 0, len(mutations))
	for key := range mutations {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		a, b := mutations[keys[i]], mutations[keys[j]]
		return a.first < b.first || a.first == b.first && keys[i] < keys[j]
	})
	ordered := make([]mutationlog.ProjectionMutation, 0, len(keys))
	for _, key := range keys {
		ordered = append(ordered, mutations[key].value)
	}
	return mutationlog.ReconstructEntityStateProjection(ordered)
}
