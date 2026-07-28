package store

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
)

type dynamicAgentTopologyOwner struct {
	runID        string
	instancePath string
	plan         []byte
}

type dynamicAgentTopologyPlan struct {
	Agents []struct {
		AgentID        string `json:"agent_id"`
		ConfigRevision string `json:"config_revision"`
	} `json:"agents"`
}

func authorizePostgresDynamicAgentTopologyMutation(
	ctx context.Context,
	tx *sql.Tx,
	req runtimemanager.AgentLifecycleTransition,
) error {
	return authorizeDynamicAgentTopologyMutation(ctx, tx, req, false)
}

func authorizeSQLiteDynamicAgentTopologyMutation(
	ctx context.Context,
	tx *sql.Tx,
	req runtimemanager.AgentLifecycleTransition,
) error {
	return authorizeDynamicAgentTopologyMutation(ctx, tx, req, true)
}

func authorizeDynamicAgentTopologyMutation(
	ctx context.Context,
	tx *sql.Tx,
	req runtimemanager.AgentLifecycleTransition,
	sqlite bool,
) error {
	if !lifecycleTransitionChangesDesiredAgentTopology(req) {
		return nil
	}
	paths, err := lifecycleTransitionFlowPaths(ctx, tx, req, sqlite)
	if err != nil {
		return err
	}
	var owners []dynamicAgentTopologyOwner
	for _, path := range paths {
		items, err := loadDynamicAgentTopologyOwners(ctx, tx, path, sqlite)
		if err != nil {
			return err
		}
		owners = append(owners, items...)
	}
	declaringOwners, err := loadDynamicAgentTopologyOwnersForAgent(ctx, tx, req.AgentID, sqlite)
	if err != nil {
		return err
	}
	owners = uniqueDynamicAgentTopologyOwners(append(owners, declaringOwners...))
	if len(owners) == 0 {
		if req.DynamicTopology != nil {
			return dynamicAgentTopologyConflict(req, "readiness_owner_missing", nil)
		}
		return nil
	}
	if len(owners) != 1 {
		return dynamicAgentTopologyConflict(req, "readiness_owner_ambiguous", owners)
	}
	owner := owners[0]
	authority := req.DynamicTopology
	if authority == nil {
		return dynamicAgentTopologyConflict(req, "readiness_owner_required", owners)
	}
	runID, instancePath, planFingerprint, desiredPresent := authority.AuthorityFacts()
	if strings.TrimSpace(runID) != owner.runID ||
		strings.Trim(strings.TrimSpace(instancePath), "/") != owner.instancePath {
		return dynamicAgentTopologyConflict(req, "readiness_owner_mismatch", owners)
	}
	fingerprint, err := canonicaljson.HashRaw(owner.plan)
	if err != nil {
		return fmt.Errorf("hash dynamic agent topology readiness plan: %w", err)
	}
	if strings.TrimSpace(planFingerprint) != fingerprint {
		return dynamicAgentTopologyConflict(req, "readiness_plan_changed", owners)
	}
	var plan dynamicAgentTopologyPlan
	if err := canonicaljson.DecodeInto(owner.plan, &plan); err != nil {
		return fmt.Errorf("decode dynamic agent topology readiness plan: %w", err)
	}
	var desiredRevision string
	for _, agent := range plan.Agents {
		if strings.TrimSpace(agent.AgentID) != req.AgentID {
			continue
		}
		if desiredRevision != "" {
			return dynamicAgentTopologyConflict(req, "readiness_agent_duplicated", owners)
		}
		desiredRevision = strings.TrimSpace(agent.ConfigRevision)
	}
	if desiredPresent {
		if desiredRevision == "" || desiredRevision != strings.TrimSpace(req.ConfigRevision) {
			return dynamicAgentTopologyConflict(req, "readiness_agent_revision_mismatch", owners)
		}
	} else if desiredRevision != "" {
		return dynamicAgentTopologyConflict(req, "readiness_agent_still_declared", owners)
	}
	return nil
}

func uniqueDynamicAgentTopologyOwners(owners []dynamicAgentTopologyOwner) []dynamicAgentTopologyOwner {
	byKey := make(map[string]dynamicAgentTopologyOwner, len(owners))
	for _, owner := range owners {
		byKey[owner.runID+"\x00"+owner.instancePath] = owner
	}
	out := make([]dynamicAgentTopologyOwner, 0, len(byKey))
	for _, owner := range byKey {
		out = append(out, owner)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].runID != out[j].runID {
			return out[i].runID < out[j].runID
		}
		return out[i].instancePath < out[j].instancePath
	})
	return out
}

func lifecycleTransitionChangesDesiredAgentTopology(req runtimemanager.AgentLifecycleTransition) bool {
	if req.Agent != nil {
		return true
	}
	return req.TargetPhase == runtimemanager.AgentLifecycleTerminated &&
		strings.EqualFold(strings.TrimSpace(req.Trigger), "teardown")
}

func lifecycleTransitionFlowPaths(
	ctx context.Context,
	tx *sql.Tx,
	req runtimemanager.AgentLifecycleTransition,
	sqlite bool,
) ([]string, error) {
	paths := map[string]struct{}{}
	if req.Agent != nil {
		if path := strings.Trim(req.Agent.Config.CanonicalFlowPath(), "/"); path != "" {
			paths[path] = struct{}{}
		}
	}
	var current sql.NullString
	query := `SELECT flow_instance FROM agents WHERE agent_id = $1`
	if sqlite {
		query = `SELECT flow_instance FROM agents WHERE agent_id = ?`
	}
	err := tx.QueryRowContext(ctx, query, req.AgentID).Scan(&current)
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}
	if current.Valid {
		if path := strings.Trim(strings.TrimSpace(current.String), "/"); path != "" {
			paths[path] = struct{}{}
		}
	}
	out := make([]string, 0, len(paths))
	for path := range paths {
		out = append(out, path)
	}
	sort.Strings(out)
	return out, nil
}

func loadDynamicAgentTopologyOwners(
	ctx context.Context,
	tx *sql.Tx,
	instancePath string,
	sqlite bool,
) ([]dynamicAgentTopologyOwner, error) {
	query := `
		SELECT readiness.run_id::text, readiness.instance_id, readiness.plan::text
		FROM flow_instance_runtime_readiness readiness
		JOIN runs run ON run.run_id = readiness.run_id
		JOIN flow_instances instance ON instance.instance_id = readiness.instance_id
		WHERE readiness.instance_id = $1
		  AND run.status IN ('running', 'paused')
		  AND instance.status = 'active'
		  AND instance.terminated_at IS NULL
		ORDER BY readiness.run_id
		FOR UPDATE OF readiness
	`
	if sqlite {
		query = `
			SELECT readiness.run_id, readiness.instance_id, readiness.plan
			FROM flow_instance_runtime_readiness readiness
			JOIN runs run ON run.run_id = readiness.run_id
			JOIN flow_instances instance ON instance.instance_id = readiness.instance_id
			WHERE readiness.instance_id = ?
			  AND run.status IN ('running', 'paused')
			  AND instance.status = 'active'
			  AND instance.terminated_at IS NULL
			ORDER BY readiness.run_id
		`
	}
	rows, err := tx.QueryContext(ctx, query, instancePath)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dynamicAgentTopologyOwner
	for rows.Next() {
		var owner dynamicAgentTopologyOwner
		if err := rows.Scan(&owner.runID, &owner.instancePath, &owner.plan); err != nil {
			return nil, err
		}
		owner.runID = strings.TrimSpace(owner.runID)
		owner.instancePath = strings.Trim(strings.TrimSpace(owner.instancePath), "/")
		out = append(out, owner)
	}
	return out, rows.Err()
}

func loadDynamicAgentTopologyOwnersForAgent(
	ctx context.Context,
	tx *sql.Tx,
	agentID string,
	sqlite bool,
) ([]dynamicAgentTopologyOwner, error) {
	query := `
		SELECT readiness.run_id::text, readiness.instance_id, readiness.plan::text
		FROM flow_instance_runtime_readiness readiness
		JOIN runs run ON run.run_id = readiness.run_id
		JOIN flow_instances instance ON instance.instance_id = readiness.instance_id
		WHERE run.status IN ('running', 'paused')
		  AND instance.status = 'active'
		  AND instance.terminated_at IS NULL
		  AND readiness.plan @> jsonb_build_object(
		      'agents',
		      jsonb_build_array(jsonb_build_object('agent_id', $1::text))
		  )
		ORDER BY readiness.run_id, readiness.instance_id
		FOR UPDATE OF readiness
	`
	if sqlite {
		query = `
			SELECT readiness.run_id, readiness.instance_id, readiness.plan
			FROM flow_instance_runtime_readiness readiness
			JOIN runs run ON run.run_id = readiness.run_id
			JOIN flow_instances instance ON instance.instance_id = readiness.instance_id
			WHERE run.status IN ('running', 'paused')
			  AND instance.status = 'active'
			  AND instance.terminated_at IS NULL
			  AND EXISTS (
			      SELECT 1
			      FROM json_each(readiness.plan, '$.agents') agent
			      WHERE json_extract(agent.value, '$.agent_id') = ?
			  )
			ORDER BY readiness.run_id, readiness.instance_id
		`
	}
	rows, err := tx.QueryContext(ctx, query, strings.TrimSpace(agentID))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dynamicAgentTopologyOwner
	for rows.Next() {
		var owner dynamicAgentTopologyOwner
		if err := rows.Scan(&owner.runID, &owner.instancePath, &owner.plan); err != nil {
			return nil, err
		}
		owner.runID = strings.TrimSpace(owner.runID)
		owner.instancePath = strings.Trim(strings.TrimSpace(owner.instancePath), "/")
		out = append(out, owner)
	}
	return out, rows.Err()
}

func dynamicAgentTopologyConflict(
	req runtimemanager.AgentLifecycleTransition,
	reason string,
	owners []dynamicAgentTopologyOwner,
) error {
	ownerRunID := ""
	ownerInstancePath := ""
	if len(owners) == 1 {
		ownerRunID = owners[0].runID
		ownerInstancePath = owners[0].instancePath
	}
	return runtimefailures.New(
		runtimefailures.ClassLifecycleConflict,
		"dynamic_agent_topology_owned_by_readiness",
		"agent-lifecycle-store",
		req.OperationKind,
		map[string]any{
			"agent_id": req.AgentID, "reason": reason,
			"owner_run_id": ownerRunID, "owner_instance_path": ownerInstancePath,
		},
	)
}
