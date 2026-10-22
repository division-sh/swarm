package activityjournal

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimeownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	storerunstate "github.com/division-sh/swarm/internal/store/internal/backend/runstate"
	"github.com/google/uuid"
)

// SelectedGrantFact is immutable attribution only. It carries no process,
// generation or execution permission, and is not a completed-execution receipt.
type SelectedGrantFact struct {
	FactID    string                         `json:"fact_id"`
	CreatedAt time.Time                      `json:"created_at"`
	Evidence  runtimeownership.GrantEvidence `json:"evidence"`
	Snapshot  json.RawMessage                `json:"snapshot"`
}

// ReadRetiredSelectedGrantHistoryTx reads all versions, not merely a retired
// head. The cohort owner must separately prove the complete execution set and
// its other terminal facts under the reset fence in this same transaction.
func ReadRetiredSelectedGrantHistoryTx(ctx context.Context, tx *sql.Tx, executionID string, sqlite bool) (facts []SelectedGrantFact, err error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	id, parseErr := uuid.Parse(executionID)
	if tx == nil || parseErr != nil || id == uuid.Nil || id.String() != executionID {
		return nil, errors.New("selected grant history requires its native transaction and exact execution UUID")
	}
	// The snapshot arm only discovers contradictory projections. A row found
	// through it must still pass the original exact durable-column decoder.
	snapshotExecution := `snapshot->'selected_fork'->>'execution_id'`
	if sqlite {
		snapshotExecution = `json_extract(snapshot,'$.selected_fork.execution_id')`
	}
	rows, err := tx.QueryContext(ctx, `SELECT CAST(fact_id AS TEXT) AS fact_id,created_at,
		CAST(grant_id AS TEXT) AS grant_id,CAST(process_authority_id AS TEXT) AS process_authority_id,process_owner_id,
		state_version,state,bundle_hash,CAST(runtime_instance_id AS TEXT) AS runtime_instance_id,
		runtime_generation,source_set_revision,CAST(selected_binding_id AS TEXT) AS selected_binding_id,
		CAST(selected_fork_run_id AS TEXT) AS selected_fork_run_id,
		CAST(selected_execution_id AS TEXT) AS selected_execution_id,snapshot
		FROM runtime_generation_grants WHERE selected_execution_id=$1 OR `+snapshotExecution+`=CAST($1 AS TEXT)
		ORDER BY grant_id,state_version`, executionID)
	if err != nil {
		return nil, fmt.Errorf("read complete selected grant history: %w", err)
	}
	defer func() {
		err = errors.Join(err, rows.Close())
		if err != nil {
			facts = nil
		}
	}()
	seenFacts := make(map[string]bool)
	for rows.Next() {
		var fact SelectedGrantFact
		var row activityExecutionGrantRow
		var createdAt any
		if err := rows.Scan(&fact.FactID, &createdAt,
			&row.grantID, &row.processAuthorityID, &row.processOwnerID, &row.stateVersion, &row.state, &row.bundleHash,
			&row.runtimeInstanceID, &row.runtimeGeneration, &row.sourceSetRevision,
			&row.selectedBindingID, &row.selectedRunID, &row.selectedExecutionID, &row.snapshot); err != nil {
			return nil, fmt.Errorf("decode selected grant history row: %w", err)
		}
		var present bool
		fact.CreatedAt, present, err = storerunstate.DecodeBindingTimestamp(createdAt)
		if err != nil {
			return nil, fmt.Errorf("decode selected grant history clock: %w", err)
		}
		factID, parseErr := uuid.Parse(fact.FactID)
		if parseErr != nil || factID == uuid.Nil || factID.String() != fact.FactID || seenFacts[fact.FactID] || !present || fact.CreatedAt.IsZero() {
			return nil, errors.New("selected grant history has invalid or repeated physical fact coordinates")
		}
		grantID, parseErr := uuid.Parse(row.grantID)
		if parseErr != nil || grantID == uuid.Nil || grantID.String() != row.grantID || row.stateVersion == 0 || row.stateVersion > math.MaxInt64 {
			return nil, errors.New("selected grant history has an inexact grant/version")
		}
		fact.Evidence, err = decodeActivityExecutionGrantRow(row, row.grantID, row.stateVersion)
		if err != nil {
			return nil, err
		}
		if fact.Evidence.SelectedFork == nil || fact.Evidence.SelectedFork.ExecutionID != executionID {
			return nil, errors.New("selected grant history has a foreign execution projection")
		}
		if err := validateSelectedGrantHistoryPrefix(facts, fact.Evidence); err != nil {
			return nil, err
		}
		fact.Snapshot = append(json.RawMessage(nil), row.snapshot...)
		seenFacts[fact.FactID] = true
		facts = append(facts, fact)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate selected grant history: %w", err)
	}
	if len(facts) != 0 && facts[len(facts)-1].Evidence.State != runtimeownership.GrantRetired {
		return nil, errors.New("selected grant history is not retired")
	}
	return facts, nil
}

func validateSelectedGrantHistoryPrefix(facts []SelectedGrantFact, next runtimeownership.GrantEvidence) error {
	if len(facts) == 0 {
		if next.StateVersion != 1 || next.State != runtimeownership.GrantPrepared {
			return errors.New("selected grant history lacks its original prepared version")
		}
		return nil
	}
	previous := facts[len(facts)-1].Evidence
	if previous.GrantID != next.GrantID || next.StateVersion != previous.StateVersion+1 {
		return errors.New("selected execution grant history has a missing, repeated or foreign version")
	}
	allowed := next.State == runtimeownership.GrantRetired && previous.State != runtimeownership.GrantRetired ||
		previous.State == runtimeownership.GrantPrepared && next.State == runtimeownership.GrantProbeSettled ||
		previous.State == runtimeownership.GrantProbeSettled && next.State == runtimeownership.GrantAdmitted
	if !allowed {
		return errors.New("selected grant history has contradictory recorded states")
	}
	// State and the one probe-settlement addition are the only mutable parts
	// of the original grant lifecycle; all captured ownership stays exact.
	previous.State, previous.StateVersion = next.State, next.StateVersion
	if next.State == runtimeownership.GrantProbeSettled {
		previous.ProbeSurfaceIDs = next.ProbeSurfaceIDs
	}
	want, err := canonicaljson.Bytes(previous)
	if err != nil {
		return err
	}
	got, err := canonicaljson.Bytes(next)
	if err != nil {
		return err
	}
	if string(want) != string(got) {
		return errors.New("selected grant history changed its captured ownership or probe evidence")
	}
	return nil
}
