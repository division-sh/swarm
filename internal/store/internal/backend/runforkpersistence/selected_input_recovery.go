package runforkpersistence

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	eventrecordpostgres "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/postgres"
	eventrecordsqlite "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/sqlite"
	"github.com/google/uuid"
)

// RequireSelectedForkInputPublished observes exact immutable publication
// evidence before receiver preflight. The caller supplies the already-projected,
// payload-admitted input and its selected schema, never a newly timed event.
func (s *RunForkPostgresOwner) RequireSelectedForkInputPublished(ctx context.Context, authority effects.Authority, sourceEvent runfork.RunForkSelectedContractSourceEvent, expectedSchema events.PayloadSchemaBinding) (bool, error) {
	if s == nil || s.backend == nil {
		return false, fmt.Errorf("postgres store is required")
	}
	if err := validateSelectedForkInputReadRequest(ctx, authority, sourceEvent, expectedSchema); err != nil {
		return false, err
	}
	if err := s.requireCurrentSchema(); err != nil {
		return false, err
	}
	var published bool
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		snapshot, err := s.LoadSnapshotTx(ctx, tx, authority.SelectedFork.ForkRunID, false)
		if err != nil {
			return err
		}
		binding, err := requireSelectedForkInputReadBindingTx(ctx, tx, snapshot, authority, expectedSchema, false)
		if err != nil {
			return err
		}
		published, err = readSelectedForkInputPublishedTx(ctx, tx, binding, sourceEvent, expectedSchema, false)
		return err
	})
	return published && err == nil, err
}

func (s *RunForkSQLiteOwner) RequireSelectedForkInputPublished(ctx context.Context, authority effects.Authority, sourceEvent runfork.RunForkSelectedContractSourceEvent, expectedSchema events.PayloadSchemaBinding) (bool, error) {
	if s == nil || s.backend == nil {
		return false, fmt.Errorf("sqlite store is required")
	}
	if err := validateSelectedForkInputReadRequest(ctx, authority, sourceEvent, expectedSchema); err != nil {
		return false, err
	}
	if err := s.requireCurrentSchema(); err != nil {
		return false, err
	}
	var published bool
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		snapshot, err := s.LoadSnapshotTx(ctx, tx, authority.SelectedFork.ForkRunID)
		if err != nil {
			return err
		}
		binding, err := requireSelectedForkInputReadBindingTx(ctx, tx, snapshot, authority, expectedSchema, true)
		if err != nil {
			return err
		}
		published, err = readSelectedForkInputPublishedTx(ctx, tx, binding, sourceEvent, expectedSchema, true)
		return err
	})
	return published && err == nil, err
}

func validateSelectedForkInputReadRequest(ctx context.Context, authority effects.Authority, sourceEvent runfork.RunForkSelectedContractSourceEvent, expectedSchema events.PayloadSchemaBinding) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !authority.Valid() || authority.Kind != effects.AuthoritySelectedContractFork {
		return fmt.Errorf("selected input publication read requires exact current selected authority")
	}
	id, err := uuid.Parse(sourceEvent.SourceEventID)
	if err != nil || id == uuid.Nil || id.String() != sourceEvent.SourceEventID || sourceEvent.EventName == "" || sourceEvent.EventName != strings.TrimSpace(sourceEvent.EventName) || !sourceEvent.ExecutionMode.Valid() {
		return fmt.Errorf("selected input publication read requires exact source event identity and mode")
	}
	if err := expectedSchema.Validate(); err != nil {
		return err
	}
	_, err = events.NewPayloadAdmission(sourceEvent.Payload, expectedSchema)
	return err
}

func requireSelectedForkInputReadBindingTx(ctx context.Context, tx *sql.Tx, snapshot runlifecycle.Snapshot, authority effects.Authority, expectedSchema events.PayloadSchemaBinding, sqlite bool) (runfork.RunForkSelectedContractBinding, error) {
	if snapshot.RunID != authority.SelectedFork.ForkRunID || snapshot.BundleHash != expectedSchema.BundleHash() {
		return runfork.RunForkSelectedContractBinding{}, fmt.Errorf("selected input schema differs from exact child artifact")
	}
	binding, err := loadRunForkSelectedContractBinding(ctx, tx, snapshot.RunID)
	if err != nil {
		return binding, err
	}
	record, err := loadSelectedRecoveryRecordTx(ctx, tx, snapshot, runfork.SelectedForkRecoveryEntry{
		Binding: binding, BundleHash: snapshot.BundleHash,
	}, sqlite, false)
	if err != nil {
		return binding, err
	}
	if !record.hasExecution || record.state != "running" || record.failure != nil || record.ExecutionID != authority.SelectedFork.ExecutionID {
		return binding, fmt.Errorf("selected input publication read requires the current running executor")
	}
	if err := requireSelectedInputCurrentExecutionTx(ctx, tx, authority, sqlite); err != nil {
		return binding, err
	}
	return binding, nil
}

// This is the selected runtime's current-fence predicate without active-run
// admission. Terminal children remain observable, but no authority is issued.
func requireSelectedInputCurrentExecutionTx(ctx context.Context, tx *sql.Tx, authority effects.Authority, sqlite bool) error {
	lease := "lease_expires_at>CURRENT_TIMESTAMP"
	if sqlite {
		lease = sqliteCurrentLeaseSQL
	}
	var current bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM run_fork_selected_contract_runtime_executions
		WHERE execution_id=$1 AND fork_run_id=$2 AND generation=$3 AND execution_owner=$4 AND fence_generation=$5
		AND admission_fingerprint=$6 AND container_plan_fingerprint=$7 AND actor_census_fingerprint=$8
		AND effective_config_fingerprint=$9 AND state='running' AND `+lease+`)`,
		authority.SelectedFork.ExecutionID, authority.SelectedFork.ForkRunID, authority.SelectedFork.Generation,
		authority.ExecutionOwner, authority.FenceGeneration, authority.SelectedFork.AdmissionFingerprint,
		authority.SelectedFork.ContainerPlanFingerprint, authority.SelectedFork.ActorCensusFingerprint, authority.SelectedFork.EffectiveConfigFingerprint).Scan(&current)
	if err != nil {
		return err
	}
	if !current {
		return fmt.Errorf("selected input publication read authority is no longer current")
	}
	return nil
}

func readSelectedForkInputPublishedTx(ctx context.Context, tx *sql.Tx, binding runfork.RunForkSelectedContractBinding, sourceEvent runfork.RunForkSelectedContractSourceEvent, expectedSchema events.PayloadSchemaBinding, sqlite bool) (bool, error) {
	eventID := activityidentity.ForkLineageEventID(binding.ForkRunID, sourceEvent.SourceEventID)
	lineage, lineageFound, err := readSelectedForkInputLineageTx(ctx, tx, binding.ForkRunID, sourceEvent.SourceEventID, eventID)
	if err != nil {
		return false, err
	}
	var admitted events.AdmittedEvent
	var found bool
	if sqlite {
		admitted, _, found, err = eventrecordsqlite.LoadAdmitted(ctx, tx, eventID)
	} else {
		admitted, _, found, err = eventrecordpostgres.LoadAdmitted(ctx, tx, eventID)
	}
	if err != nil {
		return false, err
	}
	if !lineageFound && !found {
		return false, nil
	}
	if !lineageFound || !found {
		return false, eventrecord.Corrupt(eventID, fmt.Errorf("selected input publication has partial event/lineage evidence"))
	}
	if err := validateSelectedForkPublishedInput(binding, lineage, admitted.Event(), sourceEvent, expectedSchema); err != nil {
		return false, eventrecord.Corrupt(eventID, err)
	}
	return true, nil
}

func readSelectedForkInputLineageTx(ctx context.Context, tx *sql.Tx, childRunID, sourceEventID, eventID string) (runfork.RunForkSelectedContractExecutionLineage, bool, error) {
	rows, err := tx.QueryContext(ctx, `SELECT CAST(fork_run_id AS TEXT), CAST(source_run_id AS TEXT),
		CAST(source_event_id AS TEXT), CAST(fork_event_id AS TEXT), event_name, selection_authority, created_at
		FROM run_fork_selected_contract_executions
		WHERE (fork_run_id=$1 AND source_event_id=$2) OR fork_event_id=$3`, childRunID, sourceEventID, eventID)
	if err != nil {
		return runfork.RunForkSelectedContractExecutionLineage{}, false, err
	}
	defer rows.Close()
	var lineage runfork.RunForkSelectedContractExecutionLineage
	var found bool
	for rows.Next() {
		if found {
			return lineage, false, eventrecord.Corrupt(eventID, fmt.Errorf("selected input publication has multiple lineage owners"))
		}
		var rawTime any
		if err := rows.Scan(&lineage.ForkRunID, &lineage.SourceRunID, &lineage.SourceEventID, &lineage.ForkEventID, &lineage.EventName, &lineage.SelectionAuthority, &rawTime); err != nil {
			return lineage, false, err
		}
		createdAt, present, err := sqliteTimeValue(rawTime)
		if err != nil || !present {
			return lineage, false, eventrecord.Corrupt(eventID, fmt.Errorf("selected input lineage has invalid creation time: %v", err))
		}
		lineage.CreatedAt, found = createdAt, true
	}
	if err := rows.Err(); err != nil {
		return lineage, false, err
	}
	return lineage, found, rows.Close()
}

func validateSelectedForkPublishedInput(binding runfork.RunForkSelectedContractBinding, lineage runfork.RunForkSelectedContractExecutionLineage, event events.Event, sourceEvent runfork.RunForkSelectedContractSourceEvent, expectedSchema events.PayloadSchemaBinding) error {
	if err := validateSelectedForkPublishedInputIdentity(binding, lineage, event, sourceEvent); err != nil {
		return err
	}
	if err := validateSelectedForkPublishedInputPayload(event, sourceEvent, expectedSchema); err != nil {
		return err
	}
	return validateSelectedForkPublishedInputFrame(binding, lineage, event, sourceEvent)
}

func validateSelectedForkPublishedInputIdentity(binding runfork.RunForkSelectedContractBinding, lineage runfork.RunForkSelectedContractExecutionLineage, event events.Event, sourceEvent runfork.RunForkSelectedContractSourceEvent) error {
	eventID := activityidentity.ForkLineageEventID(binding.ForkRunID, sourceEvent.SourceEventID)
	if lineage.ForkRunID != binding.ForkRunID || lineage.SourceRunID != binding.SourceRunID || lineage.SourceEventID != sourceEvent.SourceEventID || lineage.ForkEventID != eventID || lineage.EventName != sourceEvent.EventName {
		return fmt.Errorf("selected input lineage differs from exact source/child/event binding")
	}
	selected, ok := event.SelectedForkLineage()
	if !ok || event.AdmissionClass() != events.EventAdmissionSelectedForkReplay || event.ID() != eventID || event.RunID() != binding.ForkRunID ||
		selected.SourceRunID() != binding.SourceRunID || selected.SourceEventID() != sourceEvent.SourceEventID || selected.AuthorityStamp() != lineage.SelectionAuthority {
		return fmt.Errorf("selected input event differs from committed selected lineage")
	}
	return nil
}

func validateSelectedForkPublishedInputPayload(event events.Event, sourceEvent runfork.RunForkSelectedContractSourceEvent, expectedSchema events.PayloadSchemaBinding) error {
	if string(event.Type()) != sourceEvent.EventName || event.ExecutionMode() != sourceEvent.ExecutionMode || !bytes.Equal(event.Payload(), sourceEvent.Payload) || event.RoutingSource() != sourceEvent.RoutingSource {
		return fmt.Errorf("selected input publication changed immutable type/mode/payload/routing")
	}
	payload, ok := event.PayloadAdmission()
	if !ok || !payload.Binding().Equal(expectedSchema) {
		return fmt.Errorf("selected input publication changed its exact selected schema")
	}
	return nil
}

func validateSelectedForkPublishedInputFrame(binding runfork.RunForkSelectedContractBinding, lineage runfork.RunForkSelectedContractExecutionLineage, event events.Event, sourceEvent runfork.RunForkSelectedContractSourceEvent) error {
	// Compare the two retained timestamps, never a fresh publication timestamp.
	if !lineage.CreatedAt.Equal(event.CreatedAt()) || event.Producer().Type() != events.EventProducerPlatform || event.Producer().ID() != lineage.SelectionAuthority || event.TaskID() != "" || event.ParentEventID() != "" || event.ChainDepth() != 0 {
		return fmt.Errorf("selected input publication changed its original publication frame")
	}
	if original, present := sourceEvent.InputPublication.Event(); present && (original.RunID() != binding.SourceRunID || original.ID() != sourceEvent.SourceEventID || string(original.Type()) != sourceEvent.EventName || original.ExecutionMode() != sourceEvent.ExecutionMode) {
		return fmt.Errorf("selected input original publication differs from fixed source identity")
	}
	return nil
}
