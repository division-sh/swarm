package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
)

// WorkflowInitialMaterializationRecord is the complete immutable persistence
// projection for one workflow instance creation. Runtime owns semantic
// planning; the selected store owns exact replay comparison and the complete
// transaction.
type WorkflowInitialMaterializationRecord struct {
	State             WorkflowEngineStateRecord
	ProjectionVersion int
	Projection        json.RawMessage
	OccurredAt        time.Time
	Readiness         json.RawMessage
}

func (r WorkflowInitialMaterializationRecord) Validate() error {
	if err := r.State.Validate(); err != nil {
		return fmt.Errorf("workflow initial materialization state: %w", err)
	}
	if !r.State.Transition.CreatesState() {
		return fmt.Errorf("workflow initial materialization requires a creating state record")
	}
	if r.ProjectionVersion != workflowInitialMaterializationProjectionVersion || len(r.Projection) == 0 || !json.Valid(r.Projection) {
		return fmt.Errorf("workflow initial materialization requires the canonical immutable projection")
	}
	if r.OccurredAt.IsZero() || !canonicalWorkflowInstancePersistedTime(r.OccurredAt).Equal(r.State.CreatedAt) {
		return fmt.Errorf("workflow initial materialization occurrence must equal state creation time")
	}
	if len(r.Readiness) > 0 && !json.Valid(r.Readiness) {
		return fmt.Errorf("workflow initial materialization readiness must be valid JSON")
	}
	return nil
}

type WorkflowInitialMaterializationCommand struct {
	Record    WorkflowInitialMaterializationRecord
	Lifecycle WorkflowLifecycleMutationPlan
}

func (c WorkflowInitialMaterializationCommand) Validate() error {
	if err := c.Record.Validate(); err != nil {
		return err
	}
	if err := c.Lifecycle.Validate(c.Record.State.Identity.RunID, c.Record.State.Identity.Route, c.Record.State.EntityID); err != nil {
		return fmt.Errorf("workflow initial materialization lifecycle: %w", err)
	}
	if c.Lifecycle.RequestCompletionCandidate {
		return fmt.Errorf("workflow initial materialization cannot request run completion")
	}
	return nil
}

type CommittedWorkflowInitialMaterialization struct {
	Result    WorkflowInitialMaterializationResult
	Lifecycle CommittedWorkflowLifecycleMutation
}

func (r CommittedWorkflowInitialMaterialization) Validate() error {
	if r.Result != WorkflowInitialMaterializationCreated && r.Result != WorkflowInitialMaterializationAlreadyExists {
		return fmt.Errorf("committed workflow initial materialization has no disposition")
	}
	if err := r.Lifecycle.Validate(); err != nil {
		return fmt.Errorf("committed workflow initial materialization lifecycle: %w", err)
	}
	if r.Result == WorkflowInitialMaterializationAlreadyExists && !emptyCommittedWorkflowLifecycleMutation(r.Lifecycle) {
		return fmt.Errorf("replayed workflow initial materialization cannot carry new lifecycle evidence")
	}
	return nil
}

func emptyCommittedWorkflowLifecycleMutation(committed CommittedWorkflowLifecycleMutation) bool {
	return len(committed.Wakeups) == 0 &&
		len(committed.Cancellations) == 0 &&
		len(committed.GenericScheduleActivations) == 0 &&
		len(committed.GenericScheduleCancellations) == 0
}

// WorkflowInitialMaterializationCommitOwner is the selected-store owner for
// exact initial creation and replay. No transaction, callback, or backend
// choice crosses this port.
type WorkflowInitialMaterializationCommitOwner interface {
	CommitWorkflowInitialMaterialization(context.Context, WorkflowInitialMaterializationCommand) (CommittedWorkflowInitialMaterialization, error)
}

func workflowInitialMaterializationRecord(
	state WorkflowEngineStateRecord,
	projection workflowInitialMaterializationProjection,
	readiness *DynamicFlowRuntimeReadinessPlan,
) (WorkflowInitialMaterializationRecord, error) {
	projectionJSON, err := canonicaljson.Bytes(projection)
	if err != nil {
		return WorkflowInitialMaterializationRecord{}, err
	}
	var readinessJSON json.RawMessage
	if readiness != nil {
		normalizedReadiness, normalizeErr := readiness.Normalized()
		if normalizeErr != nil {
			return WorkflowInitialMaterializationRecord{}, fmt.Errorf("normalize workflow initial materialization readiness: %w", normalizeErr)
		}
		readinessJSON, err = canonicaljson.Bytes(normalizedReadiness)
		if err != nil {
			return WorkflowInitialMaterializationRecord{}, err
		}
	}
	record := WorkflowInitialMaterializationRecord{
		State:             state,
		ProjectionVersion: workflowInitialMaterializationProjectionVersion,
		Projection:        projectionJSON,
		OccurredAt:        canonicalWorkflowInstancePersistedTime(projection.OccurredAt),
		Readiness:         readinessJSON,
	}
	if strings.TrimSpace(projection.RunID) == "" || state.Identity.RunID != strings.TrimSpace(projection.RunID) {
		return WorkflowInitialMaterializationRecord{}, fmt.Errorf("workflow initial materialization run identity disagrees with state")
	}
	if err := record.Validate(); err != nil {
		return WorkflowInitialMaterializationRecord{}, err
	}
	return record, nil
}
