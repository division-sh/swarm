package runforkpersistence

import (
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

const (
	runForkWorkflowTimerInventoryOwner = "store.run_fork.workflow_timer_inventory"
	runForkWorkflowTimerReadbackOwner  = "store.run_fork.workflow_timer_materialization"
	runForkWorkflowTimerPendingPrefix  = "ordinary_workflow_timer_pending_readback:"
	runForkWorkflowTimerAppliedPrefix  = "ordinary_workflow_timer_native_readback:"
)

type runForkTimerHistoryInventory struct {
	Complete           bool
	SourceRunID        string
	Point              runfork.RunForkPoint
	WorkflowTimerIDs   []string
	ActiveTimerIDs     []string
	UnresolvedTimerIDs []string
	RecordDigest       string
}

func runForkWorkflowTimerRecordInventory(sourceRunID string, records []pipeline.WorkflowTimerActivationPersistenceRecord) (runForkTimerHistoryInventory, error) {
	canonical := make([]pipeline.WorkflowTimerActivationPersistenceRecord, 0, len(records))
	seen := make(map[string]struct{}, len(records))
	inventory := runForkTimerHistoryInventory{SourceRunID: sourceRunID}
	for _, record := range records {
		activation, err := pipeline.DecodeWorkflowTimerActivationPersistenceRecord(record)
		if err != nil {
			return runForkTimerHistoryInventory{}, err
		}
		if activation.RunID != sourceRunID {
			return runForkTimerHistoryInventory{}, fmt.Errorf("workflow timer inventory contains a foreign run")
		}
		id := activation.Ref.ActivationID
		if _, duplicate := seen[id]; duplicate {
			return runForkTimerHistoryInventory{}, fmt.Errorf("workflow timer inventory repeats activation %s", id)
		}
		seen[id] = struct{}{}
		inventory.WorkflowTimerIDs = append(inventory.WorkflowTimerIDs, id)
		if activation.Status == "active" {
			inventory.ActiveTimerIDs = append(inventory.ActiveTimerIDs, id)
		}
		// The canonical decoder owns interpretation; the certificate binds the
		// admitted primitive fields exactly, without normalizing later edits.
		canonical = append(canonical, record)
	}
	sort.Strings(inventory.WorkflowTimerIDs)
	sort.Strings(inventory.ActiveTimerIDs)
	sort.Slice(canonical, func(i, j int) bool { return canonical[i].ActivationID < canonical[j].ActivationID })
	digest, err := canonicaljson.Hash(canonical)
	if err != nil {
		return runForkTimerHistoryInventory{}, err
	}
	inventory.RecordDigest = digest
	return inventory, nil
}

func (i runForkTimerHistoryInventory) pendingCertificate() (string, error) {
	source, err := uuid.Parse(i.SourceRunID)
	if !i.Complete || err != nil || source == uuid.Nil || source.String() != i.SourceRunID ||
		len(i.WorkflowTimerIDs) == 0 || len(i.UnresolvedTimerIDs) != 0 || i.RecordDigest == "" {
		return "", fmt.Errorf("timer history lacks complete exact ordinary workflow inventory")
	}
	if err := i.Point.Validate(); err != nil {
		return "", err
	}
	if !slices.IsSorted(i.WorkflowTimerIDs) || !slices.IsSorted(i.ActiveTimerIDs) {
		return "", fmt.Errorf("timer history inventory is not canonical")
	}
	ids := make(map[string]struct{}, len(i.WorkflowTimerIDs))
	for _, id := range i.WorkflowTimerIDs {
		if _, duplicate := ids[id]; duplicate || id == "" {
			return "", fmt.Errorf("timer history inventory has duplicate or empty identity")
		}
		ids[id] = struct{}{}
	}
	active := make(map[string]struct{}, len(i.ActiveTimerIDs))
	for _, id := range i.ActiveTimerIDs {
		if _, present := ids[id]; !present {
			return "", fmt.Errorf("timer history active identity is outside its complete inventory")
		}
		if _, duplicate := active[id]; duplicate {
			return "", fmt.Errorf("timer history repeats active identity")
		}
		active[id] = struct{}{}
	}
	digest, err := canonicaljson.Hash(struct {
		SourceRunID string
		Point       runfork.RunForkPoint
		RowCount    int
		ActiveCount int
		Terminal    int
		IDs         []string
		ActiveIDs   []string
		Records     string
	}{i.SourceRunID, i.Point, len(i.WorkflowTimerIDs), len(i.ActiveTimerIDs), len(i.WorkflowTimerIDs) - len(i.ActiveTimerIDs),
		i.WorkflowTimerIDs, i.ActiveTimerIDs, i.RecordDigest})
	if err != nil {
		return "", err
	}
	return runForkWorkflowTimerPendingPrefix + digest, nil
}

// This permits postponing only the timer blocker during materialization. It
// never discharges source admission or grants execution authority.
func runForkWorkflowTimerHistoryMaterializable(plan runfork.RunForkPlan) (bool, error) {
	inventory, err := runForkWorkflowTimerRecordInventory(plan.SourceRunID, plan.WorkflowTimers)
	if err != nil {
		return false, err
	}
	inventory.Complete, inventory.Point = true, plan.ForkPoint
	certificate, err := inventory.pendingCertificate()
	if err != nil {
		return false, nil
	}
	admission := plan.ReplayResumeAdmission
	if admission.Owner != runfork.RunForkReplayResumeAdmissionOwner {
		return false, nil
	}
	facts, blockers := 0, 0
	for _, disposition := range admission.Dispositions {
		if disposition.Fact != runfork.RunForkReplayResumeFactTimerHistory {
			continue
		}
		facts++
		if disposition.Owner != runForkWorkflowTimerInventoryOwner || disposition.Classification != certificate ||
			disposition.Disposition != runfork.RunForkReplayResumeDispositionFailClosedBlocker || disposition.BlockerCode != runfork.RunForkBlockerTimerHistoryUnproven {
			return false, nil
		}
	}
	for _, blocker := range admission.UnsupportedBlockers {
		if blocker.Code == runfork.RunForkBlockerTimerHistoryUnproven {
			blockers++
		}
	}
	return facts == 1 && blockers == 1, nil
}

func runForkWorkflowTimerAppliedCertificate(pending, forkRunID string, bornAt time.Time, projected []runForkWorkflowTimerProjection) (string, error) {
	type projection struct {
		Record  pipeline.WorkflowTimerActivationPersistenceRecord
		Removed bool
	}
	records := make([]projection, 0, len(projected))
	removed := 0
	for _, timer := range projected {
		records = append(records, projection{timer.activation.PersistenceRecord(), timer.removed})
		if timer.removed {
			removed++
		}
	}
	sort.Slice(records, func(i, j int) bool { return records[i].Record.ActivationID < records[j].Record.ActivationID })
	digest, err := canonicaljson.Hash(struct {
		Inventory     string
		ChildRunID    string
		ChildBornAt   time.Time
		ReadbackCount int
		RemovedCount  int
		Projections   []projection
	}{pending, forkRunID, bornAt, len(records), removed, records})
	if err != nil {
		return "", err
	}
	return runForkWorkflowTimerAppliedPrefix + digest, nil
}
