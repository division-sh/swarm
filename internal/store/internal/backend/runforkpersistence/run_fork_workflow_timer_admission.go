package runforkpersistence

import (
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

const (
	runForkTimerInventoryOwner = "store.run_fork.timer_history_inventory"
	runForkTimerReadbackOwner  = "store.run_fork.timer_history_materialization"
	runForkTimerPendingPrefix  = "timer_history_pending_readback:"
	runForkTimerAppliedPrefix  = "timer_history_native_readback:"
)

type runForkTimerHistoryInventory struct {
	Complete                  bool
	SourceRunID               string
	Point                     runfork.RunForkPoint
	WorkflowTimerIDs          []string
	ActiveTimerIDs            []string
	ArrivalScheduleIDs        []string
	TransferredPublicationIDs []string
	UnresolvedTimerIDs        []string
	RecordDigest              string
}

func runForkTimerRecordInventory(sourceRunID string, records []pipeline.WorkflowTimerActivationPersistenceRecord, arrivals []genericschedule.Activation, transferred ...genericschedule.TransferredJoinOccurrence) (runForkTimerHistoryInventory, error) {
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
	arrivalEvidence, err := runForkArrivalScheduleEvidence(sourceRunID, arrivals, seen)
	if err != nil {
		return runForkTimerHistoryInventory{}, err
	}
	for _, row := range arrivalEvidence {
		inventory.ArrivalScheduleIDs = append(inventory.ArrivalScheduleIDs, row.ID)
	}
	transferredEvidence, err := runForkTransferredJoinEvidence(sourceRunID, transferred)
	if err != nil {
		return runForkTimerHistoryInventory{}, err
	}
	for _, row := range transferredEvidence {
		inventory.TransferredPublicationIDs = append(inventory.TransferredPublicationIDs, row.ID)
	}
	digest, err := canonicaljson.Hash(struct {
		Workflow    []pipeline.WorkflowTimerActivationPersistenceRecord
		Arrival     []runForkArrivalScheduleRecord
		Transferred []runForkArrivalScheduleRecord
	}{canonical, arrivalEvidence, transferredEvidence})
	if err != nil {
		return runForkTimerHistoryInventory{}, err
	}
	inventory.RecordDigest = digest
	return inventory, nil
}

type runForkArrivalScheduleRecord struct{ ID, Evidence string }

func runForkArrivalScheduleEvidence(sourceRunID string, arrivals []genericschedule.Activation, seen map[string]struct{}) ([]runForkArrivalScheduleRecord, error) {
	rows := make([]runForkArrivalScheduleRecord, 0, len(arrivals))
	for _, schedule := range arrivals {
		if err := schedule.Validate(); err != nil {
			return nil, err
		}
		_, ref, valid := timeridentity.ParseJoinHandle(schedule.Command.Payload.Interface().(map[string]any))
		if !valid || ref.Mode() != timeridentity.JoinRefModeArrival || schedule.Command.RunID != sourceRunID {
			return nil, fmt.Errorf("timer history contains a foreign or non-arrival schedule")
		}
		if _, duplicate := seen[schedule.ID]; duplicate {
			return nil, fmt.Errorf("timer history repeats activation %s", schedule.ID)
		}
		seen[schedule.ID] = struct{}{}
		digest, err := schedule.EvidenceDigest()
		if err != nil {
			return nil, err
		}
		rows = append(rows, runForkArrivalScheduleRecord{schedule.ID, digest})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].ID < rows[j].ID })
	return rows, nil
}

func (i runForkTimerHistoryInventory) pendingCertificate() (string, error) {
	source, err := uuid.Parse(i.SourceRunID)
	if !i.Complete || err != nil || source == uuid.Nil || source.String() != i.SourceRunID ||
		len(i.WorkflowTimerIDs)+len(i.ArrivalScheduleIDs)+len(i.TransferredPublicationIDs) == 0 || len(i.UnresolvedTimerIDs) != 0 || i.RecordDigest == "" {
		return "", fmt.Errorf("timer history lacks complete exact workflow and arrival inventory")
	}
	if err := i.Point.Validate(); err != nil {
		return "", err
	}
	if !slices.IsSorted(i.WorkflowTimerIDs) || !slices.IsSorted(i.ActiveTimerIDs) || !slices.IsSorted(i.ArrivalScheduleIDs) || !slices.IsSorted(i.TransferredPublicationIDs) {
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
	for _, id := range i.ArrivalScheduleIDs {
		if _, duplicate := ids[id]; duplicate || id == "" {
			return "", fmt.Errorf("timer history repeats or omits an arrival identity")
		}
		ids[id] = struct{}{}
	}
	for _, id := range i.TransferredPublicationIDs {
		if _, duplicate := ids[id]; duplicate || id == "" {
			return "", fmt.Errorf("timer history repeats or omits a transferred publication identity")
		}
		ids[id] = struct{}{}
	}
	digest, err := canonicaljson.Hash(struct {
		SourceRunID    string
		Point          runfork.RunForkPoint
		RowCount       int
		ActiveCount    int
		Terminal       int
		IDs            []string
		ActiveIDs      []string
		ArrivalIDs     []string
		TransferredIDs []string
		Records        string
	}{i.SourceRunID, i.Point, len(i.WorkflowTimerIDs), len(i.ActiveTimerIDs), len(i.WorkflowTimerIDs) - len(i.ActiveTimerIDs),
		i.WorkflowTimerIDs, i.ActiveTimerIDs, i.ArrivalScheduleIDs, i.TransferredPublicationIDs, i.RecordDigest})
	if err != nil {
		return "", err
	}
	return runForkTimerPendingPrefix + digest, nil
}

// This permits postponing only the timer blocker during materialization. It
// never discharges source admission or grants execution authority.
func runForkTimerHistoryMaterializable(plan runfork.RunForkPlan) (bool, error) {
	inventory, err := runForkTimerRecordInventory(plan.SourceRunID, plan.WorkflowTimers, plan.JoinSchedules, plan.TransferredJoins...)
	if err != nil {
		return false, err
	}
	inventory.Complete, inventory.Point = true, plan.ForkPoint
	certificate, err := inventory.pendingCertificate()
	if err != nil {
		return false, nil
	}
	for _, schedule := range plan.JoinSchedules {
		if schedule.Status == genericschedule.StatusFired {
			publication, found := plan.HistoricalArrivalPublication(schedule.CurrentEventID)
			if !found {
				return false, nil
			}
			if _, err := schedule.ValidatePublishedOccurrence(publication.Event()); err != nil {
				return false, err
			}
			continue
		}
		if err := schedule.ValidateForkJoinRestorationSource(); err != nil {
			return false, nil
		}
	}
	for _, source := range plan.TransferredJoins {
		publication, found := plan.HistoricalArrivalPublication(source.Publication.EventID)
		if !found {
			// Retained intention is readable, but not committed source publication.
			return false, nil
		}
		if err := source.ValidateEvent(publication.Event()); err != nil {
			return false, err
		}
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
		if disposition.Owner != runForkTimerInventoryOwner || disposition.Classification != certificate ||
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

func runForkTimerAppliedCertificate(pending, forkRunID string, bornAt time.Time, projected []runForkWorkflowTimerProjection, arrival []genericschedule.Activation, published []runfork.InputPublicationCoordinates, transferred ...genericschedule.TransferredJoinOccurrence) (string, error) {
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
	seen := make(map[string]struct{}, len(records))
	for _, record := range records {
		seen[record.Record.ActivationID] = struct{}{}
	}
	arrivals, err := runForkArrivalScheduleEvidence(forkRunID, arrival, seen)
	if err != nil {
		return "", err
	}
	transferEvidence, err := runForkTransferredJoinEvidence(forkRunID, transferred)
	if err != nil {
		return "", err
	}
	digest, err := canonicaljson.Hash(struct {
		Inventory     string
		ChildRunID    string
		ChildBornAt   time.Time
		ReadbackCount int
		RemovedCount  int
		Projections   []projection
		Arrival       []runForkArrivalScheduleRecord
		Published     []runfork.InputPublicationCoordinates
		Transferred   []runForkArrivalScheduleRecord
	}{pending, forkRunID, bornAt, len(records) + len(arrivals) + len(transferred), removed, records, arrivals, published, transferEvidence})
	if err != nil {
		return "", err
	}
	return runForkTimerAppliedPrefix + digest, nil
}
