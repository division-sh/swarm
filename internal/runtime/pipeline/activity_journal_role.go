package pipeline

import "errors"

type activityJournalRole struct {
	journal ActivityAttemptJournal
}

// InstallActivityJournal is called by generation assembly before dispatch. Each
// dispatched activity retains its own journal across later generation changes.
func (pc *PipelineCoordinator) InstallActivityJournal(journal ActivityAttemptJournal) error {
	if pc == nil || journal == nil {
		return errors.New("activity journal installation requires its coordinator and captured journal")
	}
	pc.activityJournalRole.Store(&activityJournalRole{journal: journal})
	return nil
}

func (d *pipelineActivityDispatcher) captureActivityJournal() {
	if d.journal != nil || d.coordinator == nil {
		return
	}
	if role := d.coordinator.activityJournalRole.Load(); role != nil {
		d.journal = role.journal
		return
	}
	if d.coordinator.workflowStore != nil {
		// The configured native owner is read-only until a retained session binds
		// its installed role. Its existing writer refuses an unbound execution.
		d.journal = d.coordinator.workflowStore.activityJournal
	}
}
