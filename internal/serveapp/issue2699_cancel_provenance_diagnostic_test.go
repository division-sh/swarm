package serveapp

import (
	"context"
	"encoding/json"
	"sort"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/storetest"
)

// Disposable exact-base discriminator authorized by reviewer-d6208681134.
// It reads only after the original accounting predicate observes cancellation.
func issue2699TraceCancellation(t *testing.T, ctx context.Context, rt issue2564H2Fixture, run string, snapshot issue2564H2Snapshot, timer issue2564H2Timer) {
	t.Helper()
	t.Logf("H2_CANCEL_CUT observed_at=%s run=%s activation=%s entity=%s instance=%s status=%s declaration=%s creation_cause=%s created=%s due=%s fired=%s",
		time.Now().UTC().Format(time.RFC3339Nano), run, timer.ID, timer.Entity, timer.Instance, timer.Status,
		timer.Ref.DeclarationKey, timer.Ref.Cause, timer.Created.Format(time.RFC3339Nano), timer.Due.Format(time.RFC3339Nano), timer.Fired.Format(time.RFC3339Nano))
	creationMatches, occurrenceMatches := 0, 0
	for _, hub := range snapshot.Hubs {
		if hub.Entity != timer.Entity {
			continue
		}
		t.Logf("H2_CANCEL_HEADER entity=%s stage=%s revision=%d", hub.Entity, hub.Stage, hub.Revision)
		for _, record := range hub.Cuts {
			candidate := timeridentity.WorkflowTimerActivationID(run, timer.Entity, timer.Instance, timer.Ref.DeclarationKey,
				timer.Ref.DeclarationRevision, string(timer.Ref.Cause), timer.Ref.Generation.KeySuffix(),
				record.TriggerEventID, "platform.stage_timer", record.TransitionID, record.From, record.To)
			if candidate == timer.ID {
				creationMatches++
				t.Logf("H2_CANCEL_CREATION_MATCH revision=%d trigger=%s transition=%s from=%s to=%s fired_at=%s",
					hub.CutRevisions[record.TriggerEventID], record.TriggerEventID, record.TransitionID, record.From, record.To, record.FiredAt.Format(time.RFC3339Nano))
			}
		}
	}
	for _, event := range snapshot.Events {
		if event.Occurrence.Activation == timer.Ref {
			occurrenceMatches++
			t.Logf("H2_CANCEL_OCCURRENCE event=%s outcome=%s reason=%s due=%s", event.ID, event.Outcome, event.Reason, event.Occurrence.DueAt.Format(time.RFC3339Nano))
		}
	}
	t.Logf("H2_CANCEL_IDENTITY creation_matches=%d occurrence_matches=%d", creationMatches, occurrenceMatches)
	header, err := rt.selected.LoadRunHeader(ctx, run)
	if err != nil {
		t.Logf("H2_CANCEL_RUN_READ error=%v", err)
	} else {
		t.Logf("H2_CANCEL_RUN_READ observed_at=%s state=%s ended_at=%v", time.Now().UTC().Format(time.RFC3339Nano), header.Status, header.EndedAt)
	}
	storage, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, rt.selected)
	if err != nil {
		t.Logf("H2_CANCEL_STORAGE_READ error=%v", err)
		return
	}
	rows, err := storetest.DecodeSelectedForkSnapshotRows(storage["run_fork_fact_revisions"])
	if err != nil {
		t.Logf("H2_CANCEL_LEDGER_DECODE error=%v", err)
		return
	}
	changedRevisions := map[string]bool{}
	for _, row := range rows {
		if issue2699CellText(row["run_id"]) != run || issue2699CellText(row["family"]) != "timers" || issue2699CellText(row["fact_key"]) != timer.ID {
			continue
		}
		revision := issue2699CellText(row["revision"])
		fact := map[string]json.RawMessage{}
		if err := json.Unmarshal([]byte(issue2699CellText(row["fact"])), &fact); err != nil {
			t.Logf("H2_CANCEL_TIMER_FACT_DECODE revision=%s error=%v", revision, err)
			continue
		}
		// Whitelist physical fields; never log the timer payload or source bytes.
		t.Logf("H2_CANCEL_TIMER_REVISION revision=%s present=%s status=%s created_at=%s fire_at=%s fired_at=%s", revision,
			issue2699CellText(row["present"]), fact["status"], fact["created_at"], fact["fire_at"], fact["fired_at"])
		var status string
		if json.Unmarshal(fact["status"], &status) == nil && status == "cancelled" {
			changedRevisions[revision] = true
		}
	}
	for _, row := range rows {
		if issue2699CellText(row["run_id"]) != run || !changedRevisions[issue2699CellText(row["revision"])] {
			continue
		}
		revision, family, key := issue2699CellText(row["revision"]), issue2699CellText(row["family"]), issue2699CellText(row["fact_key"])
		t.Logf("H2_CANCEL_COFACT revision=%s family=%s key=%s present=%s", revision, family, key, issue2699CellText(row["present"]))
		if family == "entity_metadata" && key == timer.Entity {
			var fact struct {
				Config struct {
					Stage   string                              `json:"current_state"`
					History []pipeline.WorkflowTransitionRecord `json:"transition_history"`
				} `json:"flow_config"`
			}
			if err := json.Unmarshal([]byte(issue2699CellText(row["fact"])), &fact); err != nil {
				t.Logf("H2_CANCEL_METADATA_DECODE error=%v", err)
				continue
			}
			for _, record := range fact.Config.History {
				t.Logf("H2_CANCEL_TRANSITION revision=%s stage=%s trigger=%s transition=%s from=%s to=%s fired_at=%s",
					revision, fact.Config.Stage, record.TriggerEventID, record.TransitionID, record.From, record.To, record.FiredAt.Format(time.RFC3339Nano))
			}
		}
	}
	for _, table := range []string{"runs", "run_control_state", "run_fork_revisions"} {
		rows, err := storetest.DecodeSelectedForkSnapshotRows(storage[table])
		if err != nil {
			t.Logf("H2_CANCEL_CONTROL_DECODE table=%s error=%v", table, err)
			continue
		}
		for _, row := range rows {
			if issue2699CellText(row["run_id"]) != run {
				continue
			}
			if table == "run_fork_revisions" && !changedRevisions[issue2699CellText(row["revision"])] {
				continue
			}
			fields := []string{}
			for _, key := range []string{"status", "control_status", "reason", "controlled_by", "revision", "recorded_at", "started_at", "ended_at", "updated_at"} {
				if value, exists := row[key]; exists {
					fields = append(fields, key+"="+issue2699CellText(value))
				}
			}
			sort.Strings(fields)
			t.Logf("H2_CANCEL_CONTROL table=%s fields=%v", table, fields)
		}
	}
	t.Logf("H2_CANCEL_TRACE_END observed_at=%s", time.Now().UTC().Format(time.RFC3339Nano))
}

func issue2699CellText(cell storetest.SelectedForkSnapshotCell) string {
	if cell.Type == "string" || cell.Type == "[]uint8" || cell.Type == "<nil>" {
		value, err := cell.Text()
		if err != nil {
			return "<decode-error>"
		}
		return value
	}
	return string(cell.Value)
}
