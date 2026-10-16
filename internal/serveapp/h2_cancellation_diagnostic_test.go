package serveapp

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"runtime"
	"slices"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testpostgres"
)

// Disposable diagnostic: native SQL logging belongs to this test's private server.
func TestH2CancellationProvenanceDiagnostic(t *testing.T) {
	t.Logf("H2_DIAGNOSTIC_SCHEDULER gomaxprocs=%d gogc=%q godebug=%q", runtime.GOMAXPROCS(0), os.Getenv("GOGC"), os.Getenv("GODEBUG"))
	probe := testpostgres.StartCapacityProbe(t, 300)
	t.Setenv(testpostgres.SourceEnv, probe.DSN)
	t.Cleanup(func() {
		path := os.Getenv("AGENT_E_H2_DIAGNOSTIC_LOG")
		if path == "" {
			t.Fatal("explicit diagnostic log destination is required")
		}
		file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			t.Errorf("open private SQL trace: %v", err)
			return
		}
		compressed := gzip.NewWriter(file)
		_, writeErr := io.WriteString(compressed, probe.Log(t))
		for _, err := range []error{writeErr, compressed.Close(), file.Close()} {
			if err != nil {
				t.Errorf("retain private SQL trace: %v", err)
			}
		}
		t.Logf("H2_NATIVE_SQL_TRACE path=%s", path)
	})
	TestIssue2564ReconstructedEquivalentH2BothStores(t)
}

func traceH2CancellationDiagnostic(t *testing.T, ctx context.Context, rt issue2564H2Fixture, run string, snapshot issue2564H2Snapshot, timer issue2564H2Timer) {
	t.Helper()
	t.Logf("H2_CANCEL observed=%s run=%s timer=%+v", time.Now().UTC().Format(time.RFC3339Nano), run, timer)
	traceH2DiagnosticInventory(t, "pre_close_cancel", run, snapshot)
	for _, hub := range snapshot.Hubs {
		if hub.Entity != timer.Entity {
			continue
		}
		t.Logf("H2_CANCEL_HEADER entity=%s stage=%s revision=%d", hub.Entity, hub.Stage, hub.Revision)
		for event, cut := range hub.Cuts {
			candidate := timeridentity.WorkflowTimerActivationID(run, timer.Entity, timer.Instance, timer.Ref.DeclarationKey,
				timer.Ref.DeclarationRevision, string(timer.Ref.Cause), timer.Ref.Generation.KeySuffix(),
				cut.TriggerEventID, "platform.stage_timer", cut.TransitionID, cut.From, cut.To)
			if candidate == timer.ID {
				t.Logf("H2_CANCEL_CREATOR event=%s revision=%d transition=%s from=%s to=%s time=%s", event, hub.CutRevisions[event], cut.TransitionID, cut.From, cut.To, cut.FiredAt.Format(time.RFC3339Nano))
			}
		}
	}
	storage, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, rt.selected)
	if err != nil {
		t.Logf("H2_CANCEL_SNAPSHOT_ERROR %v", err)
		return
	}
	rows, err := storetest.DecodeSelectedForkSnapshotRows(storage["run_fork_fact_revisions"])
	if err != nil {
		t.Logf("H2_CANCEL_HISTORY_ERROR %v", err)
		return
	}
	revisions := map[string]bool{}
	for _, row := range rows {
		if h2DiagnosticCell(row["run_id"]) == run && h2DiagnosticCell(row["family"]) == "timers" && h2DiagnosticCell(row["fact_key"]) == timer.ID {
			var fact struct {
				Status string `json:"status"`
			}
			if err := json.Unmarshal([]byte(h2DiagnosticCell(row["fact"])), &fact); err != nil {
				t.Logf("H2_CANCEL_FACT_ERROR %v", err)
				continue
			}
			t.Logf("H2_CANCEL_TIMER_FACT revision=%s present=%s status=%s", h2DiagnosticCell(row["revision"]), h2DiagnosticCell(row["present"]), fact.Status)
			if fact.Status == "cancelled" {
				revisions[h2DiagnosticCell(row["revision"])] = true
			}
		}
	}
	for _, row := range rows {
		if h2DiagnosticCell(row["run_id"]) != run || !revisions[h2DiagnosticCell(row["revision"])] {
			continue
		}
		revision, family, key := h2DiagnosticCell(row["revision"]), h2DiagnosticCell(row["family"]), h2DiagnosticCell(row["fact_key"])
		t.Logf("H2_CANCEL_COFACT revision=%s family=%s key=%s present=%s", revision, family, key, h2DiagnosticCell(row["present"]))
		if family == "entity_metadata" && key == timer.Entity {
			var fact struct {
				Config struct {
					Stage   string                              `json:"current_state"`
					History []pipeline.WorkflowTransitionRecord `json:"transition_history"`
				} `json:"flow_config"`
			}
			if err := json.Unmarshal([]byte(h2DiagnosticCell(row["fact"])), &fact); err != nil {
				t.Logf("H2_CANCEL_METADATA_ERROR %v", err)
				continue
			}
			for _, cut := range fact.Config.History {
				t.Logf("H2_CANCEL_TRANSITION revision=%s stage=%s event=%s transition=%s from=%s to=%s time=%s", revision, fact.Config.Stage, cut.TriggerEventID, cut.TransitionID, cut.From, cut.To, cut.FiredAt.Format(time.RFC3339Nano))
			}
		}
	}
}

func traceH2DiagnosticInventory(t *testing.T, phase, run string, snapshot issue2564H2Snapshot) {
	t.Helper()
	ids := make([]string, 0, len(snapshot.Timers))
	for id := range snapshot.Timers {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	rows := make([]issue2564H2Timer, 0, len(ids))
	for _, id := range ids {
		rows = append(rows, snapshot.Timers[id])
	}
	encoded, err := json.Marshal(struct {
		Phase    string
		Run      string
		Observed time.Time
		Timers   []issue2564H2Timer
		Hubs     map[string]issue2564H2Hub
		Events   map[string]issue2564H2Event
	}{phase, run, time.Now().UTC(), rows, snapshot.Hubs, snapshot.Events})
	if err != nil {
		t.Errorf("encode diagnostic inventory: %v", err)
		return
	}
	t.Logf("H2_DIAGNOSTIC_INVENTORY %s", encoded)
}

func h2DiagnosticCell(cell storetest.SelectedForkSnapshotCell) string {
	if cell.Type == "string" || cell.Type == "[]uint8" || cell.Type == "<nil>" {
		value, err := cell.Text()
		if err != nil {
			return fmt.Sprintf("<decode-error:%v>", err)
		}
		return value
	}
	return string(cell.Value)
}
