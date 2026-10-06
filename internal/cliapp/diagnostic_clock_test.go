package cliapp

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/genericschedule"
)

func TestDiagnosticClockRowsUseDurableStatusAndDue(t *testing.T) {
	due := time.Date(2026, 10, 2, 14, 32, 0, 0, time.UTC)
	rows := diagnosticClockRows([]genericschedule.ClockReadback{
		{FlowID: ".", Name: "poll", Status: genericschedule.StatusActive, NextDueAt: &due, RetainsRun: true},
		{FlowID: "worker", Name: "poll", Status: genericschedule.StatusCancelled, CancelCause: "clock_removed"},
		{FlowID: "worker", Name: "retry", Status: genericschedule.StatusFailed, Failure: &genericschedule.ClockFailure{Code: "dispatch_failed", Message: "clock publication failed"}},
	})
	if len(rows) != 3 || rows[0].Label != "clock" || rows[0].Value != "./poll: active; next due 2026-10-02T14:32:00Z; retaining this run" {
		t.Fatalf("active clock lost exact due/retention: %#v", rows)
	}
	if rows[1].Value != "worker/poll: cancelled; clock_removed" {
		t.Fatalf("cancelled clock rendered a future occurrence: %#v", rows[1])
	}
	if rows[2].Value != "worker/retry: failed; clock publication failed" {
		t.Fatalf("failed clock lost failure evidence or rendered a future occurrence: %#v", rows[2])
	}
}

func TestDiagnosticClockRowsRenderParkedAndSkippedInterval(t *testing.T) {
	from := time.Date(2026, 10, 6, 10, 0, 0, 0, time.UTC)
	through := from.Add(time.Hour)
	rows := diagnosticClockRows([]genericschedule.ClockReadback{
		{FlowID: ".", Name: "poll", Status: genericschedule.StatusParked, Suspension: &genericschedule.ClockSuspension{ParkedAt: from}},
		{FlowID: ".", Name: "poll", Status: genericschedule.StatusActive, Suspension: &genericschedule.ClockSuspension{SuspendedFrom: from, ResumedAt: through, SkippedOccurrences: 12}},
	})
	if rows[0].Value != "./poll: parked; suspended since 2026-10-06T10:00:00Z" ||
		rows[1].Value != "./poll: active; suspended 2026-10-06T10:00:00Z to 2026-10-06T11:00:00Z, 12 occurrences skipped" {
		t.Fatalf("clock suspension lost durable evidence=%+v", rows)
	}
}

func TestRunStatusReadsClockEvidenceWithoutArming(t *testing.T) {
	runID := "11111111-1111-4111-8111-111111111111"
	due := time.Date(2026, 10, 2, 14, 32, 0, 0, time.UTC)
	clock := genericschedule.ClockReadback{
		ActivationID: "22222222-2222-4222-8222-222222222222", RunID: runID, FlowID: ".", FlowInstance: runID,
		Name: "poll", Emit: "poll.tick", Every: "1m0s", Status: genericschedule.StatusActive,
		InitialDueAt: due, NextDueAt: &due, RetainsRun: true,
	}
	for _, format := range []string{"text", "json"} {
		t.Run(format, func(t *testing.T) {
			setCLIAPITestToken(t, "test-token")
			server, requests := newDiagnosticSuccessServer(t, func(req jsonRPCRequest, _ int) map[string]any {
				if req.Method != "run.get" {
					t.Fatalf("clock inspection called %q instead of run.get", req.Method)
				}
				header := validDiagnosticRunHeader(runID)
				header["clock_schedules"] = []genericschedule.ClockReadback{clock}
				return map[string]any{"run": header}
			})
			defer server.Close()
			args := []string{"run", "status", runID, "--no-diagnose"}
			if format == "json" {
				args = append(args, "--json")
			}
			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), args, &stdout, &stderr, testRootCommandOptions(server))
			if code != 0 || len(*requests) != 1 {
				t.Fatalf("clock inspection failed or issued extra work: code=%d requests=%#v stderr=%s", code, requests, stderr.String())
			}
			if format == "json" {
				var result diagnosticRunGetResult
				if err := json.Unmarshal(stdout.Bytes(), &result); err != nil || !reflect.DeepEqual(result.Run.ClockSchedules, []genericschedule.ClockReadback{clock}) {
					t.Fatalf("CLI lost admitted clock evidence: %#v err=%v", result, err)
				}
			} else if !strings.Contains(stdout.String(), "./poll: active; next due 2026-10-02T14:32:00Z; retaining this run") {
				t.Fatalf("CLI lost durable clock rendering: %s", stdout.String())
			}
		})
	}
}
