package main

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/runstalled"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store"

	_ "modernc.org/sqlite"
)

func TestRunStalledSnapshotFromDebugReportUsesProjectRunOperationalStatus(t *testing.T) {
	now := time.Date(2026, 6, 4, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name       string
		report     store.RunDebugReport
		wantLayer  string
		wantReason string
	}{
		{
			name: "delivery lifecycle",
			report: store.RunDebugReport{
				RunID:          "run-delivery",
				RunTableStatus: "running",
				LastEventAt:    now,
			},
			wantLayer:  "delivery_lifecycle",
			wantReason: "no_active_deliveries",
		},
		{
			name: "scoring terminal outcome",
			report: store.RunDebugReport{
				RunID:          "run-scoring",
				RunTableStatus: "running",
				LastEventAt:    now,
				EventCounts: []store.RunDebugEventCount{
					{EventName: "scoring/scoring.requested", Count: 1},
				},
			},
			wantLayer:  "scoring_terminal_outcome",
			wantReason: "terminal_scoring_outcome_missing",
		},
		{
			name: "active delivery remains running",
			report: store.RunDebugReport{
				RunID:          "run-active",
				RunTableStatus: "running",
				LastEventAt:    now,
				Deliveries: []store.RunDebugDeliveryCount{
					{Status: "in_progress", Count: 1},
				},
			},
			wantLayer:  "",
			wantReason: "",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := runStalledSnapshotFromDebugReport(tc.report, "review/inst-1", tc.report.LastEventAt)
			if got.Diagnosis.BlockingLayer != tc.wantLayer {
				t.Fatalf("blocking_layer = %q, want %q", got.Diagnosis.BlockingLayer, tc.wantLayer)
			}
			if got.Diagnosis.BlockingReason != tc.wantReason {
				t.Fatalf("blocking_reason = %q, want %q", got.Diagnosis.BlockingReason, tc.wantReason)
			}
			if tc.wantLayer == "" && got.Diagnosis.OperationalState != "running" {
				t.Fatalf("operational_state = %q, want running", got.Diagnosis.OperationalState)
			}
			if tc.wantLayer != "" && got.Diagnosis.OperationalState != "stalled" {
				t.Fatalf("operational_state = %q, want stalled", got.Diagnosis.OperationalState)
			}
		})
	}
}

func TestRunStalledSnapshotFromDebugReportUsesNonEscalationProgressTimestamp(t *testing.T) {
	progressAt := time.Date(2026, 6, 4, 12, 0, 0, 0, time.UTC)
	selfAlertAt := progressAt.Add(10 * time.Minute)
	report := store.RunDebugReport{
		RunID:          "run-self-alert",
		RunTableStatus: "running",
		LastEventAt:    selfAlertAt,
	}
	got := runStalledSnapshotFromDebugReport(report, "", progressAt)
	if !got.LastProgressAt.Equal(progressAt) {
		t.Fatalf("last progress at = %s, want %s", got.LastProgressAt, progressAt)
	}
	if got.Diagnosis.OperationalState != "stalled" {
		t.Fatalf("operational_state = %q, want stalled", got.Diagnosis.OperationalState)
	}
}

func TestServeRunStalledReaderLoadsLatestNonEscalationProgressAt(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE events (
			run_id TEXT NOT NULL,
			event_name TEXT NOT NULL,
			created_at TEXT NOT NULL
		)
	`); err != nil {
		t.Fatalf("create events: %v", err)
	}
	progressAt := time.Date(2026, 6, 4, 12, 0, 0, 0, time.UTC)
	selfAlertAt := progressAt.Add(10 * time.Minute)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (run_id, event_name, created_at) VALUES
			('run-1', 'task.started', ?),
			('run-1', 'platform.run_stalled', ?)
	`, progressAt.Format(time.RFC3339Nano), selfAlertAt.Format(time.RFC3339Nano)); err != nil {
		t.Fatalf("seed events: %v", err)
	}
	reader := &serveRunStalledReader{db: db}
	got, err := reader.loadLatestNonEscalationProgressAt(ctx, "run-1")
	if err != nil {
		t.Fatalf("load latest non-escalation progress: %v", err)
	}
	if !got.Equal(progressAt) {
		t.Fatalf("progress timestamp = %s, want %s", got, progressAt)
	}
}

func TestRunStalledFlowIDForInstanceChoosesMostSpecificScope(t *testing.T) {
	scopes := []semanticview.FlowScope{
		{ID: "parent", Path: "review"},
		{ID: "child", Path: "review/qa"},
		{ID: "id-only"},
	}
	if got := runStalledFlowIDForInstance(scopes, "review/qa/inst-1"); got != "child" {
		t.Fatalf("flow id = %q, want child", got)
	}
	if got := runStalledFlowIDForInstance(scopes, "review/inst-1"); got != "parent" {
		t.Fatalf("flow id = %q, want parent", got)
	}
	if got := runStalledFlowIDForInstance(scopes, "id-only/inst-1"); got != "id-only" {
		t.Fatalf("flow id = %q, want id-only", got)
	}
}

func TestRunStalledPolicyParsing(t *testing.T) {
	if got, ok := runStalledPolicyBool("false"); !ok || got {
		t.Fatalf("bool parse = %v/%v, want false/true", got, ok)
	}
	if got, ok := runStalledPolicySeconds(float64(runstalled.DefaultThresholdSeconds)); !ok || got != runstalled.DefaultThresholdSeconds {
		t.Fatalf("seconds parse = %d/%v, want default/true", got, ok)
	}
	if _, ok := runStalledPolicySeconds(3.5); ok {
		t.Fatalf("fractional threshold parsed successfully, want rejection")
	}
}
