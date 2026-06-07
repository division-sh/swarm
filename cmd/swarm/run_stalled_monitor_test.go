package main

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/runstalled"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store"
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

func TestServeRunStalledReaderLoadsSnapshotProgressFromStore(t *testing.T) {
	ctx := context.Background()
	progressAt := time.Date(2026, 6, 4, 12, 0, 0, 0, time.UTC)
	reader := &serveRunStalledReader{store: fakeRunStalledReadStore{
		report: store.RunDebugReport{
			RunID:          "run-1",
			RunTableStatus: "running",
			LastEventAt:    progressAt.Add(10 * time.Minute),
		},
		flowInstance: "review/inst-1",
		progressAt:   progressAt,
	}}
	got, err := reader.LoadRunSnapshot(ctx, "run-1")
	if err != nil {
		t.Fatalf("load run snapshot: %v", err)
	}
	if !got.LastProgressAt.Equal(progressAt) {
		t.Fatalf("progress timestamp = %s, want %s", got.LastProgressAt, progressAt)
	}
	if got.FlowInstance != "review/inst-1" {
		t.Fatalf("flow instance = %q, want review/inst-1", got.FlowInstance)
	}
}

func TestSelectedStoreFacadeRunStalledReaderPrefersPostgres(t *testing.T) {
	postgresStore := &store.PostgresStore{}
	stores := storeBundle{Postgres: postgresStore}

	if got := stores.facade().runStalledReader(); got != postgresStore {
		t.Fatalf("run stalled reader = %T, want selected postgres store", got)
	}
}

type fakeRunStalledReadStore struct {
	report       store.RunDebugReport
	flowInstance string
	progressAt   time.Time
}

func (s fakeRunStalledReadStore) ListRunHeaders(context.Context, store.RunHeaderListOptions) ([]store.RunHeader, string, error) {
	return nil, "", nil
}

func (s fakeRunStalledReadStore) LoadRunDebugReport(context.Context, string, store.RunDebugQueryOptions) (store.RunDebugReport, error) {
	return s.report, nil
}

func (s fakeRunStalledReadStore) ListOperatorEvents(context.Context, store.OperatorEventListOptions) (store.OperatorEventListResult, error) {
	return store.OperatorEventListResult{}, nil
}

func (s fakeRunStalledReadStore) LoadLatestRunFlowInstance(context.Context, string) (string, error) {
	return s.flowInstance, nil
}

func (s fakeRunStalledReadStore) LoadLatestRunNonEscalationProgressAt(context.Context, string, string) (time.Time, error) {
	return s.progressAt, nil
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
