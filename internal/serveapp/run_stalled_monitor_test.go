package serveapp

import (
	"context"
	"strings"
	"testing"
	"time"

	operatorread "github.com/division-sh/swarm/internal/operatorread"

	"github.com/division-sh/swarm/internal/runtime/runstalled"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store"
)

func TestRunStalledSnapshotFromDebugReportUsesProjectRunOperationalStatus(t *testing.T) {
	now := time.Date(2026, 6, 4, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name       string
		report     operatorread.RunDebugReport
		wantLayer  string
		wantReason string
	}{
		{
			name: "delivery lifecycle",
			report: operatorread.RunDebugReport{
				RunID:          "run-delivery",
				RunTableStatus: "running",
				LastEventAt:    now,
			},
			wantLayer:  "delivery_lifecycle",
			wantReason: "no_active_deliveries",
		},
		{
			name: "scoring names do not create obligations",
			report: operatorread.RunDebugReport{
				RunID:          "run-scoring",
				RunTableStatus: "running",
				LastEventAt:    now,
				EventCounts: []operatorread.RunDebugEventCount{
					{EventName: "scoring/scoring.requested", Count: 1},
				},
			},
			wantLayer:  "delivery_lifecycle",
			wantReason: "no_active_deliveries",
		},
		{
			name: "active delivery remains running",
			report: operatorread.RunDebugReport{
				RunID:          "run-active",
				RunTableStatus: "running",
				LastEventAt:    now,
				Deliveries: []operatorread.RunDebugDeliveryCount{
					{Status: "in_progress", Count: 1},
				},
			},
			wantLayer:  "",
			wantReason: "",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.report.OperationalFacts = stalledMonitorFacts(now, "review/inst-1")
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
	report := operatorread.RunDebugReport{
		RunID:            "run-self-alert",
		RunTableStatus:   "running",
		LastEventAt:      selfAlertAt,
		OperationalFacts: stalledMonitorFacts(progressAt, "review/inst-1"),
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
		report: operatorread.RunDebugReport{
			RunID:            "run-1",
			RunTableStatus:   "running",
			LastEventAt:      progressAt.Add(10 * time.Minute),
			OperationalFacts: stalledMonitorFacts(progressAt, "review/inst-1"),
		},
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

func TestServeRunStalledReaderUsesSelectedProjection(t *testing.T) {
	postgresStore := &store.PostgresStore{}
	reader, ok := newServeRunStalledReader(postgresStore)
	if !ok || reader.store != postgresStore {
		t.Fatalf("run stalled reader = %#v, want selected run-stalled projection", reader)
	}
}

func TestServeRunStalledReaderKeepsMixedScopesAndDoesNotInventAlertArbitration(t *testing.T) {
	facts := stalledMonitorFacts(time.Unix(100, 0).UTC(), "review/one")
	facts.Instances = append(facts.Instances, operatorread.RunInstanceFacts{FlowInstance: "review/two", Complete: true, Revision: 1})
	reader := &serveRunStalledReader{store: fakeRunStalledReadStore{report: operatorread.RunDebugReport{
		RunID: "run", RunTableStatus: "running", OperationalFacts: facts,
	}}}
	if _, err := reader.LoadRunSnapshot(context.Background(), "run"); err == nil || !strings.Contains(err.Error(), "arbitration") {
		t.Fatalf("simultaneous stalls selected an arbitrary alert scope: %v", err)
	}
	facts.Instances[1].Work.Timers = 1
	snapshot, err := reader.LoadRunSnapshot(context.Background(), "run")
	if err != nil || snapshot.Diagnosis.OperationalState != "running" || len(snapshot.Diagnosis.Attributions) != 1 || snapshot.Diagnosis.Attributions[0].FlowInstance != "review/one" {
		t.Fatalf("waiting sibling hid the stalled sibling or invented escalation: %+v %v", snapshot, err)
	}
}

type fakeRunStalledReadStore struct {
	report operatorread.RunDebugReport
}

func (s fakeRunStalledReadStore) ListRunHeaders(context.Context, operatorread.RunHeaderListOptions) ([]operatorread.RunHeader, string, error) {
	return nil, "", nil
}

func (s fakeRunStalledReadStore) LoadRunDebugReport(context.Context, string, operatorread.RunDebugQueryOptions) (operatorread.RunDebugReport, error) {
	return s.report, nil
}

func (s fakeRunStalledReadStore) ListOperatorEvents(context.Context, operatorread.OperatorEventListOptions) (operatorread.OperatorEventListResult, error) {
	return operatorread.OperatorEventListResult{}, nil
}

func stalledMonitorFacts(progress time.Time, path string) *operatorread.RunOperationalFacts {
	return &operatorread.RunOperationalFacts{Complete: true, LastProgressAt: progress,
		Instances: []operatorread.RunInstanceFacts{{FlowInstance: path, Complete: true, Revision: 1}},
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
