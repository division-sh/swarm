package main

import (
	"bytes"
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	runtimecontracts "swarm/internal/runtime/contracts"
	"swarm/internal/runtime/semanticview"
	"swarm/internal/store"
)

func TestPrintTraceReport(t *testing.T) {
	var buf bytes.Buffer
	report := store.TraceReport{
		TraceID: "trace-123",
		Events: []store.TraceEvent{
			{
				EventID:       "evt-1",
				EventName:     "scan.requested",
				SourceEventID: "",
				ProducedBy:    "campaign-coordinator",
				CreatedAt:     time.Unix(1700000000, 0).UTC(),
			},
		},
		Deliveries: []store.TraceDelivery{
			{EventID: "evt-1", SubscriberType: "agent", SubscriberID: "worker-1", Status: "pending", ReasonCode: "matched_agent_subscription"},
		},
		Receipts: []store.TraceReceipt{
			{EventID: "evt-1", SubscriberType: "platform", SubscriberID: "pipeline", Outcome: "success", ReasonCode: "pipeline_persisted"},
		},
	}

	printTraceReport(&buf, report)
	out := buf.String()
	for _, want := range []string{
		"Trace trace-123",
		"Summary: pending delivery agent/worker-1 reason=matched_agent_subscription for scan.requested",
		"scan.requested",
		"delivery  agent/worker-1  status=pending reason=matched_agent_subscription",
		"receipt   platform/pipeline  outcome=success reason=pipeline_persisted",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("trace output missing %q:\n%s", want, out)
		}
	}
}

func TestPrintTraceReport_InProgressDeliverySummary(t *testing.T) {
	var buf bytes.Buffer
	startedAt := time.Unix(1700000100, 0).UTC()
	report := store.TraceReport{
		TraceID: "trace-456",
		Events: []store.TraceEvent{
			{
				EventID:   "evt-2",
				EventName: "discovery/market_research.scan_assigned",
				CreatedAt: time.Unix(1700000000, 0).UTC(),
			},
		},
		Deliveries: []store.TraceDelivery{
			{
				EventID:         "evt-2",
				SubscriberType:  "agent",
				SubscriberID:    "market-research-agent",
				Status:          "in_progress",
				ReasonCode:      "agent_processing",
				ActiveSessionID: "sess-123",
				StartedAt:       sql.NullTime{Time: startedAt, Valid: true},
			},
		},
	}

	printTraceReport(&buf, report)
	out := buf.String()
	for _, want := range []string{
		"Summary: in-progress delivery agent/market-research-agent session=sess-123 for discovery/market_research.scan_assigned",
		"delivery  agent/market-research-agent  status=in_progress reason=agent_processing session=sess-123 started=2023-11-14T22:15:00Z",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("trace output missing %q:\n%s", want, out)
		}
	}
}

func TestLoadDotEnvFileSetsMissingVarsOnly(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ".env")
	if err := os.WriteFile(path, []byte("ALPHA=one\nBETA=\"two words\"\nexport GAMMA='three'\n"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	t.Setenv("ALPHA", "shell")

	if err := loadDotEnvFile(path); err != nil {
		t.Fatalf("loadDotEnvFile: %v", err)
	}

	if got := os.Getenv("ALPHA"); got != "shell" {
		t.Fatalf("ALPHA = %q, want shell override", got)
	}
	if got := os.Getenv("BETA"); got != "two words" {
		t.Fatalf("BETA = %q", got)
	}
	if got := os.Getenv("GAMMA"); got != "three" {
		t.Fatalf("GAMMA = %q", got)
	}
}

func TestLoadDotEnvFileMissingIsNoop(t *testing.T) {
	if err := loadDotEnvFile(filepath.Join(t.TempDir(), ".env")); err != nil {
		t.Fatalf("loadDotEnvFile: %v", err)
	}
}

func TestLoadDotEnvFileRejectsMalformedLine(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ".env")
	if err := os.WriteFile(path, []byte("BROKEN\n"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	err := loadDotEnvFile(path)
	if err == nil || !strings.Contains(err.Error(), "expected KEY=VALUE") {
		t.Fatalf("loadDotEnvFile error = %v", err)
	}
}

func TestRunVerifyCommand_BadContractsPath(t *testing.T) {
	var buf bytes.Buffer
	code := runVerifyCommand(context.Background(), repoRoot(), []string{
		"-contracts", filepath.Join(t.TempDir(), "missing"),
	}, &buf)
	if code == 0 {
		t.Fatal("expected non-zero exit code")
	}
	if out := buf.String(); !strings.Contains(out, "verify failed: resolve contracts") {
		t.Fatalf("output = %q", out)
	}
}

func TestVerifyEmitSchemaCoverage_RejectsStrictMissingEmitSchemas(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"agent-1": {
				ID:         "agent-1",
				EmitEvents: []string{"missing.event"},
			},
		},
	})
	t.Setenv("SWARM_EMIT_SCHEMA_STRICT", "true")
	err := verifyEmitSchemaCoverage(source)
	if err == nil || !strings.Contains(err.Error(), "emit schema strict mode enabled") {
		t.Fatalf("verifyEmitSchemaCoverage error = %v, want strict emit schema error", err)
	}
}

func TestVerifyEmitSchemaCoverage_AllowsMissingWhenStrictDisabled(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"agent-1": {
				ID:         "agent-1",
				EmitEvents: []string{"missing.event"},
			},
		},
	})
	t.Setenv("SWARM_EMIT_SCHEMA_STRICT", "false")
	if err := verifyEmitSchemaCoverage(source); err != nil {
		t.Fatalf("verifyEmitSchemaCoverage: %v", err)
	}
}
