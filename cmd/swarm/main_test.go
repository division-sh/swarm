package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	runtimecontracts "swarm/internal/runtime/contracts"
	"swarm/internal/runtime/semanticview"
)

func TestPrintRunStatusReport(t *testing.T) {
	var buf bytes.Buffer
	started := time.Unix(1700000000, 0).UTC()
	last := started.Add(5 * time.Minute)
	report := runStatusReport{
		RunID:             "run-123",
		RunTableStatus:    "running",
		RootEventID:       "evt-1",
		RootEventType:     "scan.requested",
		StartedAt:         started,
		LastEventAt:       last,
		EventCount:        7,
		WarnErrorLogCount: 1,
		Heuristics: []string{
			"run appears settled after scoring started but no terminal scoring outcome was emitted",
		},
		EventCounts: []runStatusEventCount{
			{EventName: "scan.requested", Count: 1},
			{EventName: "vertical.discovered", Count: 2},
		},
		Deliveries: []runStatusDeliveryCount{
			{SubscriberID: "analysis-agent", Status: "delivered", Count: 2},
		},
		AgentTurns: []runStatusAgentTurn{
			{AgentID: "analysis-agent", Turns: 2, ErrorCount: 0, LastAt: last},
		},
		RuntimeLogSummary: []runStatusRuntimeSummary{
			{Level: "warn", Component: "mcp-gateway", Action: "mcp.context.fallback_used", Count: 3},
		},
		RuntimeLogs: []runStatusRuntimeLog{
			{Level: "warn", Component: "mcp-gateway", Action: "mcp.context.fallback_used", Error: "missing or invalid mcp context token", CreatedAt: last},
		},
		RecentEvents: []runStatusEvent{
			{EventName: "vertical.discovered", EntityID: "ent-1", CreatedAt: last},
		},
	}

	printRunStatusReport(&buf, report)
	out := buf.String()
	for _, want := range []string{
		"Run run-123",
		"Root: scan.requested (evt-1)",
		"Run Status: running",
		"Summary: events=7 deliveries=1 dead_letters=0 agent_turns=1 runtime_warn_errors=1",
		"Heuristics:",
		"run appears settled after scoring started but no terminal scoring outcome was emitted",
		"Event Counts:",
		"analysis-agent  status=delivered  count=2",
		"Runtime Log Summary:",
		"WARN  mcp-gateway/mcp.context.fallback_used  count=3",
		"Runtime Warnings/Errors:",
		"WARN  mcp-gateway/mcp.context.fallback_used",
		"Recent Events:",
		"vertical.discovered  entity=ent-1",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("status output missing %q:\n%s", want, out)
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
