package cliapp

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/authoringview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/templateflowpilot"
)

func TestW5AgentsVerifyAndDescribeSupportedSurface(t *testing.T) {
	root := templateflowpilot.Write(t, templateflowpilot.Options{})
	writeDescribeTestFile(t, filepath.Join(root, "agents.yaml"), "worker:\n  model: regular\n  intent: {inline: root business intent}\n  subscriptions: [account.requested]\n")
	writeDescribeTestFile(t, filepath.Join(root, "account", "agents.yaml"), "worker:\n  model: regular\n  memory: true\n  intent: {inline: account business intent}\n  subscriptions: [account.ready]\n")
	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), RepoRoot(), []string{"verify", root, "--config", writeTestVerifyRuntimeConfig(t), "--json"}, &stdout, &stderr, defaultRootCommandOptions())
	if code != 0 {
		t.Fatalf("verify=%d stdout=%s stderr=%s", code, &stdout, &stderr)
	}
	stdout.Reset()
	stderr.Reset()
	code = executeRootCommandWithOptions(context.Background(), RepoRoot(), []string{"describe", root, "--json"}, &stdout, &stderr, defaultRootCommandOptions())
	if code != 0 {
		t.Fatalf("describe=%d stdout=%s stderr=%s", code, &stdout, &stderr)
	}
	var view authoringview.View
	if err := json.Unmarshal(stdout.Bytes(), &view); err != nil {
		t.Fatal(err)
	}
	if len(view.Root.Agents) != 1 || len(view.Flows) != 2 || view.Flows[0].ID != "account" || len(view.Flows[0].Agents) != 1 {
		t.Fatalf("scoped agents missing: %+v", view)
	}
	for _, scope := range []struct {
		agent  authoringview.AgentView
		memory bool
	}{{view.Root.Agents[0], false}, {view.Flows[0].Agents[0], true}} {
		field := scope.agent.Fields["memory"]
		if scope.agent.ID != "worker" || field.Value != scope.memory || field.Source != "" {
			t.Fatalf("effective identity or memory changed: %+v", scope.agent)
		}
		if _, present := scope.agent.Fields["memory_source"]; present {
			t.Fatal("retired source surfaced")
		}
	}
	if strings.Contains(stdout.String(), "memory_source") {
		t.Fatal("retired source surfaced outside agent fields")
	}
	for _, fields := range []string{"memory: false", "entity_writes: {Item: {save: null}}", "native_tools: {unknown: null}", "tools_tier2: []", "max_turns_per_task: 1.5"} {
		t.Run(fields, func(t *testing.T) {
			writeDescribeTestFile(t, filepath.Join(root, "agents.yaml"), "worker:\n  model: regular\n  intent: {inline: business intent}\n  <<: {"+fields+"}\n")
			stdout.Reset()
			stderr.Reset()
			code := executeRootCommandWithOptions(context.Background(), RepoRoot(), []string{"verify", root, "--config", writeTestVerifyRuntimeConfig(t), "--json"}, &stdout, &stderr, defaultRootCommandOptions())
			if code == 0 || !strings.Contains(stdout.String()+stderr.String(), "agents.yaml") || !strings.Contains(stdout.String()+stderr.String(), strings.Split(fields, ":")[0]) {
				t.Fatalf("invalid authored source accepted or lost evidence: code=%d stdout=%s stderr=%s", code, &stdout, &stderr)
			}
		})
	}
}
