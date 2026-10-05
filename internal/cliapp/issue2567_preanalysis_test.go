package cliapp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func issue2567Source(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	for name, body := range map[string]string{
		"schema.yaml":   "name: first-verify\nstages: {waiting: {initial: true}, done: {terminal: true}}\npins: {inputs: [work.requested]}\n",
		"events.yaml":   "work.requested:\n",
		"entities.yaml": "work: {}\n",
		"nodes.yaml":    "worker:\n  execution_type: system_node\n  event_handlers:\n    work.requested: {advances_to: done}\n",
	} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func TestIssue2567PreanalysisPublicFlagIsIgnored(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	root := issue2567Source(t)
	configDir := filepath.Join(t.TempDir(), "configured")
	flagDir := filepath.Join(t.TempDir(), "flag")
	config := filepath.Join(t.TempDir(), "swarm.yaml")
	writeRuntimeConfigText(t, config, fmt.Sprintf("paths: {swarm_dir: %q}\nserve: {api_listen_addr: '127.0.0.1:0', mcp_listen_addr: '127.0.0.1:0'}\n", configDir))
	var out, diagnostic bytes.Buffer
	code := executeRootCommand(context.Background(), RepoRoot(), []string{"--swarm-dir", flagDir, "verify", root, "--config", config, "--json"}, &out, &diagnostic)
	var result verifyCommandResult
	if err := json.Unmarshal(out.Bytes(), &result); err != nil {
		t.Fatalf("exit=%d decode=%v stdout=%s stderr=%s", code, err, &out, &diagnostic)
	}
	for _, observation := range result.Observations {
		if observation.CheckID == "selected_store_access" {
			if !strings.Contains(observation.Reason, configDir) || strings.Contains(observation.Reason, flagDir) {
				t.Fatalf("baseline no longer reproduces wrong flag selection: %+v", observation)
			}
			t.Logf("baseline exit=%d selected_store_access=%s, ignored flag=%s", code, observation.Reason, flagDir)
			return
		}
	}
	t.Fatalf("missing store observation: exit=%d result=%+v stderr=%s", code, result, &diagnostic)
}

func TestIssue2567PreanalysisFreshHomeIsBlocked(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	root := issue2567Source(t)
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, ".config"))
	config := filepath.Join(root, "swarm.yaml")
	writeRuntimeConfigText(t, config, "serve: {api_listen_addr: '127.0.0.1:0', mcp_listen_addr: '127.0.0.1:0'}\n")
	var out, diagnostic bytes.Buffer
	code := executeRootCommand(context.Background(), RepoRoot(), []string{"verify", root, "--config", config, "--json"}, &out, &diagnostic)
	var result verifyCommandResult
	if err := json.Unmarshal(out.Bytes(), &result); err != nil {
		t.Fatalf("exit=%d decode=%v stdout=%s stderr=%s", code, err, &out, &diagnostic)
	}
	if code == 0 || result.OK {
		t.Fatalf("baseline no longer reproduces missing-store blocker: exit=%d result=%+v", code, result)
	}
	found := false
	for _, finding := range result.Errors {
		if finding.CheckID == "selected_store_access" && strings.Contains(finding.Message, home) {
			found = true
			t.Logf("baseline exit=%d missing selected store blocks first verification: %s", code, finding.Message)
		} else {
			t.Errorf("unexpected independent failure obscures missing-store proof: %+v", finding)
		}
	}
	if !found {
		t.Fatal("missing store refusal was not recorded")
	}
	if _, err := os.Stat(filepath.Join(home, ".swarm")); !os.IsNotExist(err) {
		t.Fatalf("verify changed fresh-home runtime state: %v", err)
	}
}
