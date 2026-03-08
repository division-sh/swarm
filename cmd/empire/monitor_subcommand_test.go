package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	llm "empireai/internal/runtime/llm"
)

func TestSyncTMuxMonitorSession_CreatesAndPrunesAgentWindows(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "tmux.log")
	tmuxStub := filepath.Join(dir, "tmux_stub.sh")
	script := `#!/bin/sh
LOG="` + logPath + `"
case "$1" in
  has-session)
    exit 0
    ;;
  list-windows)
    printf '%s\n' overview stale
    exit 0
    ;;
esac
printf '%s\n' "$*" >> "$LOG"
exit 0
`
	if err := os.WriteFile(tmuxStub, []byte(script), 0o755); err != nil {
		t.Fatalf("write tmux stub: %v", err)
	}

	rootDir := filepath.Join(dir, "monitor")
	if err := syncTMuxMonitorSession(context.Background(), tmuxStub, "empire-monitor", rootDir, []string{"agent-one"}, false); err != nil {
		t.Fatalf("syncTMuxMonitorSession: %v", err)
	}

	raw, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read tmux log: %v", err)
	}
	text := string(raw)
	if !strings.Contains(text, "new-window -d -t empire-monitor -n agent-one") {
		t.Fatalf("expected new agent window command, got:\n%s", text)
	}
	if !strings.Contains(text, "kill-window -t empire-monitor:stale") {
		t.Fatalf("expected stale window prune, got:\n%s", text)
	}
	if !strings.Contains(text, llm.MonitorLogPath(rootDir, "agent-one")) {
		t.Fatalf("expected monitor log path in tmux command, got:\n%s", text)
	}
}
