package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os/exec"
	"sort"
	"strings"

	"empireai/internal/config"
	llm "empireai/internal/runtime/llm"
)

var monitorExecCommandContext = exec.CommandContext

func runMonitorSubcommand(args []string) error {
	if len(args) == 0 || strings.TrimSpace(args[0]) == "tmux" {
		if len(args) == 0 {
			return runMonitorTMuxSubcommand(nil)
		}
		return runMonitorTMuxSubcommand(args[1:])
	}
	return fmt.Errorf("unknown monitor subcommand: %s", strings.TrimSpace(args[0]))
}

func runMonitorTMuxSubcommand(args []string) error {
	fs := flag.NewFlagSet("monitor tmux", flag.ContinueOnError)
	cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
	storeMode := fs.String("store", "postgres", "Storage mode")
	migrate := fs.Bool("migrate", false, "Apply migrations")
	migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
	sessionName := fs.String("session", "runtime-monitor", "tmux session name")
	tmuxBin := fs.String("tmux-bin", "tmux", "tmux binary")
	rootDir := fs.String("root", llm.DefaultMonitorDir(), "Monitor log directory")
	attach := fs.Bool("attach", true, "Attach to the tmux session after syncing windows")
	if err := fs.Parse(args); err != nil {
		return err
	}
	ctx := context.Background()
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
	if stores.SQLDB == nil {
		return fmt.Errorf("monitor requires persistent store mode (use -store postgres)")
	}
	agentIDs, err := discoverMonitorAgentIDs(ctx, stores.SQLDB)
	if err != nil {
		return err
	}
	return syncTMuxMonitorSession(ctx, *tmuxBin, *sessionName, *rootDir, agentIDs, *attach)
}

func discoverMonitorAgentIDs(ctx context.Context, db *sql.DB) ([]string, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT DISTINCT ON (agent_id) agent_id
		FROM agent_sessions
		WHERE status = 'active'
		ORDER BY agent_id, last_used_at DESC NULLS LAST, created_at DESC
	`)
	if err != nil {
		return nil, fmt.Errorf("list active monitor agents: %w", err)
	}
	defer rows.Close()

	ids := make([]string, 0, 32)
	for rows.Next() {
		var agentID string
		if err := rows.Scan(&agentID); err != nil {
			return nil, fmt.Errorf("scan active monitor agent: %w", err)
		}
		agentID = strings.TrimSpace(agentID)
		if agentID == "" {
			continue
		}
		ids = append(ids, agentID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate active monitor agents: %w", err)
	}
	sort.Strings(ids)
	return ids, nil
}

func syncTMuxMonitorSession(ctx context.Context, tmuxBin, sessionName, rootDir string, agentIDs []string, attach bool) error {
	tmuxBin = strings.TrimSpace(tmuxBin)
	sessionName = strings.TrimSpace(sessionName)
	if tmuxBin == "" {
		tmuxBin = "tmux"
	}
	if sessionName == "" {
		sessionName = "runtime-monitor"
	}

	exists, err := tmuxSessionExists(ctx, tmuxBin, sessionName)
	if err != nil {
		return err
	}
	if !exists {
		if _, err := runTMux(ctx, tmuxBin, "new-session", "-d", "-s", sessionName, "-n", "overview", idleWindowCommand("empire monitor")); err != nil {
			return fmt.Errorf("create tmux session %s: %w", sessionName, err)
		}
	}

	existing, err := tmuxWindowNames(ctx, tmuxBin, sessionName)
	if err != nil {
		return err
	}
	desired := make(map[string]string, len(agentIDs))
	for _, agentID := range agentIDs {
		windowName := tmuxWindowName(agentID)
		desired[windowName] = monitorTailCommand(rootDir, agentID)
	}

	for windowName, cmd := range desired {
		target := sessionName + ":" + windowName
		if _, ok := existing[windowName]; ok {
			if _, err := runTMux(ctx, tmuxBin, "respawn-window", "-k", "-t", target, cmd); err != nil {
				return fmt.Errorf("respawn tmux window %s: %w", target, err)
			}
			continue
		}
		if _, err := runTMux(ctx, tmuxBin, "new-window", "-d", "-t", sessionName, "-n", windowName, cmd); err != nil {
			return fmt.Errorf("create tmux window %s: %w", target, err)
		}
	}

	for windowName := range existing {
		if windowName == "overview" {
			continue
		}
		if _, ok := desired[windowName]; ok {
			continue
		}
		if _, err := runTMux(ctx, tmuxBin, "kill-window", "-t", sessionName+":"+windowName); err != nil {
			return fmt.Errorf("kill stale tmux window %s:%s: %w", sessionName, windowName, err)
		}
	}

	if attach {
		_, err := runTMux(ctx, tmuxBin, "attach-session", "-t", sessionName)
		return err
	}
	return nil
}

func tmuxSessionExists(ctx context.Context, tmuxBin, sessionName string) (bool, error) {
	_, err := runTMux(ctx, tmuxBin, "has-session", "-t", sessionName)
	if err == nil {
		return true, nil
	}
	msg := strings.ToLower(strings.TrimSpace(err.Error()))
	if strings.Contains(msg, "can't find session") || strings.Contains(msg, "failed to connect to server") {
		return false, nil
	}
	return false, err
}

func tmuxWindowNames(ctx context.Context, tmuxBin, sessionName string) (map[string]struct{}, error) {
	out, err := runTMux(ctx, tmuxBin, "list-windows", "-t", sessionName, "-F", "#{window_name}")
	if err != nil {
		return nil, fmt.Errorf("list tmux windows for %s: %w", sessionName, err)
	}
	set := make(map[string]struct{})
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		set[line] = struct{}{}
	}
	return set, nil
}

func monitorTailCommand(rootDir, agentID string) string {
	path := llm.MonitorLogPath(rootDir, agentID)
	return "sh -lc " + shellQuote("mkdir -p "+shellQuoteForShell(filepathDir(path))+" && touch "+shellQuoteForShell(path)+" && printf '%s\\n' 'tailing "+escapeSingleQuotes(agentID)+"' && tail -n 200 -F "+shellQuoteForShell(path))
}

func idleWindowCommand(label string) string {
	return "sh -lc " + shellQuote("printf '%s\\n' "+shellQuoteForShell(label)+" && tail -f /dev/null")
}

func tmuxWindowName(agentID string) string {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return "unknown"
	}
	var b strings.Builder
	for _, r := range agentID {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '-', r == '_', r == '.':
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	name := strings.Trim(b.String(), "_")
	if name == "" {
		name = "unknown"
	}
	return name
}

func runTMux(ctx context.Context, tmuxBin string, args ...string) (string, error) {
	cmd := monitorExecCommandContext(ctx, tmuxBin, args...)
	raw, err := cmd.CombinedOutput()
	out := strings.TrimSpace(string(raw))
	if err != nil {
		if out == "" {
			return "", err
		}
		return "", fmt.Errorf("%w: %s", err, out)
	}
	return out, nil
}

func shellQuote(raw string) string {
	return "'" + escapeSingleQuotes(raw) + "'"
}

func shellQuoteForShell(raw string) string {
	return "'" + escapeSingleQuotes(raw) + "'"
}

func escapeSingleQuotes(raw string) string {
	return strings.ReplaceAll(raw, "'", `'\''`)
}

func filepathDir(path string) string {
	i := strings.LastIndex(path, "/")
	if i <= 0 {
		return "."
	}
	return path[:i]
}
