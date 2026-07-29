package releasee2e

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestClaudeCLIManagedLifecycleFromReleaseBinaryDefaults(t *testing.T) {
	repo := releaseE2ERepoRoot(t)
	releaseRoot := t.TempDir()
	binaryPath := filepath.Join(releaseRoot, "swarm")
	build := exec.Command("go", "build", "-o", binaryPath, "./cmd/swarm")
	build.Dir = repo
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build release binary: %v\n%s", err, output)
	}

	writeReleaseFile(t, filepath.Join(releaseRoot, "go.mod"), "module release-e2e-project\n\ngo 1.23.0\n")
	copyReleaseTree(t,
		filepath.Join(repo, "internal", "releasee2e", "testdata", "claude_cli_managed_lifecycle"),
		filepath.Join(releaseRoot, "contracts"),
	)
	payloadSource := filepath.Join(releaseRoot, "contracts", "payload.json")
	payloadPath := filepath.Join(releaseRoot, "payload.json")
	if err := os.Rename(payloadSource, payloadPath); err != nil {
		t.Fatalf("move release payload: %v", err)
	}
	if _, err := os.Stat(filepath.Join(releaseRoot, "swarm.yaml")); !os.IsNotExist(err) {
		t.Fatalf("release project unexpectedly contains swarm.yaml: %v", err)
	}

	fakeRoot := filepath.Join(releaseRoot, "fake-docker-state")
	fakeBin := filepath.Join(releaseRoot, "fake-bin")
	if err := os.MkdirAll(fakeRoot, 0o700); err != nil {
		t.Fatalf("create fake Docker state: %v", err)
	}
	if err := os.MkdirAll(fakeBin, 0o700); err != nil {
		t.Fatalf("create fake executable directory: %v", err)
	}
	testBinary, err := os.Executable()
	if err != nil {
		t.Fatalf("resolve test binary: %v", err)
	}
	dockerScript := fmt.Sprintf("#!/bin/sh\n%s=1 exec %s -- \"$@\"\n", fakeDockerHelperEnv, shellQuote(testBinary))
	writeExecutable(t, filepath.Join(fakeBin, "docker"), dockerScript)

	home := filepath.Join(releaseRoot, "home")
	env := releaseProcessEnv(fakeBin, fakeRoot, home)
	verify := runReleaseCommand(t, 45*time.Second, releaseRoot, env, "", binaryPath, "verify")
	if verify.err != nil {
		t.Fatalf("release verify failed: %v\n%s", verify.err, verify.output)
	}
	secret := "release-e2e-oauth-value"
	secrets := runReleaseCommand(t, 45*time.Second, releaseRoot, env, secret+"\n",
		binaryPath, "secrets", "set", "CLAUDE_CODE_OAUTH_TOKEN", "--stdin")
	if secrets.err != nil {
		t.Fatalf("public credential setup failed: %v\n%s", secrets.err, secrets.output)
	}
	if strings.Contains(secrets.output, secret) {
		t.Fatalf("credential setup leaked secret value:\n%s", secrets.output)
	}

	releaseLock := acquireReleaseMCPPortLock(t)
	defer releaseLock()
	requireDefaultMCPPortAvailable(t)
	apiPort := freeReleaseTCPPort(t)
	run := runReleaseCommand(t, 90*time.Second, releaseRoot, env, "",
		binaryPath,
		"run", "start",
		"--backend", "claude_cli",
		"--api-port", fmt.Sprint(apiPort),
		"--event", "work.requested",
		"--payload", payloadPath,
	)
	if run.err != nil {
		t.Fatalf("release foreground run failed: %v\n%s\nDocker calls:\n%s", run.err, run.output, fakeDockerLogText(t, fakeRoot))
	}
	for _, want := range []string{"run started:", "run terminal:", "status=completed"} {
		if !strings.Contains(run.output, want) {
			t.Fatalf("release foreground output missing %q:\n%s", want, run.output)
		}
	}
	for _, forbidden := range []string{
		"startup probe effect authority is invalid",
		"platform.internal_failure",
		"dead letter",
		secret,
	} {
		if strings.Contains(strings.ToLower(run.output), strings.ToLower(forbidden)) {
			t.Fatalf("release foreground output contains %q:\n%s", forbidden, run.output)
		}
	}
	if _, err := os.Stat(filepath.Join(releaseRoot, ".swarm", "stores", "dev.db")); err != nil {
		t.Fatalf("default SQLite store was not created at .swarm/stores/dev.db: %v", err)
	}

	records := readFakeDockerRecords(t, fakeRoot)
	assertReleaseDockerEvidence(t, records)
	assertReleaseExternalProcessesExited(t, fakeRoot)
	assertReleasePersistentWorkspacesPreserved(t, fakeRoot)
}

type releaseCommandResult struct {
	output string
	err    error
}

func runReleaseCommand(t *testing.T, timeout time.Duration, dir string, env []string, stdin string, command string, args ...string) releaseCommandResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, command, args...)
	cmd.Dir = dir
	cmd.Env = env
	if stdin != "" {
		cmd.Stdin = strings.NewReader(stdin)
	}
	output, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		err = fmt.Errorf("%w after %s", ctx.Err(), timeout)
	}
	return releaseCommandResult{output: string(output), err: err}
}

func assertReleaseDockerEvidence(t *testing.T, records []fakeDockerRecord) {
	t.Helper()
	required := map[string]bool{
		"docker_version":   false,
		"network_inspect":  false,
		"image_inspect":    false,
		"cli_preflight":    false,
		"container_create": false,
		"container_start":  false,
		"network_connect":  false,
		"claude_startup":   false,
		"claude_live":      false,
		"mcp_emit":         false,
	}
	startupIndex, liveIndex := -1, -1
	startupSession, liveSession := "", ""
	for index, record := range records {
		if record.Class == "unexpected" {
			t.Fatalf("strict Docker emulator observed an unexpected command: %#v", record.Args)
		}
		if _, ok := required[record.Class]; ok {
			required[record.Class] = true
		}
		switch record.Class {
		case "claude_startup":
			if startupIndex == -1 {
				startupIndex, startupSession = index, record.SessionID
			}
			if allowed := dockerOptionValue(record.Args, "--allowedTools"); !strings.Contains(allowed, "mcp__runtime-tools__emit_work_completed") {
				t.Fatalf("startup accepted tools = %q, want authored emit tool", allowed)
			}
		case "claude_live":
			if liveIndex == -1 {
				liveIndex, liveSession = index, record.SessionID
			}
		case "mcp_emit":
			if record.ToolName != "emit_work_completed" {
				t.Fatalf("MCP emit tool = %q, want emit_work_completed", record.ToolName)
			}
			if !strings.HasPrefix(record.MCPURL, "http://127.0.0.1:8082/mcp") {
				t.Fatalf("MCP URL = %q, want runtime default listener", record.MCPURL)
			}
		}
	}
	var missing []string
	for class, seen := range required {
		if !seen {
			missing = append(missing, class)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Fatalf("release lifecycle is missing Docker/Claude evidence %v; records=%#v", missing, records)
	}
	if startupIndex >= liveIndex || startupIndex < 0 {
		t.Fatalf("startup/live ordering = %d/%d, want startup before post-readiness live delivery", startupIndex, liveIndex)
	}
	if startupSession == "" || liveSession == "" || startupSession == liveSession {
		t.Fatalf("startup/live attempt identities = %q/%q, want distinct non-empty identities", startupSession, liveSession)
	}
}

func assertReleaseExternalProcessesExited(t *testing.T, root string) {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(root, "active"))
	if err != nil {
		t.Fatalf("read fake Docker process activity: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("fake Docker/Claude processes survived foreground shutdown: %v", entries)
	}
}

func assertReleasePersistentWorkspacesPreserved(t *testing.T, root string) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(root, "state.json"))
	if err != nil {
		t.Fatalf("read fake Docker state: %v", err)
	}
	var state fakeDockerState
	if err := json.Unmarshal(raw, &state); err != nil {
		t.Fatalf("decode fake Docker state: %v", err)
	}
	for _, name := range []string{"swarm-scaffold", "swarm-system", "swarm-agent-release-worker"} {
		container, ok := state.Containers[name]
		if !ok || !container.Running {
			t.Fatalf("persistent workspace %q state = %#v, want preserved and running", name, container)
		}
	}
}

func readFakeDockerRecords(t *testing.T, root string) []fakeDockerRecord {
	t.Helper()
	file, err := os.Open(filepath.Join(root, "calls.jsonl"))
	if err != nil {
		t.Fatalf("open fake Docker calls: %v", err)
	}
	defer file.Close()
	var records []fakeDockerRecord
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var record fakeDockerRecord
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			t.Fatalf("decode fake Docker call: %v", err)
		}
		records = append(records, record)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan fake Docker calls: %v", err)
	}
	return records
}

func fakeDockerLogText(t *testing.T, root string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(root, "calls.jsonl"))
	if err != nil {
		return err.Error()
	}
	return string(raw)
}

func releaseProcessEnv(fakeBin, fakeRoot, home string) []string {
	blockedPrefixes := []string{
		"SWARM_",
		"DOCKER_",
		"PG",
		"XDG_",
	}
	blockedExact := map[string]bool{
		"HOME":                          true,
		"CLAUDE_CODE_OAUTH_TOKEN":       true,
		"ANTHROPIC_API_KEY":             true,
		"OPENAI_API_KEY":                true,
		"OPENAI_COMPATIBLE_API_KEY":     true,
		"SWARM_TOOL_GATEWAY_URL":        true,
		"SWARM_TOOL_GATEWAY_TOKEN":      true,
		"SWARM_TOOL_GATEWAY_AUTH_TOKEN": true,
	}
	env := make([]string, 0, len(os.Environ())+7)
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if blockedExact[key] {
			continue
		}
		blocked := false
		for _, prefix := range blockedPrefixes {
			blocked = blocked || strings.HasPrefix(key, prefix)
		}
		if !blocked && key != "PATH" {
			env = append(env, entry)
		}
	}
	return append(env,
		"HOME="+home,
		"XDG_CONFIG_HOME="+filepath.Join(home, ".config"),
		"XDG_CACHE_HOME="+filepath.Join(home, ".cache"),
		"XDG_DATA_HOME="+filepath.Join(home, ".local", "share"),
		"PATH="+fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"),
		fakeDockerRootEnv+"="+fakeRoot,
		"NO_COLOR=1",
	)
}

func releaseE2ERepoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("get package directory: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("locate repository root from package directory")
		}
		dir = parent
	}
}

func copyReleaseTree(t *testing.T, source, target string) {
	t.Helper()
	err := filepath.WalkDir(source, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		destination := filepath.Join(target, relative)
		if entry.IsDir() {
			return os.MkdirAll(destination, 0o755)
		}
		input, err := os.Open(path)
		if err != nil {
			return err
		}
		defer input.Close()
		if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
			return err
		}
		output, err := os.Create(destination)
		if err != nil {
			return err
		}
		if _, err := io.Copy(output, input); err != nil {
			output.Close()
			return err
		}
		return output.Close()
	})
	if err != nil {
		t.Fatalf("copy release fixture: %v", err)
	}
}

func writeReleaseFile(t *testing.T, path, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("create %s parent: %v", path, err)
	}
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func writeExecutable(t *testing.T, path, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatalf("write executable %s: %v", path, err)
	}
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}

func freeReleaseTCPPort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve API port: %v", err)
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}

func requireDefaultMCPPortAvailable(t *testing.T) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:8082")
	if err != nil {
		t.Fatalf("default MCP listener 127.0.0.1:8082 is unavailable: %v", err)
	}
	if err := listener.Close(); err != nil {
		t.Fatalf("release default MCP listener reservation: %v", err)
	}
}

func acquireReleaseMCPPortLock(t *testing.T) func() {
	t.Helper()
	path := filepath.Join(os.TempDir(), "swarm-release-e2e-mcp-8082.lock")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatalf("open release E2E MCP port lock: %v", err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return func() {
				_ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
				_ = file.Close()
			}
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EAGAIN) {
			_ = file.Close()
			t.Fatalf("acquire release E2E MCP port lock: %v", err)
		}
		if time.Now().After(deadline) {
			_ = file.Close()
			t.Fatalf("timed out waiting for release E2E MCP port lock")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestReleaseMCPPortLockIsKernelReleased(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mcp.lock")
	first, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	if err := syscall.Flock(int(first.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatalf("acquire first advisory lock: %v", err)
	}
	second, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if err := syscall.Flock(int(second.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("second advisory lock error = %v, want blocked", err)
	}
	if err := syscall.Flock(int(first.Fd()), syscall.LOCK_UN); err != nil {
		t.Fatalf("release first advisory lock: %v", err)
	}
	if err := syscall.Flock(int(second.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatalf("acquire advisory lock after release: %v", err)
	}
	_ = syscall.Flock(int(second.Fd()), syscall.LOCK_UN)
}
