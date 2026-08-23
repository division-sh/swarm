package releasee2e

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func TestClaudeCLIPositiveSelectorAgainstInstalledWorkspace(t *testing.T) {
	if os.Getenv("SWARM_CLAUDE_SELECTOR_E2E") != "1" {
		t.Skip("set SWARM_CLAUDE_SELECTOR_E2E=1 to run: go test ./internal/releasee2e -run TestClaudeCLIPositiveSelectorAgainstInstalledWorkspace -count=1")
	}
	docker, err := exec.LookPath("docker")
	if err != nil {
		t.Skip("Docker is unavailable")
	}
	image := strings.TrimSpace(os.Getenv("SWARM_CLAUDE_SELECTOR_WORKSPACE_IMAGE"))
	if image == "" {
		image = releaseE2EWorkspaceImage
	}
	if output, err := exec.Command(docker, "image", "inspect", image).CombinedOutput(); err != nil {
		t.Skipf("workspace image %s is unavailable: %v (%s)", image, err, bytes.TrimSpace(output))
	}

	selected := releaseE2EBuiltinTools()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, docker,
		"run", "--rm", "-i",
		"-e", "CLAUDE_CODE_OAUTH_TOKEN=invalid-selector-proof-token",
		image,
		"claude", "-p",
		"--output-format", "stream-json",
		"--verbose",
		"--tools", strings.Join(selected, ","),
		"--allowedTools", strings.Join(selected, ","),
	)
	cmd.Stdin = strings.NewReader("Reply with the exact text ok.")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	_ = cmd.Run() // Authentication may fail after the initialization evidence.
	if ctx.Err() != nil {
		t.Fatalf("installed Claude selector proof timed out: %v", ctx.Err())
	}
	visible, found, err := claudeInitTools(stdout.Bytes())
	if err != nil {
		t.Fatalf("parse installed Claude initialization: %v", err)
	}
	if !found {
		t.Fatalf("installed Claude emitted no system/init evidence; stderr=%q stdout=%q", redactSelectorOutput(stderr.String()), redactSelectorOutput(stdout.String()))
	}
	if !slices.Equal(visible, selected) {
		t.Fatalf("installed Claude system/init.tools = %#v, want exact positive selection %#v", visible, selected)
	}
}

func TestClaudeCLIPaidAgenticLifecycleFromReleaseBinaryDefaults(t *testing.T) {
	if os.Getenv("SWARM_CLAUDE_PAID_AGENTIC_E2E") != "1" {
		t.Skip("set SWARM_CLAUDE_PAID_AGENTIC_E2E=1 to run: go test ./internal/releasee2e -run TestClaudeCLIPaidAgenticLifecycleFromReleaseBinaryDefaults -count=1")
	}
	credential := strings.TrimSpace(os.Getenv("CLAUDE_CODE_OAUTH_TOKEN"))
	if credential == "" {
		t.Fatal("CLAUDE_CODE_OAUTH_TOKEN is required when SWARM_CLAUDE_PAID_AGENTIC_E2E=1")
	}
	docker, err := exec.LookPath("docker")
	if err != nil {
		t.Fatal("Docker is required for the paid Claude agentic E2E")
	}
	if output, err := exec.Command(docker, "image", "inspect", releaseE2EWorkspaceImage).CombinedOutput(); err != nil {
		t.Fatalf("default workspace image %s is unavailable: %v (%s)", releaseE2EWorkspaceImage, err, bytes.TrimSpace(output))
	}

	repo := releaseE2ERepoRoot(t)
	releaseRoot := t.TempDir()
	binaryPath := buildReleaseBinary(t, releaseRoot)
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
		t.Fatalf("paid release project unexpectedly contains swarm.yaml: %v", err)
	}

	home := filepath.Join(releaseRoot, "home")
	env := releaseProcessEnv(filepath.Dir(docker), filepath.Join(releaseRoot, "unused-fake-docker-state"), home)
	secrets := runReleaseCommand(t, 45*time.Second, releaseRoot, env, credential+"\n",
		binaryPath, "secrets", "set", "CLAUDE_CODE_OAUTH_TOKEN", "--stdin")
	if secrets.err != nil {
		t.Fatalf("public credential setup failed: %v\n%s", secrets.err, secrets.output)
	}
	if strings.Contains(secrets.output, credential) {
		t.Fatal("public credential setup leaked the provider credential")
	}

	releaseLock := acquireReleaseMCPPortLock(t)
	defer releaseLock()
	requireDefaultMCPPortAvailable(t)
	apiPort := freeReleaseTCPPort(t)
	run := runReleaseCommand(t, 4*time.Minute, releaseRoot, env, "",
		binaryPath,
		"run", "start",
		"--backend", "claude_cli",
		"--api-port", fmt.Sprint(apiPort),
		"--event", "work.requested",
		"--payload", payloadPath,
	)
	if run.err != nil {
		t.Fatalf("paid public-binary agentic flow failed: %v\n%s", run.err, run.output)
	}
	for _, want := range []string{"run started:", "run terminal:", "status=completed"} {
		if !strings.Contains(run.output, want) {
			t.Fatalf("paid public-binary output missing %q:\n%s", want, run.output)
		}
	}
	for _, forbidden := range []string{"typed delivery mismatch", "platform.internal_failure", "dead letter", credential} {
		if strings.Contains(strings.ToLower(run.output), strings.ToLower(forbidden)) {
			t.Fatalf("paid public-binary output contains %q:\n%s", forbidden, run.output)
		}
	}
	assertPaidAgenticToolEvidence(t, filepath.Join(releaseRoot, ".swarm", "stores", "dev.db"))
}

func claudeInitTools(raw []byte) ([]string, bool, error) {
	scanner := bufio.NewScanner(bytes.NewReader(raw))
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		var message map[string]any
		if json.Unmarshal(scanner.Bytes(), &message) != nil ||
			!strings.EqualFold(strings.TrimSpace(fmt.Sprint(message["type"])), "system") ||
			!strings.EqualFold(strings.TrimSpace(fmt.Sprint(message["subtype"])), "init") {
			continue
		}
		rawTools, ok := message["tools"].([]any)
		if !ok {
			return nil, false, fmt.Errorf("system/init.tools is not an array")
		}
		tools := make([]string, 0, len(rawTools))
		for _, rawTool := range rawTools {
			name, ok := rawTool.(string)
			if !ok || strings.TrimSpace(name) == "" {
				return nil, false, fmt.Errorf("system/init.tools contains a non-string or empty name")
			}
			tools = append(tools, strings.TrimSpace(name))
		}
		slices.Sort(tools)
		return tools, true, nil
	}
	return nil, false, scanner.Err()
}

func redactSelectorOutput(value string) string {
	return strings.ReplaceAll(value, "invalid-selector-proof-token", "<redacted>")
}

func assertPaidAgenticToolEvidence(t *testing.T, databasePath string) {
	t.Helper()
	db, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatalf("open paid agentic SQLite evidence: %v", err)
	}
	defer db.Close()
	rows, err := db.Query(`SELECT COALESCE(CAST(tool_calls AS TEXT), '[]'), COALESCE(CAST(failure AS TEXT), '') FROM agent_turns ORDER BY created_at`)
	if err != nil {
		t.Fatalf("read paid agentic turn evidence: %v", err)
	}
	defer rows.Close()
	var toolEvidence strings.Builder
	for rows.Next() {
		var toolCalls, failure string
		if err := rows.Scan(&toolCalls, &failure); err != nil {
			t.Fatalf("scan paid agentic turn evidence: %v", err)
		}
		if strings.TrimSpace(failure) != "" {
			t.Fatalf("paid agentic turn persisted failure: %s", failure)
		}
		toolEvidence.WriteString(toolCalls)
		toolEvidence.WriteByte('\n')
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate paid agentic turn evidence: %v", err)
	}
	evidence := toolEvidence.String()
	if !strings.Contains(evidence, "web_search") || !strings.Contains(evidence, "emit_work_completed") {
		t.Fatalf("paid agentic tool evidence does not prove native web use plus MCP emit:\n%s", evidence)
	}
}
