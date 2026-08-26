package releasee2e

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"
)

const (
	fakeDockerHelperEnv        = "RELEASE_E2E_DOCKER_HELPER"
	fakeDockerRootEnv          = "RELEASE_E2E_DOCKER_ROOT"
	fakeDockerMCPEmitGateEnv   = "RELEASE_E2E_MCP_EMIT_GATE"
	releaseE2EOAuthToken       = "release-e2e-oauth-value"
	releaseE2ERawMCPURL        = "http://host.docker.internal:8082/mcp"
	releaseE2EHostMCPURL       = "http://127.0.0.1:8082/mcp"
	releaseE2EWorkspaceImage   = "swarm-workspace:latest"
	releaseE2ENetwork          = "mas_default"
	releaseE2EAgentWorkdir     = "/workspace"
	releaseE2EManagedModel     = "sonnet"
	releaseE2EAgentFingerprint = "1f16a79924a40cd1e6e42105063bd4b8d5e3332769c777508831bb193986d791"
	releaseE2EAgentContainer   = "swarm-agent-" + releaseE2EAgentFingerprint
	releaseE2EAgentVolume      = "workspaces_agent_" + releaseE2EAgentFingerprint
	releaseE2EOrphanKill       = `if command -v pkill >/dev/null 2>&1; then
  pkill -KILL -f '(^|/)(claude|codex)( |$)' >/dev/null 2>&1 || true
else
  for p in /proc/[0-9]*; do
    cmd=$(tr '\000' ' ' < "$p/cmdline" 2>/dev/null || true)
    case "$cmd" in
      *claude*|*codex*) kill -9 "${p##*/}" >/dev/null 2>&1 || true ;;
    esac
  done
fi`
)

type fakeDockerContainer struct {
	Running bool              `json:"running"`
	Labels  map[string]string `json:"labels,omitempty"`
}

type fakeDockerState struct {
	Containers map[string]fakeDockerContainer `json:"containers"`
}

type fakeDockerRecord struct {
	Class      string   `json:"class"`
	Args       []string `json:"args,omitempty"`
	SessionID  string   `json:"session_id,omitempty"`
	ToolName   string   `json:"tool_name,omitempty"`
	ToolStatus string   `json:"tool_status,omitempty"`
	MailboxID  string   `json:"mailbox_id,omitempty"`
	RawMCPURL  string   `json:"raw_mcp_url,omitempty"`
	MCPURL     string   `json:"mcp_url,omitempty"`
	Reason     string   `json:"reason,omitempty"`
}

type fakeClaudeInvocation struct {
	commandArgs []string
	sessionID   string
	startup     bool
	rawMCPURL   string
	hostMCPURL  string
	headers     map[string]string
}

func TestMain(m *testing.M) {
	if os.Getenv(fakeDockerHelperEnv) == "1" {
		os.Exit(runFakeDocker(fakeDockerArgs(os.Args)))
	}
	os.Exit(m.Run())
}

func beginFakeDockerActivity(root string) (func(), error) {
	activeDir := filepath.Join(root, "active")
	if err := os.MkdirAll(activeDir, 0o700); err != nil {
		return nil, fmt.Errorf("create activity directory: %w", err)
	}
	file, err := os.CreateTemp(root, ".fake-docker-active-*")
	if err != nil {
		return nil, fmt.Errorf("create activity marker: %w", err)
	}
	cleanup := func() {
		_ = file.Close()
		_ = os.Remove(file.Name())
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		cleanup()
		return nil, fmt.Errorf("lock activity marker: %w", err)
	}
	activePath := filepath.Join(activeDir, fmt.Sprint(os.Getpid()))
	if err := os.Rename(file.Name(), activePath); err != nil {
		_ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
		cleanup()
		return nil, fmt.Errorf("publish activity marker: %w", err)
	}
	return func() {
		_ = os.Remove(activePath)
		_ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
		_ = file.Close()
	}, nil
}

func liveFakeDockerProcesses(root string) ([]string, error) {
	entries, err := os.ReadDir(filepath.Join(root, "active"))
	if err != nil {
		return nil, err
	}
	var live []string
	for _, entry := range entries {
		path := filepath.Join(root, "active", entry.Name())
		file, err := os.OpenFile(path, os.O_RDWR, 0)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("open activity marker %q: %w", entry.Name(), err)
		}
		lockErr := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		switch {
		case lockErr == nil:
			_ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
			_ = file.Close()
			if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				return nil, fmt.Errorf("remove stale activity marker %q: %w", entry.Name(), err)
			}
		case errors.Is(lockErr, syscall.EWOULDBLOCK), errors.Is(lockErr, syscall.EAGAIN):
			_ = file.Close()
			live = append(live, entry.Name())
		default:
			_ = file.Close()
			return nil, fmt.Errorf("probe activity marker %q: %w", entry.Name(), lockErr)
		}
	}
	sort.Strings(live)
	return live, nil
}

func TestFakeDockerActivityTracksOnlyLiveProcesses(t *testing.T) {
	root := t.TempDir()
	endActivity, err := beginFakeDockerActivity(root)
	if err != nil {
		t.Fatal(err)
	}
	live, err := liveFakeDockerProcesses(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(live) != 1 {
		t.Fatalf("live fake Docker processes = %v, want one", live)
	}
	endActivity()

	stalePath := filepath.Join(root, "active", "stale")
	if err := os.WriteFile(stalePath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	live, err = liveFakeDockerProcesses(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(live) != 0 {
		t.Fatalf("live fake Docker processes after exit = %v, want none", live)
	}
	if _, err := os.Stat(stalePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale activity marker still exists: %v", err)
	}
}

func fakeDockerArgs(args []string) []string {
	for i, arg := range args {
		if arg == "--" {
			return append([]string(nil), args[i+1:]...)
		}
	}
	return nil
}

func runFakeDocker(args []string) int {
	root := strings.TrimSpace(os.Getenv(fakeDockerRootEnv))
	if root == "" {
		fmt.Fprintln(os.Stderr, "release E2E fake Docker state root is missing")
		return 97
	}
	endActivity, err := beginFakeDockerActivity(root)
	if err != nil {
		fmt.Fprintf(os.Stderr, "record fake Docker activity: %v\n", err)
		return 97
	}
	defer endActivity()
	if len(args) == 0 {
		return fakeDockerUnexpected(root, args, "missing command")
	}
	if args[0] != "exec" {
		if err := validateReleaseDockerCommand(root, args); err != nil {
			return fakeDockerUnexpected(root, args, err.Error())
		}
	}

	switch args[0] {
	case "version":
		recordFakeDocker(root, fakeDockerRecord{Class: "docker_version", Args: redactDockerArgs(args)})
		fmt.Fprintln(os.Stdout, "25.0.0")
		return 0
	case "network":
		class := map[string]string{
			"inspect": "network_inspect",
			"create":  "network_create",
			"connect": "network_connect",
		}[args[1]]
		recordFakeDocker(root, fakeDockerRecord{Class: class, Args: redactDockerArgs(args)})
		return 0
	case "image":
		recordFakeDocker(root, fakeDockerRecord{Class: "image_inspect", Args: redactDockerArgs(args)})
		fmt.Fprintln(os.Stdout, "[]")
		return 0
	case "run":
		recordFakeDocker(root, fakeDockerRecord{Class: "cli_preflight", Args: redactDockerArgs(args)})
		return 0
	case "inspect":
		return fakeDockerInspect(root, args)
	case "create":
		return fakeDockerCreate(root, args)
	case "start":
		return fakeDockerSetRunning(root, args, true)
	case "stop":
		return fakeDockerSetRunning(root, args, false)
	case "rm":
		return fakeDockerRemove(root, args)
	case "container":
		return fakeDockerContainerCommand(root, args)
	case "exec":
		return fakeDockerExec(root, args)
	}
	return fakeDockerUnexpected(root, args, "unsupported command")
}

func validateReleaseDockerCommand(root string, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("missing command")
	}
	switch args[0] {
	case "version":
		if !equalStrings(args, []string{"version", "--format", "{{.Server.Version}}"}) {
			return fmt.Errorf("unsupported Docker version shape")
		}
	case "network":
		switch {
		case equalStrings(args, []string{"network", "inspect", releaseE2ENetwork}):
		case equalStrings(args, []string{"network", "create", releaseE2ENetwork}):
		case len(args) == 4 && args[1] == "connect" && args[2] == releaseE2ENetwork && releaseE2EContainerName(args[3]):
		default:
			return fmt.Errorf("unsupported Docker network shape")
		}
	case "image":
		if !equalStrings(args, []string{"image", "inspect", releaseE2EWorkspaceImage}) {
			return fmt.Errorf("unsupported Docker image shape")
		}
	case "run":
		want := []string{
			"run", "--rm", "--entrypoint", "sh", releaseE2EWorkspaceImage,
			"-lc", `command -v -- "$1" >/dev/null && "$1" --version >/dev/null`,
			"swarm-cli-proof", "claude",
		}
		if !equalStrings(args, want) {
			return fmt.Errorf("unsupported Claude CLI preflight shape")
		}
	case "inspect":
		if len(args) != 4 || args[1] != "--format" || !releaseE2EContainerName(args[3]) {
			return fmt.Errorf("unsupported Docker inspect shape")
		}
		switch args[2] {
		case "{{.State.Running}}", "{{json .}}", "{{json .Mounts}}", "{{json .Config.Labels}}":
		default:
			return fmt.Errorf("unsupported Docker inspect format")
		}
	case "create":
		if err := validateReleaseDockerCreate(root, args); err != nil {
			return err
		}
	case "start", "stop":
		if len(args) != 2 || !releaseE2EContainerName(args[1]) {
			return fmt.Errorf("unsupported Docker container lifecycle shape")
		}
	case "rm":
		if len(args) != 3 || args[1] != "--force" || !releaseE2EContainerName(args[2]) {
			return fmt.Errorf("unsupported Docker container removal shape")
		}
	case "container":
		want := []string{
			"container", "ls", "--all",
			"--filter", "label=dev.swarm.owner=runtime",
			"--filter", "label=dev.swarm.reset.eligible=true",
			"--format", "{{.Names}}",
		}
		if !equalStrings(args, want) {
			return fmt.Errorf("unsupported Docker container inventory shape")
		}
	default:
		return fmt.Errorf("unsupported command")
	}
	return nil
}

type releaseDockerCreate struct {
	name       string
	network    string
	workdir    string
	privileged bool
	labels     map[string]string
	mounts     []string
	command    []string
}

func validateReleaseDockerCreate(root string, args []string) error {
	create, err := parseReleaseDockerCreate(args)
	if err != nil {
		return err
	}
	if !releaseE2EContainerName(create.name) {
		return fmt.Errorf("unexpected create container %q", create.name)
	}
	if create.network != releaseE2ENetwork {
		return fmt.Errorf("create network = %q, want %q", create.network, releaseE2ENetwork)
	}
	if !equalStrings(create.command, []string{releaseE2EWorkspaceImage, "sleep", "infinity"}) {
		return fmt.Errorf("unexpected create image or command")
	}

	type expectation struct {
		workdir       string
		privileged    bool
		kind          string
		resetEligible string
		source        string
		scope         string
		requiredMount map[string]string
	}
	expected := map[string]expectation{
		"swarm-scaffold": {
			workdir:       "/opt/swarm/scaffold",
			kind:          "scaffold",
			resetEligible: "false",
			source:        "workspace.EnsureSystemWorkspaces",
			scope:         "scaffold",
			requiredMount: map[string]string{"/opt/swarm/scaffold": "scaffold"},
		},
		"swarm-system": {
			workdir:       "/opt/swarm",
			privileged:    true,
			kind:          "system",
			resetEligible: "false",
			source:        "workspace.EnsureSystemWorkspaces",
			scope:         "system",
			requiredMount: map[string]string{
				"/opt/swarm/entities": "entities",
				"/opt/swarm/nginx":    "nginx",
				"/etc/systemd/system": "systemd",
			},
		},
		releaseE2EAgentContainer: {
			workdir:       releaseE2EAgentWorkdir,
			kind:          "agent",
			resetEligible: "true",
			source:        "workspace.ResolveWorkspace",
			scope:         "per-agent",
			requiredMount: map[string]string{releaseE2EAgentWorkdir: releaseE2EAgentVolume},
		},
	}[create.name]
	if create.workdir != expected.workdir || create.privileged != expected.privileged {
		return fmt.Errorf("unexpected create workdir or privilege for %s", create.name)
	}
	if err := validateReleaseDockerMounts(root, create.mounts, expected.requiredMount); err != nil {
		return fmt.Errorf("create %s mounts: %w", create.name, err)
	}
	if err := validateReleaseDockerLabels(create, expected.kind, expected.resetEligible, expected.source, expected.scope); err != nil {
		return err
	}
	return nil
}

func parseReleaseDockerCreate(args []string) (releaseDockerCreate, error) {
	create := releaseDockerCreate{labels: map[string]string{}}
	if len(args) < 2 || args[0] != "create" {
		return create, fmt.Errorf("unsupported Docker create shape")
	}
	for index := 1; index < len(args); {
		switch args[index] {
		case "--name", "--network", "-w", "-v", "--label":
			if index+1 >= len(args) {
				return create, fmt.Errorf("create option %s omitted its value", args[index])
			}
			option, value := args[index], args[index+1]
			switch option {
			case "--name":
				if create.name != "" {
					return create, fmt.Errorf("create repeated --name")
				}
				create.name = value
			case "--network":
				if create.network != "" {
					return create, fmt.Errorf("create repeated --network")
				}
				create.network = value
			case "-w":
				if create.workdir != "" {
					return create, fmt.Errorf("create repeated -w")
				}
				create.workdir = value
			case "-v":
				create.mounts = append(create.mounts, value)
			case "--label":
				key, labelValue, ok := strings.Cut(value, "=")
				if !ok || key == "" || labelValue == "" {
					return create, fmt.Errorf("create has malformed label")
				}
				if _, duplicate := create.labels[key]; duplicate {
					return create, fmt.Errorf("create repeated label %s", key)
				}
				create.labels[key] = labelValue
			}
			index += 2
		case "--privileged":
			if create.privileged {
				return create, fmt.Errorf("create repeated --privileged")
			}
			create.privileged = true
			index++
		default:
			create.command = append([]string(nil), args[index:]...)
			index = len(args)
		}
	}
	if create.name == "" || create.network == "" || create.workdir == "" || len(create.command) == 0 {
		return create, fmt.Errorf("create omitted required fixture identity")
	}
	return create, nil
}

func validateReleaseDockerMounts(root string, raw []string, required map[string]string) error {
	releaseRoot := filepath.Dir(filepath.Clean(root))
	requiredProjectMounts := map[string]string{
		"/opt/swarm/contracts": filepath.Join(releaseRoot, "contracts"),
	}
	seen := map[string]string{}
	for _, mount := range raw {
		parts := strings.Split(mount, ":")
		if len(parts) < 2 || len(parts) > 3 {
			return fmt.Errorf("malformed mount %q", mount)
		}
		source, target := strings.Join(parts[:len(parts)-1], ":"), parts[len(parts)-1]
		mode := ""
		if len(parts) == 3 {
			source, target, mode = parts[0], parts[1], parts[2]
		}
		if source == "" || target == "" {
			return fmt.Errorf("malformed mount %q", mount)
		}
		switch target {
		case "/opt/swarm/contracts":
			wantSource := requiredProjectMounts[target]
			if mode != "ro" || !filepath.IsAbs(source) || !releasePathsEqual(source, wantSource) {
				return fmt.Errorf("project mount %q does not match %s:%s:ro", mount, wantSource, target)
			}
		default:
			wantSource, ok := required[target]
			if !ok || mode != "" || source != wantSource {
				return fmt.Errorf("unexpected workspace mount %q", mount)
			}
		}
		if _, duplicate := seen[target]; duplicate {
			return fmt.Errorf("duplicate mount target %q", target)
		}
		seen[target] = source
	}
	for target := range requiredProjectMounts {
		if _, ok := seen[target]; !ok {
			return fmt.Errorf("missing required project mount target %q", target)
		}
	}
	for target := range required {
		if _, ok := seen[target]; !ok {
			return fmt.Errorf("missing required mount target %q", target)
		}
	}
	return nil
}

func releasePathsEqual(left, right string) bool {
	if filepath.Clean(left) == filepath.Clean(right) {
		return true
	}
	resolvedLeft, leftErr := filepath.EvalSymlinks(left)
	resolvedRight, rightErr := filepath.EvalSymlinks(right)
	return leftErr == nil && rightErr == nil && filepath.Clean(resolvedLeft) == filepath.Clean(resolvedRight)
}

func validateReleaseDockerLabels(create releaseDockerCreate, kind, resetEligible, source, scope string) error {
	required := map[string]string{
		"dev.swarm.owner":           "runtime",
		"dev.swarm.container.kind":  kind,
		"dev.swarm.reset.eligible":  resetEligible,
		"dev.swarm.creation_source": source,
		"dev.swarm.container.name":  create.name,
		"dev.swarm.workspace.scope": scope,
	}
	for key, want := range required {
		if create.labels[key] != want {
			return fmt.Errorf("create %s label %s = %q, want %q", create.name, key, create.labels[key], want)
		}
	}
	allowed := map[string]bool{
		"dev.swarm.owner":           true,
		"dev.swarm.container.kind":  true,
		"dev.swarm.reset.eligible":  true,
		"dev.swarm.creation_source": true,
		"dev.swarm.container.name":  true,
		"dev.swarm.workspace.scope": true,
	}
	if bundleHash := create.labels["dev.swarm.bundle_hash"]; bundleHash != "" {
		allowed["dev.swarm.bundle_hash"] = true
		if !validReleaseBundleHash(bundleHash) {
			return fmt.Errorf("create %s has invalid bundle hash identity", create.name)
		}
	}
	if create.name == releaseE2EAgentContainer {
		wantIdentityLabels := map[string]string{
			"dev.swarm.agent_id":                 "release-worker",
			"dev.swarm.agent_name_owner":         "claude-cli-release-lifecycle://flows/worker/release-worker",
			"dev.swarm.agent_name_source":        "declared",
			"dev.swarm.agent_route_presence":     "present",
			"dev.swarm.agent_flow_scope_key":     "worker",
			"dev.swarm.agent_flow_instance_id":   "worker",
			"dev.swarm.agent_flow_instance_path": "worker",
		}
		for key, want := range wantIdentityLabels {
			allowed[key] = true
			if create.labels[key] != want {
				return fmt.Errorf("create agent identity label %s = %q, want %q", key, create.labels[key], want)
			}
		}
		if runID := create.labels["dev.swarm.run_id"]; runID != "" {
			allowed["dev.swarm.run_id"] = true
			if !validReleaseUUID(runID) {
				return fmt.Errorf("create agent run identity is invalid")
			}
		}
	}
	if len(create.labels) != len(allowed) {
		return fmt.Errorf("create %s label set is not exact", create.name)
	}
	for key := range create.labels {
		if !allowed[key] {
			return fmt.Errorf("create %s has unsupported label %s", create.name, key)
		}
	}
	return nil
}

func releaseE2EContainerName(name string) bool {
	switch name {
	case "swarm-scaffold", "swarm-system":
		return true
	default:
		return name == releaseE2EAgentContainer
	}
}

func equalStrings(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for index := range got {
		if got[index] != want[index] {
			return false
		}
	}
	return true
}

func fakeDockerInspect(root string, args []string) int {
	if len(args) != 4 || args[1] != "--format" {
		return fakeDockerUnexpected(root, args, "unsupported inspect shape")
	}
	format, name := args[2], args[3]
	var container fakeDockerContainer
	var exists bool
	withFakeDockerState(root, func(state *fakeDockerState) {
		container, exists = state.Containers[name]
	})
	recordFakeDocker(root, fakeDockerRecord{Class: "container_inspect", Args: redactDockerArgs(args)})
	if !exists {
		fmt.Fprintf(os.Stderr, "Error: No such object: %s\n", name)
		return 1
	}
	switch format {
	case "{{.State.Running}}":
		fmt.Fprintln(os.Stdout, container.Running)
	case "{{json .Config.Labels}}":
		_ = json.NewEncoder(os.Stdout).Encode(container.Labels)
	case "{{json .}}":
		_ = json.NewEncoder(os.Stdout).Encode(map[string]any{
			"State":  map[string]any{"Running": container.Running},
			"Config": map[string]any{"Labels": container.Labels},
		})
	case "{{json .Mounts}}":
		fmt.Fprintln(os.Stdout, "[]")
	default:
		return fakeDockerUnexpected(root, args, "unsupported inspect format")
	}
	return 0
}

func fakeDockerCreate(root string, args []string) int {
	name := dockerOptionValue(args, "--name")
	if name == "" {
		return fakeDockerUnexpected(root, args, "create omitted --name")
	}
	labels := map[string]string{}
	for i := 1; i+1 < len(args); i++ {
		if args[i] != "--label" {
			continue
		}
		key, value, ok := strings.Cut(args[i+1], "=")
		if ok {
			labels[key] = value
		}
		i++
	}
	withFakeDockerState(root, func(state *fakeDockerState) {
		state.Containers[name] = fakeDockerContainer{Labels: labels}
	})
	recordFakeDocker(root, fakeDockerRecord{Class: "container_create", Args: redactDockerArgs(args)})
	fmt.Fprintln(os.Stdout, "release-e2e-container")
	return 0
}

func fakeDockerSetRunning(root string, args []string, running bool) int {
	if len(args) != 2 {
		return fakeDockerUnexpected(root, args, "container lifecycle command requires one name")
	}
	name := args[1]
	var exists bool
	withFakeDockerState(root, func(state *fakeDockerState) {
		container, ok := state.Containers[name]
		exists = ok
		if ok {
			container.Running = running
			state.Containers[name] = container
		}
	})
	if !exists {
		fmt.Fprintf(os.Stderr, "Error: No such object: %s\n", name)
		return 1
	}
	class := "container_stop"
	if running {
		class = "container_start"
	}
	recordFakeDocker(root, fakeDockerRecord{Class: class, Args: redactDockerArgs(args)})
	return 0
}

func fakeDockerRemove(root string, args []string) int {
	if len(args) != 3 || args[1] != "--force" {
		return fakeDockerUnexpected(root, args, "container removal requires --force and one name")
	}
	name := args[2]
	var exists bool
	withFakeDockerState(root, func(state *fakeDockerState) {
		_, exists = state.Containers[name]
		delete(state.Containers, name)
	})
	if !exists {
		fmt.Fprintf(os.Stderr, "Error: No such object: %s\n", name)
		return 1
	}
	recordFakeDocker(root, fakeDockerRecord{Class: "container_remove", Args: redactDockerArgs(args)})
	return 0
}

func fakeDockerContainerCommand(root string, args []string) int {
	if len(args) < 2 || args[1] != "ls" {
		return fakeDockerUnexpected(root, args, "unsupported container operation")
	}
	var names []string
	withFakeDockerState(root, func(state *fakeDockerState) {
		for name := range state.Containers {
			names = append(names, name)
		}
	})
	sort.Strings(names)
	recordFakeDocker(root, fakeDockerRecord{Class: "container_list", Args: redactDockerArgs(args)})
	for _, name := range names {
		fmt.Fprintln(os.Stdout, name)
	}
	return 0
}

func validateReleaseDockerExec(args []string, input []byte) (fakeClaudeInvocation, error) {
	var invocation fakeClaudeInvocation
	if len(args) == 5 && args[0] == "exec" && releaseE2EContainerName(args[1]) &&
		args[2] == "sh" && args[3] == "-lc" && args[4] == releaseE2EOrphanKill {
		if len(bytes.TrimSpace(input)) != 0 {
			return invocation, fmt.Errorf("orphan cleanup received unexpected stdin")
		}
		invocation.commandArgs = append([]string(nil), args[2:]...)
		return invocation, nil
	}
	if len(args) < 2 || args[0] != "exec" {
		return invocation, fmt.Errorf("unsupported Docker exec shape")
	}

	interactive := false
	env := map[string]string{}
	workdir := ""
	index := 1
	for index < len(args) {
		switch args[index] {
		case "-i":
			if interactive {
				return invocation, fmt.Errorf("Docker exec repeated -i")
			}
			interactive = true
			index++
		case "-e":
			if index+1 >= len(args) {
				return invocation, fmt.Errorf("Docker exec -e omitted its value")
			}
			key, value, ok := strings.Cut(args[index+1], "=")
			if !ok || key == "" {
				return invocation, fmt.Errorf("Docker exec has malformed environment")
			}
			if _, duplicate := env[key]; duplicate {
				return invocation, fmt.Errorf("Docker exec repeated environment %s", key)
			}
			env[key] = value
			index += 2
		case "-w":
			if index+1 >= len(args) || workdir != "" {
				return invocation, fmt.Errorf("Docker exec has malformed workdir")
			}
			workdir = args[index+1]
			index += 2
		default:
			goto target
		}
	}

target:
	if !interactive || index+1 >= len(args) {
		return invocation, fmt.Errorf("Claude Docker exec is incomplete")
	}
	container := args[index]
	invocation.commandArgs = append([]string(nil), args[index+1:]...)
	if container != releaseE2EAgentContainer {
		return invocation, fmt.Errorf("Claude Docker exec container = %q, want %q", container, releaseE2EAgentContainer)
	}
	if workdir != releaseE2EAgentWorkdir {
		return invocation, fmt.Errorf("Claude Docker exec workdir = %q, want %q", workdir, releaseE2EAgentWorkdir)
	}
	if len(env) != 2 || env["SWARM_TOOL_GATEWAY_URL"] != releaseE2ERawMCPURL ||
		env["CLAUDE_CODE_OAUTH_TOKEN"] != releaseE2EOAuthToken {
		return invocation, fmt.Errorf("Claude Docker exec environment is incomplete or invalid")
	}
	if len(invocation.commandArgs) == 0 || invocation.commandArgs[0] != "claude" {
		return invocation, fmt.Errorf("Docker exec command is not the configured Claude CLI")
	}
	prompt := strings.TrimSpace(string(input))
	if prompt == "" {
		return invocation, fmt.Errorf("Claude invocation received empty stdin")
	}
	invocation.startup = prompt == "Startup validation probe. Do not call any tools. Reply with the exact text ok."
	if !invocation.startup && strings.Contains(prompt, "Startup validation probe") {
		return invocation, fmt.Errorf("startup probe prompt is not exact")
	}
	if err := validateReleaseClaudeArgs(invocation.commandArgs[1:], invocation.startup); err != nil {
		return invocation, err
	}
	invocation.sessionID = dockerOptionValue(invocation.commandArgs, "--session-id")
	rawMCPURL, hostMCPURL, headers, err := validateReleaseMCPConfig(dockerOptionValue(invocation.commandArgs, "--mcp-config"))
	if err != nil {
		return invocation, err
	}
	invocation.rawMCPURL = rawMCPURL
	invocation.hostMCPURL = hostMCPURL
	invocation.headers = headers

	return invocation, nil
}

func validateReleaseClaudeArgs(args []string, startup bool) error {
	valueFlags := map[string]string{}
	boolFlags := map[string]bool{}
	valueNames := map[string]bool{
		"--session-id":    true,
		"--output-format": true,
		"--system-prompt": true,
		"--tools":         true,
		"--allowedTools":  true,
		"--mcp-config":    true,
		"--model":         true,
	}
	boolNames := map[string]bool{
		"-p":                         true,
		"--include-partial-messages": true,
		"--verbose":                  true,
		"--strict-mcp-config":        true,
	}
	for index := 0; index < len(args); {
		name := args[index]
		if valueNames[name] {
			if index+1 >= len(args) || valueFlags[name] != "" {
				return fmt.Errorf("Claude flag %s is missing or repeated", name)
			}
			valueFlags[name] = args[index+1]
			index += 2
			continue
		}
		if boolNames[name] {
			if boolFlags[name] {
				return fmt.Errorf("Claude flag %s is repeated", name)
			}
			boolFlags[name] = true
			index++
			continue
		}
		return fmt.Errorf("Claude invocation contains unsupported argument %q", name)
	}
	for _, name := range []string{"-p", "--include-partial-messages", "--verbose", "--strict-mcp-config"} {
		if !boolFlags[name] {
			return fmt.Errorf("Claude invocation omitted %s", name)
		}
	}
	if !validReleaseUUID(valueFlags["--session-id"]) {
		return fmt.Errorf("Claude --session-id is not a canonical UUID")
	}
	if valueFlags["--output-format"] != "stream-json" {
		return fmt.Errorf("Claude --output-format = %q, want stream-json", valueFlags["--output-format"])
	}
	if strings.TrimSpace(valueFlags["--system-prompt"]) == "" {
		return fmt.Errorf("Claude invocation omitted --system-prompt")
	}
	if tools := splitAllowedTools(valueFlags["--tools"]); !equalStrings(tools, releaseE2EBuiltinTools()) {
		return fmt.Errorf("Claude --tools = %q, want exact builtin surface", valueFlags["--tools"])
	}
	wantTools := releaseE2ELiveAllowedTools()
	if startup {
		wantTools = releaseE2EStartupAllowedTools()
	}
	if tools := splitAllowedTools(valueFlags["--allowedTools"]); !equalStrings(tools, wantTools) {
		return fmt.Errorf("Claude --allowedTools = %q, want fixture tool surface", valueFlags["--allowedTools"])
	}
	if strings.TrimSpace(valueFlags["--mcp-config"]) == "" {
		return fmt.Errorf("Claude invocation omitted --mcp-config")
	}
	if !startup && valueFlags["--model"] != releaseE2EManagedModel {
		return fmt.Errorf("Claude --model = %q, want sealed managed model %q", valueFlags["--model"], releaseE2EManagedModel)
	}
	return nil
}

func validateReleaseMCPConfig(raw string) (string, string, map[string]string, error) {
	if strings.TrimSpace(raw) == "" {
		return "", "", nil, fmt.Errorf("Claude invocation omitted --mcp-config")
	}
	var root map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &root); err != nil {
		return "", "", nil, fmt.Errorf("decode --mcp-config: %w", err)
	}
	if len(root) != 1 {
		return "", "", nil, fmt.Errorf("--mcp-config top-level shape is not exact")
	}
	var servers map[string]json.RawMessage
	if err := json.Unmarshal(root["mcpServers"], &servers); err != nil || len(servers) != 1 {
		return "", "", nil, fmt.Errorf("--mcp-config runtime server set is not exact")
	}
	var server map[string]json.RawMessage
	if err := json.Unmarshal(servers["runtime-tools"], &server); err != nil || len(server) != 3 {
		return "", "", nil, fmt.Errorf("runtime-tools MCP server shape is not exact")
	}
	var serverType, rawURL string
	var headers map[string]string
	if err := json.Unmarshal(server["type"], &serverType); err != nil || serverType != "http" {
		return "", "", nil, fmt.Errorf("runtime-tools MCP type is not http")
	}
	if err := json.Unmarshal(server["url"], &rawURL); err != nil {
		return "", "", nil, fmt.Errorf("runtime-tools MCP URL is invalid")
	}
	if err := json.Unmarshal(server["headers"], &headers); err != nil || len(headers) != 2 {
		return "", "", nil, fmt.Errorf("runtime-tools MCP headers are not exact")
	}
	authorization := strings.TrimSpace(headers["Authorization"])
	contextToken := strings.TrimSpace(headers["X-SWARM-Context-Token"])
	if !strings.HasPrefix(authorization, "Bearer ") || strings.TrimSpace(strings.TrimPrefix(authorization, "Bearer ")) == "" || contextToken == "" {
		return "", "", nil, fmt.Errorf("runtime-tools MCP authorization/context headers are incomplete")
	}
	hostURL, err := hostReachableMCPURL(rawURL)
	if err != nil {
		return "", "", nil, err
	}
	return rawURL, hostURL, headers, nil
}

func fakeDockerExec(root string, args []string) int {
	input, err := io.ReadAll(os.Stdin)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read Docker exec stdin: %v\n", err)
		return 97
	}
	invocation, err := validateReleaseDockerExec(args, input)
	if err != nil {
		return fakeDockerUnexpected(root, args, err.Error())
	}
	if len(invocation.commandArgs) >= 3 && invocation.commandArgs[0] == "sh" && invocation.commandArgs[1] == "-lc" {
		recordFakeDocker(root, fakeDockerRecord{Class: "orphan_cleanup", Args: redactDockerArgs(args)})
		return 0
	}
	class := "claude_live"
	if invocation.startup {
		class = "claude_startup"
	}
	if !recordUniqueFakeDocker(root, fakeDockerRecord{
		Class:     class,
		Args:      redactDockerArgs(args),
		SessionID: invocation.sessionID,
		RawMCPURL: invocation.rawMCPURL,
		MCPURL:    invocation.hostMCPURL,
	}) {
		return fakeDockerUnexpected(root, args, "duplicate "+class+" attempt")
	}
	if invocation.startup {
		tools := splitAllowedTools(dockerOptionValue(invocation.commandArgs, "--allowedTools"))
		writeClaudeInit(invocation.sessionID, tools)
		return 0
	}
	return runFakeClaudeTurn(root, invocation)
}

func runFakeClaudeTurn(root string, invocation fakeClaudeInvocation) int {
	client := &http.Client{Timeout: 15 * time.Second}
	if _, err := fakeMCPCall(client, invocation.hostMCPURL, invocation.headers, "initialize", map[string]any{
		"protocolVersion": "2025-03-26",
		"capabilities":    map[string]any{},
		"clientInfo":      map[string]any{"name": "release-e2e-claude", "version": "1.0.0"},
	}, "release-e2e-initialize", true); err != nil {
		return fakeDockerUnexpected(root, invocation.commandArgs, err.Error())
	}
	if _, err := fakeMCPCall(client, invocation.hostMCPURL, invocation.headers, "notifications/initialized", map[string]any{}, nil, false); err != nil {
		return fakeDockerUnexpected(root, invocation.commandArgs, err.Error())
	}
	listed, err := fakeMCPCall(client, invocation.hostMCPURL, invocation.headers, "tools/list", map[string]any{}, "release-e2e-list", true)
	if err != nil {
		return fakeDockerUnexpected(root, invocation.commandArgs, err.Error())
	}
	toolName, err := exactReleaseFlowEmitTool(listed)
	if err != nil {
		return fakeDockerUnexpected(root, invocation.commandArgs, err.Error())
	}

	writeClaudeInit(invocation.sessionID, splitAllowedTools(dockerOptionValue(invocation.commandArgs, "--allowedTools")))
	noticeResult, err := fakeMCPCall(client, invocation.hostMCPURL, invocation.headers, "tools/call", map[string]any{
		"name": "notify_human",
		"arguments": map[string]any{
			"summary": "Strong job match found",
			"context": map[string]any{
				"company": "Example Labs", "job_title": "Senior Platform Engineer",
				"posting_url": "https://example.test/jobs/senior-platform-engineer",
			},
		},
		"_meta": map[string]any{"claudecode/toolUseId": "toolu-release-e2e-notice"},
	}, "release-e2e-notice-call", true)
	if err != nil {
		return fakeDockerUnexpected(root, invocation.commandArgs, err.Error())
	}
	if isError, _ := noticeResult["isError"].(bool); isError {
		return fakeDockerUnexpected(root, invocation.commandArgs, fmt.Sprintf("MCP notify_human returned an error result: %#v", noticeResult))
	}
	noticeStatus, mailboxID, err := exactReleaseNoticeToolResult(noticeResult)
	if err != nil {
		return fakeDockerUnexpected(root, invocation.commandArgs, err.Error())
	}
	if !recordUniqueFakeDocker(root, fakeDockerRecord{
		Class: "mcp_notify", ToolName: "notify_human", ToolStatus: noticeStatus, MailboxID: mailboxID,
		RawMCPURL: invocation.rawMCPURL, MCPURL: invocation.hostMCPURL,
	}) {
		return fakeDockerUnexpected(root, invocation.commandArgs, "duplicate mcp_notify attempt")
	}
	if err := waitForFakeDockerMCPEmitGate(os.Getenv(fakeDockerMCPEmitGateEnv)); err != nil {
		return fakeDockerUnexpected(root, invocation.commandArgs, err.Error())
	}
	result, err := fakeMCPCall(client, invocation.hostMCPURL, invocation.headers, "tools/call", map[string]any{
		"name":      toolName,
		"arguments": map[string]any{"flow_result": "release-e2e-flow-complete"},
		"_meta":     map[string]any{"claudecode/toolUseId": "toolu-release-e2e"},
	}, "release-e2e-call", true)
	if err != nil {
		return fakeDockerUnexpected(root, invocation.commandArgs, err.Error())
	}
	if isError, _ := result["isError"].(bool); isError {
		return fakeDockerUnexpected(root, invocation.commandArgs, fmt.Sprintf("MCP emit returned an error result: %#v", result))
	}
	if !recordUniqueFakeDocker(root, fakeDockerRecord{
		Class:     "mcp_emit",
		ToolName:  toolName,
		RawMCPURL: invocation.rawMCPURL,
		MCPURL:    invocation.hostMCPURL,
	}) {
		return fakeDockerUnexpected(root, invocation.commandArgs, "duplicate mcp_emit attempt")
	}
	_ = json.NewEncoder(os.Stdout).Encode(map[string]any{
		"type":       "result",
		"subtype":    "success",
		"session_id": invocation.sessionID,
		"result":     "release-e2e-flow-complete",
	})
	return 0
}

func exactReleaseNoticeToolResult(result map[string]any) (string, string, error) {
	var payload map[string]any
	if structured, ok := result["structuredContent"].(map[string]any); ok {
		payload = structured
	}
	if payload == nil {
		content, _ := result["content"].([]any)
		if len(content) != 1 {
			return "", "", fmt.Errorf("MCP notify_human result content = %#v, want one exact result", result["content"])
		}
		entry, _ := content[0].(map[string]any)
		if entry["type"] != "text" {
			return "", "", fmt.Errorf("MCP notify_human result entry = %#v, want text", entry)
		}
		text, _ := entry["text"].(string)
		if err := json.Unmarshal([]byte(text), &payload); err != nil {
			return "", "", fmt.Errorf("decode MCP notify_human result %q: %w", text, err)
		}
	}
	status, _ := payload["status"].(string)
	mailboxID, _ := payload["mailbox_id"].(string)
	if status != "queued" || strings.TrimSpace(mailboxID) == "" || len(payload) != 2 {
		return "", "", fmt.Errorf("MCP notify_human result = %#v, want exact queued/mailbox_id", payload)
	}
	return status, mailboxID, nil
}

func waitForFakeDockerMCPEmitGate(gatePath string) error {
	gatePath = strings.TrimSpace(gatePath)
	if gatePath == "" {
		return nil
	}
	if err := os.WriteFile(gatePath+".ready", []byte("ready\n"), 0o600); err != nil {
		return fmt.Errorf("publish MCP emit gate readiness: %w", err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		if _, err := os.Stat(gatePath); err == nil {
			return nil
		} else if !os.IsNotExist(err) {
			return fmt.Errorf("inspect MCP emit gate: %w", err)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("wait for MCP emit gate: deadline exceeded")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func writeClaudeInit(sessionID string, tools []string) {
	_ = json.NewEncoder(os.Stdout).Encode(map[string]any{
		"type":       "system",
		"subtype":    "init",
		"session_id": sessionID,
		"mcp_servers": []map[string]string{{
			"name": "runtime-tools", "status": "connected",
		}},
		"tools": tools,
	})
	_ = os.Stdout.Sync()
}

func fakeMCPCall(client *http.Client, endpoint string, headers map[string]string, method string, params map[string]any, id any, wantResponse bool) (map[string]any, error) {
	body, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"method":  method,
		"params":  params,
		"id":      id,
	})
	if err != nil {
		return nil, err
	}
	request, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("MCP %s request: %w", method, err)
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, fmt.Errorf("read MCP %s response: %w", method, err)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, fmt.Errorf("MCP %s status %d: %s", method, response.StatusCode, raw)
	}
	if !wantResponse {
		return nil, nil
	}
	var rpc struct {
		Result map[string]any `json:"result"`
		Error  any            `json:"error"`
	}
	if err := json.Unmarshal(raw, &rpc); err != nil {
		return nil, fmt.Errorf("decode MCP %s response: %w: %s", method, err, raw)
	}
	if rpc.Error != nil {
		return nil, fmt.Errorf("MCP %s error: %#v", method, rpc.Error)
	}
	return rpc.Result, nil
}

func exactReleaseFlowEmitTool(result map[string]any) (string, error) {
	tools, _ := result["tools"].([]any)
	wantNames := []string{"emit_agent_completed", "notify_human", "read_worker_state", "read_worker_state_requests"}
	names := listedToolNames(tools)
	sort.Strings(names)
	if !equalStrings(names, wantNames) {
		return "", fmt.Errorf("MCP tools/list names = %#v, want exact contextual surface %#v", names, wantNames)
	}
	notifyValidated := false
	emitName := ""
	for _, raw := range tools {
		tool, _ := raw.(map[string]any)
		name, _ := tool["name"].(string)
		if name == "notify_human" {
			wantSchema := map[string]any{
				"type": "object", "additionalProperties": false, "required": []any{"summary"},
				"properties": map[string]any{"summary": map[string]any{"type": "string", "minLength": float64(1)}, "context": map[string]any{}},
			}
			wantDescription := "Sends an informational notice to the human operator. Does NOT request approval and does not pause the flow - to ask for a decision that gates the flow, use ask_human.\n\nUsage:\nUse for an informational operator notice only. Provide summary and optional context. The flow continues without waiting for a reply; use ask_human when a human verdict must gate the flow."
			if tool["description"] != wantDescription || !jsonValuesEqual(tool["inputSchema"], wantSchema) {
				return "", fmt.Errorf("MCP notify_human definition = %#v, want exact description/schema", tool)
			}
			notifyValidated = true
			continue
		}
		if name != "emit_agent_completed" {
			continue
		}
		description, _ := tool["description"].(string)
		if description != "Emit worker/agent.completed event\n\nUsage:\n"+releaseE2EEmitToolUsage {
			return "", fmt.Errorf("MCP flow emit description = %q, want exact actor/flow-scoped definition", description)
		}
		wantSchema := map[string]any{
			"type": "object",
			"properties": map[string]any{
				"flow_result": map[string]any{"type": "string"},
			},
			"required":             []any{"flow_result"},
			"additionalProperties": false,
		}
		if !jsonValuesEqual(tool["inputSchema"], wantSchema) {
			return "", fmt.Errorf("MCP flow emit schema = %#v, want %#v", tool["inputSchema"], wantSchema)
		}
		emitName = name
	}
	if !notifyValidated {
		return "", fmt.Errorf("MCP tools/list has no exact notify_human definition")
	}
	if emitName == "" {
		return "", fmt.Errorf("MCP tools/list has no exact flow-scoped emit_agent_completed tool: %#v", listedToolNames(tools))
	}
	return emitName, nil
}

const releaseE2EEmitToolUsage = "Call this emit_* tool only to publish the named workflow event. Provide concrete JSON payload values matching the input schema. Do not include envelope-owned fields unless the schema declares them. Arguments are concrete payload values, not workflow expressions."

func jsonValuesEqual(left, right any) bool {
	leftJSON, leftErr := json.Marshal(left)
	rightJSON, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftJSON, rightJSON)
}

func listedToolNames(tools []any) []string {
	names := make([]string, 0, len(tools))
	for _, raw := range tools {
		tool, _ := raw.(map[string]any)
		name, _ := tool["name"].(string)
		names = append(names, name)
	}
	return names
}

func hostReachableMCPURL(raw string) (string, error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	if parsed.Scheme != "http" ||
		parsed.Hostname() != "host.docker.internal" ||
		parsed.Port() != "8082" ||
		parsed.Path != "/mcp" ||
		parsed.RawQuery != "" ||
		parsed.Fragment != "" ||
		parsed.User != nil {
		return "", fmt.Errorf("runtime MCP URL %q is not the default container-reachable endpoint", raw)
	}
	return releaseE2EHostMCPURL, nil
}

func splitAllowedTools(raw string) []string {
	var tools []string
	for _, tool := range strings.Split(raw, ",") {
		if tool = strings.TrimSpace(tool); tool != "" {
			tools = append(tools, tool)
		}
	}
	sort.Strings(tools)
	return tools
}

func dockerOptionValue(args []string, name string) string {
	for i := 0; i+1 < len(args); i++ {
		if args[i] == name {
			return args[i+1]
		}
	}
	return ""
}

func validReleaseBundleHash(value string) bool {
	const prefix = "bundle-v1:sha256:"
	if !strings.HasPrefix(value, prefix) {
		return false
	}
	digest := strings.TrimPrefix(value, prefix)
	if len(digest) != 64 {
		return false
	}
	_, err := hex.DecodeString(digest)
	return err == nil && strings.ToLower(digest) == digest
}

func validReleaseUUID(value string) bool {
	if len(value) != 36 || value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' {
		return false
	}
	compact := strings.ReplaceAll(value, "-", "")
	if len(compact) != 32 {
		return false
	}
	_, err := hex.DecodeString(compact)
	return err == nil && strings.ToLower(value) == value
}

func redactDockerArgs(args []string) []string {
	redacted := append([]string(nil), args...)
	for i := range redacted {
		if i > 0 && redacted[i-1] == "--mcp-config" {
			redacted[i] = "<runtime-owned-mcp-config>"
			continue
		}
		if i > 0 && redacted[i-1] == "-e" && strings.HasPrefix(redacted[i], "CLAUDE_CODE_OAUTH_TOKEN=") {
			redacted[i] = "CLAUDE_CODE_OAUTH_TOKEN=<redacted>"
		}
	}
	return redacted
}

func fakeDockerUnexpected(root string, args []string, reason string) int {
	recordFakeDocker(root, fakeDockerRecord{Class: "unexpected", Args: redactDockerArgs(args), Reason: reason})
	fmt.Fprintf(os.Stderr, "release E2E fake Docker rejected command (%s): %s\n", reason, strings.Join(redactDockerArgs(args), " "))
	return 97
}

func recordFakeDocker(root string, record fakeDockerRecord) {
	withFakeDockerLock(root, func() {
		appendFakeDockerRecord(root, record)
	})
}

func recordUniqueFakeDocker(root string, record fakeDockerRecord) bool {
	unique := true
	withFakeDockerLock(root, func() {
		path := filepath.Join(root, "calls.jsonl")
		raw, err := os.ReadFile(path)
		if err != nil && !os.IsNotExist(err) {
			fmt.Fprintf(os.Stderr, "read fake Docker call log: %v\n", err)
			unique = false
			return
		}
		for _, line := range bytes.Split(raw, []byte{'\n'}) {
			if len(bytes.TrimSpace(line)) == 0 {
				continue
			}
			var existing fakeDockerRecord
			if err := json.Unmarshal(line, &existing); err != nil {
				fmt.Fprintf(os.Stderr, "decode fake Docker call log: %v\n", err)
				unique = false
				return
			}
			if existing.Class == record.Class {
				unique = false
				return
			}
		}
		appendFakeDockerRecord(root, record)
	})
	return unique
}

func appendFakeDockerRecord(root string, record fakeDockerRecord) {
	path := filepath.Join(root, "calls.jsonl")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open fake Docker call log: %v\n", err)
		return
	}
	defer file.Close()
	_ = json.NewEncoder(file).Encode(record)
}

func withFakeDockerState(root string, mutate func(*fakeDockerState)) {
	withFakeDockerLock(root, func() {
		path := filepath.Join(root, "state.json")
		state := fakeDockerState{Containers: map[string]fakeDockerContainer{}}
		if raw, err := os.ReadFile(path); err == nil {
			_ = json.Unmarshal(raw, &state)
		}
		if state.Containers == nil {
			state.Containers = map[string]fakeDockerContainer{}
		}
		mutate(&state)
		raw, err := json.Marshal(state)
		if err != nil {
			fmt.Fprintf(os.Stderr, "marshal fake Docker state: %v\n", err)
			return
		}
		tmp := path + ".tmp"
		if err := os.WriteFile(tmp, raw, 0o600); err != nil {
			fmt.Fprintf(os.Stderr, "write fake Docker state: %v\n", err)
			return
		}
		if err := os.Rename(tmp, path); err != nil {
			fmt.Fprintf(os.Stderr, "publish fake Docker state: %v\n", err)
		}
	})
}

func withFakeDockerLock(root string, action func()) {
	lock := filepath.Join(root, "lock")
	deadline := time.Now().Add(5 * time.Second)
	for {
		if err := os.Mkdir(lock, 0o700); err == nil {
			defer os.Remove(lock)
			action()
			return
		}
		if time.Now().After(deadline) {
			fmt.Fprintln(os.Stderr, "timed out acquiring fake Docker state lock")
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
}
