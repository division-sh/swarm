package releasee2e

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

const (
	fakeDockerHelperEnv = "RELEASE_E2E_DOCKER_HELPER"
	fakeDockerRootEnv   = "RELEASE_E2E_DOCKER_ROOT"
)

type fakeDockerContainer struct {
	Running bool              `json:"running"`
	Labels  map[string]string `json:"labels,omitempty"`
}

type fakeDockerState struct {
	Containers map[string]fakeDockerContainer `json:"containers"`
}

type fakeDockerRecord struct {
	Class     string   `json:"class"`
	Args      []string `json:"args,omitempty"`
	SessionID string   `json:"session_id,omitempty"`
	ToolName  string   `json:"tool_name,omitempty"`
	MCPURL    string   `json:"mcp_url,omitempty"`
}

func TestMain(m *testing.M) {
	if os.Getenv(fakeDockerHelperEnv) == "1" {
		os.Exit(runFakeDocker(fakeDockerArgs(os.Args)))
	}
	os.Exit(m.Run())
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
	activeDir := filepath.Join(root, "active")
	if err := os.MkdirAll(activeDir, 0o700); err != nil {
		fmt.Fprintf(os.Stderr, "create fake Docker activity directory: %v\n", err)
		return 97
	}
	activePath := filepath.Join(activeDir, fmt.Sprint(os.Getpid()))
	if err := os.WriteFile(activePath, nil, 0o600); err != nil {
		fmt.Fprintf(os.Stderr, "record fake Docker activity: %v\n", err)
		return 97
	}
	defer os.Remove(activePath)
	if len(args) == 0 {
		return fakeDockerUnexpected(root, args, "missing command")
	}

	switch args[0] {
	case "version":
		recordFakeDocker(root, fakeDockerRecord{Class: "docker_version", Args: redactDockerArgs(args)})
		fmt.Fprintln(os.Stdout, "25.0.0")
		return 0
	case "network":
		if len(args) < 2 {
			return fakeDockerUnexpected(root, args, "missing network operation")
		}
		class := map[string]string{
			"inspect": "network_inspect",
			"create":  "network_create",
			"connect": "network_connect",
		}[args[1]]
		if class == "" {
			return fakeDockerUnexpected(root, args, "unsupported network operation")
		}
		recordFakeDocker(root, fakeDockerRecord{Class: class, Args: redactDockerArgs(args)})
		return 0
	case "image":
		if len(args) == 3 && args[1] == "inspect" {
			recordFakeDocker(root, fakeDockerRecord{Class: "image_inspect", Args: redactDockerArgs(args)})
			fmt.Fprintln(os.Stdout, "[]")
			return 0
		}
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
	case "container":
		return fakeDockerContainerCommand(root, args)
	case "exec":
		return fakeDockerExec(root, args)
	}
	return fakeDockerUnexpected(root, args, "unsupported command")
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

func fakeDockerExec(root string, args []string) int {
	containerIndex := 1
	for containerIndex < len(args) {
		switch args[containerIndex] {
		case "-i":
			containerIndex++
		case "-e", "-w":
			containerIndex += 2
		default:
			goto command
		}
	}

command:
	if containerIndex+1 >= len(args) {
		return fakeDockerUnexpected(root, args, "exec command is incomplete")
	}
	commandArgs := args[containerIndex+1:]
	if commandArgs[0] == "sh" && len(commandArgs) >= 3 && commandArgs[1] == "-lc" {
		recordFakeDocker(root, fakeDockerRecord{Class: "orphan_cleanup", Args: redactDockerArgs(args)})
		return 0
	}
	if filepath.Base(commandArgs[0]) != "claude" {
		return fakeDockerUnexpected(root, args, "exec command is not Claude CLI or cleanup")
	}
	input, err := io.ReadAll(os.Stdin)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read Claude stdin: %v\n", err)
		return 97
	}
	sessionID := dockerOptionValue(commandArgs, "--session-id")
	if sessionID == "" {
		return fakeDockerUnexpected(root, args, "Claude CLI invocation omitted --session-id")
	}
	startup := strings.Contains(string(input), "Startup validation probe")
	class := "claude_live"
	if startup {
		class = "claude_startup"
	}
	recordFakeDocker(root, fakeDockerRecord{
		Class:     class,
		Args:      redactDockerArgs(args),
		SessionID: sessionID,
	})
	if startup {
		tools := splitAllowedTools(dockerOptionValue(commandArgs, "--allowedTools"))
		writeClaudeInit(sessionID, tools)
		return 0
	}
	return runFakeClaudeTurn(root, commandArgs, sessionID)
}

func runFakeClaudeTurn(root string, args []string, sessionID string) int {
	rawConfig := dockerOptionValue(args, "--mcp-config")
	if rawConfig == "" {
		fmt.Fprintln(os.Stderr, "live Claude invocation omitted --mcp-config")
		return 97
	}
	var config struct {
		MCPServers map[string]struct {
			URL     string            `json:"url"`
			Headers map[string]string `json:"headers"`
		} `json:"mcpServers"`
	}
	if err := json.Unmarshal([]byte(rawConfig), &config); err != nil {
		fmt.Fprintf(os.Stderr, "decode --mcp-config: %v\n", err)
		return 97
	}
	server, ok := config.MCPServers["runtime-tools"]
	if !ok || server.URL == "" {
		fmt.Fprintln(os.Stderr, "runtime-tools MCP server is missing")
		return 97
	}
	hostURL, err := hostReachableMCPURL(server.URL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "resolve MCP URL: %v\n", err)
		return 97
	}
	client := &http.Client{Timeout: 15 * time.Second}
	if _, err := fakeMCPCall(client, hostURL, server.Headers, "initialize", map[string]any{
		"protocolVersion": "2025-03-26",
		"capabilities":    map[string]any{},
		"clientInfo":      map[string]any{"name": "release-e2e-claude", "version": "1.0.0"},
	}, "release-e2e-initialize", true); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 97
	}
	if _, err := fakeMCPCall(client, hostURL, server.Headers, "notifications/initialized", map[string]any{}, nil, false); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 97
	}
	listed, err := fakeMCPCall(client, hostURL, server.Headers, "tools/list", map[string]any{}, "release-e2e-list", true)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 97
	}
	toolName := firstEmitTool(listed)
	if toolName == "" {
		fmt.Fprintf(os.Stderr, "MCP tools/list has no authored emit tool: %#v\n", listed)
		return 97
	}

	writeClaudeInit(sessionID, []string{"mcp__runtime-tools__" + toolName})
	result, err := fakeMCPCall(client, hostURL, server.Headers, "tools/call", map[string]any{
		"name":      toolName,
		"arguments": map[string]any{"result": "release-e2e-complete"},
		"_meta":     map[string]any{"claudecode/toolUseId": "toolu-release-e2e"},
	}, "release-e2e-call", true)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 97
	}
	if isError, _ := result["isError"].(bool); isError {
		fmt.Fprintf(os.Stderr, "MCP emit returned an error result: %#v\n", result)
		return 97
	}
	recordFakeDocker(root, fakeDockerRecord{Class: "mcp_emit", ToolName: toolName, MCPURL: hostURL})
	_ = json.NewEncoder(os.Stdout).Encode(map[string]any{
		"type":       "result",
		"subtype":    "success",
		"session_id": sessionID,
		"result":     "release-e2e-complete",
	})
	return 0
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

func firstEmitTool(result map[string]any) string {
	tools, _ := result["tools"].([]any)
	for _, raw := range tools {
		tool, _ := raw.(map[string]any)
		name, _ := tool["name"].(string)
		if strings.HasPrefix(name, "emit_") {
			return name
		}
	}
	return ""
}

func hostReachableMCPURL(raw string) (string, error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	host := parsed.Hostname()
	if host == "host.docker.internal" {
		parsed.Host = "127.0.0.1:" + parsed.Port()
	}
	if parsed.Scheme != "http" || parsed.Host == "" || parsed.Path != "/mcp" {
		return "", fmt.Errorf("unexpected runtime MCP URL %q", raw)
	}
	return parsed.String(), nil
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
	recordFakeDocker(root, fakeDockerRecord{Class: "unexpected", Args: redactDockerArgs(args)})
	fmt.Fprintf(os.Stderr, "release E2E fake Docker rejected command (%s): %s\n", reason, strings.Join(redactDockerArgs(args), " "))
	return 97
}

func recordFakeDocker(root string, record fakeDockerRecord) {
	withFakeDockerLock(root, func() {
		path := filepath.Join(root, "calls.jsonl")
		file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
		if err != nil {
			fmt.Fprintf(os.Stderr, "open fake Docker call log: %v\n", err)
			return
		}
		defer file.Close()
		_ = json.NewEncoder(file).Encode(record)
	})
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
