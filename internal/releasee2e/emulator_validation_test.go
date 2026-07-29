package releasee2e

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestReleaseDockerCommandAdmissionRejectsMalformedShapes(t *testing.T) {
	validCreate := validReleaseScaffoldCreateArgs()
	cases := map[string][]string{
		"version shape":    {"version"},
		"network name":     {"network", "inspect", "wrong-network"},
		"image name":       {"image", "inspect", "wrong-image"},
		"preflight shape":  {"run", "definitely-not-a-valid-preflight"},
		"inspect target":   {"inspect", "--format", "{{.State.Running}}", "wrong-container"},
		"lifecycle target": {"start", "wrong-container"},
		"inventory shape":  {"container", "ls"},
	}
	for name, args := range cases {
		t.Run(name, func(t *testing.T) {
			if err := validateReleaseDockerCommand(args); err == nil {
				t.Fatalf("validateReleaseDockerCommand(%q) error = nil, want rejection", args)
			}
		})
	}

	t.Run("create image", func(t *testing.T) {
		args := append([]string(nil), validCreate...)
		args[len(args)-3] = "wrong-image"
		if err := validateReleaseDockerCommand(args); err == nil {
			t.Fatal("wrong create image was accepted")
		}
	})
	t.Run("create mount", func(t *testing.T) {
		args := append([]string(nil), validCreate...)
		replaceReleaseArgValue(args, "-v", "wrong-volume:/opt/swarm/scaffold")
		if err := validateReleaseDockerCommand(args); err == nil {
			t.Fatal("wrong create mount was accepted")
		}
	})
	t.Run("create identity", func(t *testing.T) {
		args := append([]string(nil), validCreate...)
		replaceReleaseArgValue(args, "--label", "dev.swarm.owner=foreign")
		if err := validateReleaseDockerCommand(args); err == nil {
			t.Fatal("wrong create identity was accepted")
		}
	})
}

func TestReleaseDockerExecAdmissionRejectsCredentialTargetAndClaudeMutations(t *testing.T) {
	valid := validReleaseClaudeDockerExecArgs(t, releaseE2ERawMCPURL)
	startupPrompt := []byte("Startup validation probe. Do not call any tools. Reply with the exact text ok.")
	if _, err := validateReleaseDockerExec(valid, startupPrompt); err != nil {
		t.Fatalf("valid fixture invocation rejected: %v", err)
	}

	credentialCases := map[string]func([]string) []string{
		"missing": func(args []string) []string {
			return removeReleaseOptionPair(args, "-e", "CLAUDE_CODE_OAUTH_TOKEN=")
		},
		"wrong": func(args []string) []string {
			replaceReleaseArgValuePrefix(args, "-e", "CLAUDE_CODE_OAUTH_TOKEN=", "CLAUDE_CODE_OAUTH_TOKEN=wrong")
			return args
		},
	}
	for phase, prompt := range map[string][]byte{
		"startup": startupPrompt,
		"live":    []byte("complete the authored work"),
	} {
		for mutation, mutate := range credentialCases {
			t.Run(phase+" "+mutation+" oauth", func(t *testing.T) {
				args := mutate(append([]string(nil), valid...))
				if _, err := validateReleaseDockerExec(args, prompt); err == nil {
					t.Fatalf("%s invocation with %s OAuth was accepted", phase, mutation)
				}
			})
		}
	}

	cases := map[string]func([]string) []string{
		"wrong gateway env": func(args []string) []string {
			replaceReleaseArgValuePrefix(args, "-e", "SWARM_TOOL_GATEWAY_URL=", "SWARM_TOOL_GATEWAY_URL=http://127.0.0.1:8082/mcp")
			return args
		},
		"wrong container": func(args []string) []string {
			for index, arg := range args {
				if arg == releaseE2EAgentContainer {
					args[index] = "totally-wrong-container"
					break
				}
			}
			return args
		},
		"wrong workdir": func(args []string) []string {
			replaceReleaseArgValue(args, "-w", "/tmp")
			return args
		},
		"missing strict MCP": func(args []string) []string {
			return removeReleaseFlag(args, "--strict-mcp-config")
		},
		"wrong output format": func(args []string) []string {
			replaceReleaseArgValue(args, "--output-format", "json")
			return args
		},
		"wrong allowed tools": func(args []string) []string {
			replaceReleaseArgValue(args, "--allowedTools", "Bash")
			return args
		},
		"unknown Claude flag": func(args []string) []string {
			return append(args, "--permission-mode", "bypassPermissions")
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			args := mutate(append([]string(nil), valid...))
			if _, err := validateReleaseDockerExec(args, startupPrompt); err == nil {
				t.Fatalf("mutated Docker exec was accepted: %q", args)
			}
		})
	}
}

func TestReleaseContainerMCPAdmissionRejectsRawEndpointMutations(t *testing.T) {
	for _, rawURL := range []string{
		"http://127.0.0.1:8082/mcp",
		"http://localhost:8082/mcp",
		"http://host.docker.internal:8083/mcp",
		"http://host.docker.internal:8082/wrong",
		"https://host.docker.internal:8082/mcp",
	} {
		t.Run(rawURL, func(t *testing.T) {
			if _, err := hostReachableMCPURL(rawURL); err == nil {
				t.Fatalf("hostReachableMCPURL(%q) error = nil, want rejection", rawURL)
			}
		})
	}

	args := validReleaseClaudeDockerExecArgs(t, "http://127.0.0.1:8082/mcp")
	if _, err := validateReleaseDockerExec(args, []byte("live turn")); err == nil {
		t.Fatal("loopback raw MCP config was accepted")
	}
}

func TestReleaseEvidenceRejectsDuplicateClosureAttempts(t *testing.T) {
	base := validReleaseEvidence()
	if err := validateReleaseDockerEvidence(base); err != nil {
		t.Fatalf("valid evidence rejected: %v", err)
	}
	for _, class := range []string{"claude_startup", "claude_live", "mcp_emit"} {
		t.Run(class, func(t *testing.T) {
			records := append([]fakeDockerRecord(nil), base...)
			for _, record := range base {
				if record.Class == class {
					records = append(records, record)
					break
				}
			}
			if err := validateReleaseDockerEvidence(records); err == nil || !strings.Contains(err.Error(), "want exactly 1") {
				t.Fatalf("duplicate %s validation error = %v, want exact-cardinality rejection", class, err)
			}
		})
	}
}

func TestReleaseEmulatorRejectsDuplicateAttemptBeforeRecording(t *testing.T) {
	root := t.TempDir()
	record := fakeDockerRecord{Class: "claude_live", SessionID: "11111111-1111-1111-1111-111111111111"}
	if !recordUniqueFakeDocker(root, record) {
		t.Fatal("first live attempt was rejected")
	}
	if recordUniqueFakeDocker(root, record) {
		t.Fatal("duplicate live attempt was accepted")
	}
}

func validReleaseClaudeDockerExecArgs(t *testing.T, rawURL string) []string {
	t.Helper()
	config := map[string]any{
		"mcpServers": map[string]any{
			"runtime-tools": map[string]any{
				"type": "http",
				"url":  rawURL,
				"headers": map[string]string{
					"Authorization":         "Bearer release-e2e-gateway",
					"X-SWARM-Context-Token": "release-e2e-context",
				},
			},
		},
	}
	rawConfig, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	return []string{
		"exec", "-i",
		"-e", "SWARM_TOOL_GATEWAY_URL=" + releaseE2ERawMCPURL,
		"-e", "CLAUDE_CODE_OAUTH_TOKEN=" + releaseE2EOAuthToken,
		"-w", releaseE2EAgentWorkdir,
		releaseE2EAgentContainer, "claude",
		"-p",
		"--session-id", "11111111-1111-1111-1111-111111111111",
		"--output-format", "stream-json",
		"--include-partial-messages",
		"--verbose",
		"--system-prompt", "release worker",
		"--disallowedTools", "Bash",
		"--allowedTools", "ExitPlanMode,mcp__runtime-tools__emit_work_completed",
		"--mcp-config", string(rawConfig),
		"--strict-mcp-config",
	}
}

func validReleaseScaffoldCreateArgs() []string {
	return []string{
		"create",
		"--name", "swarm-scaffold",
		"--network", releaseE2ENetwork,
		"--label", "dev.swarm.bundle_hash=bundle-v1:sha256:" + strings.Repeat("a", 64),
		"--label", "dev.swarm.container.kind=scaffold",
		"--label", "dev.swarm.container.name=swarm-scaffold",
		"--label", "dev.swarm.creation_source=workspace.EnsureSystemWorkspaces",
		"--label", "dev.swarm.owner=runtime",
		"--label", "dev.swarm.reset.eligible=false",
		"--label", "dev.swarm.workspace.scope=scaffold",
		"-v", "scaffold:/opt/swarm/scaffold",
		"-w", "/opt/swarm/scaffold",
		releaseE2EWorkspaceImage, "sleep", "infinity",
	}
}

func validReleaseEvidence() []fakeDockerRecord {
	return []fakeDockerRecord{
		{Class: "docker_version"},
		{Class: "network_inspect"},
		{Class: "image_inspect"},
		{Class: "cli_preflight"},
		{Class: "container_create"},
		{Class: "container_start"},
		{Class: "network_connect"},
		{
			Class:     "claude_startup",
			Args:      []string{"claude", "--allowedTools", "ExitPlanMode,mcp__runtime-tools__emit_work_completed"},
			SessionID: "11111111-1111-1111-1111-111111111111",
			RawMCPURL: releaseE2ERawMCPURL,
			MCPURL:    releaseE2EHostMCPURL,
		},
		{
			Class:     "claude_live",
			SessionID: "22222222-2222-2222-2222-222222222222",
			RawMCPURL: releaseE2ERawMCPURL,
			MCPURL:    releaseE2EHostMCPURL,
		},
		{
			Class:     "mcp_emit",
			ToolName:  "emit_work_completed",
			RawMCPURL: releaseE2ERawMCPURL,
			MCPURL:    releaseE2EHostMCPURL,
		},
	}
}

func replaceReleaseArgValue(args []string, name, value string) {
	for index := 0; index+1 < len(args); index++ {
		if args[index] == name {
			args[index+1] = value
			return
		}
	}
}

func replaceReleaseArgValuePrefix(args []string, name, prefix, value string) {
	for index := 0; index+1 < len(args); index++ {
		if args[index] == name && strings.HasPrefix(args[index+1], prefix) {
			args[index+1] = value
			return
		}
	}
}

func removeReleaseOptionPair(args []string, name, valuePrefix string) []string {
	for index := 0; index+1 < len(args); index++ {
		if args[index] == name && strings.HasPrefix(args[index+1], valuePrefix) {
			return append(args[:index:index], args[index+2:]...)
		}
	}
	return args
}

func removeReleaseFlag(args []string, name string) []string {
	for index, arg := range args {
		if arg == name {
			return append(args[:index:index], args[index+1:]...)
		}
	}
	return args
}
