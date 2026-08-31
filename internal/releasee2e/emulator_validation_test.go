package releasee2e

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReleaseDockerCommandAdmissionRejectsMalformedShapes(t *testing.T) {
	root := filepath.Join(t.TempDir(), "fake-docker-state")
	releaseRoot := filepath.Dir(root)
	admittedRoot := filepath.Join(releaseRoot, "contracts")
	writeReleaseFile(t, filepath.Join(admittedRoot, "schema.yaml"), "stages: {}\n")
	projectionRoot, err := os.MkdirTemp("", "swarm-source-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(projectionRoot) })
	copyReleaseTree(t, admittedRoot, projectionRoot)
	validCreate := validReleaseScaffoldCreateArgs(projectionRoot)
	if err := validateReleaseDockerCommand(root, validCreate); err != nil {
		t.Fatalf("valid fixture create rejected: %v", err)
	}
	validAdmissionAgent := validReleaseAgentCreateArgs(projectionRoot, false)
	if err := validateReleaseDockerCommand(root, validAdmissionAgent); err != nil {
		t.Fatalf("valid run-independent agent create rejected: %v", err)
	}
	validExecutionAgent := validReleaseAgentCreateArgs(projectionRoot, true)
	if err := validateReleaseDockerCommand(root, validExecutionAgent); err != nil {
		t.Fatalf("valid run-bound agent create rejected: %v", err)
	}
	if err := validateReleaseDockerCommand(root, []string{"inspect", "--format", "{{json .Config.Labels}}", "swarm-" + releaseE2EFixtureScope + "-scaffold"}); err != nil {
		t.Fatalf("runtime identity label inspection rejected: %v", err)
	}
	cases := map[string][]string{
		"version shape":    {"version"},
		"network name":     {"network", "inspect", "wrong-network"},
		"image name":       {"image", "inspect", "wrong-image"},
		"preflight shape":  {"run", "definitely-not-a-valid-preflight"},
		"inspect target":   {"inspect", "--format", "{{.State.Running}}", "wrong-container"},
		"unscoped target":  {"inspect", "--format", "{{.State.Running}}", "swarm-scaffold"},
		"malformed scope":  {"inspect", "--format", "{{.State.Running}}", "swarm-bundle-not-hex-projection-not-hex-scaffold"},
		"lifecycle target": {"start", "wrong-container"},
		"removal target":   {"rm", "--force", "wrong-container"},
		"inventory shape":  {"container", "ls"},
	}
	for name, args := range cases {
		t.Run(name, func(t *testing.T) {
			if err := validateReleaseDockerCommand(root, args); err == nil {
				t.Fatalf("validateReleaseDockerCommand(%q) error = nil, want rejection", args)
			}
		})
	}

	t.Run("create image", func(t *testing.T) {
		args := append([]string(nil), validCreate...)
		args[len(args)-3] = "wrong-image"
		if err := validateReleaseDockerCommand(root, args); err == nil {
			t.Fatal("wrong create image was accepted")
		}
	})
	t.Run("create mount", func(t *testing.T) {
		args := append([]string(nil), validCreate...)
		replaceReleaseArgValue(args, "-v", "wrong-volume:/opt/swarm/scaffold")
		if err := validateReleaseDockerCommand(root, args); err == nil {
			t.Fatal("wrong create mount was accepted")
		}
	})
	t.Run("create identity", func(t *testing.T) {
		args := append([]string(nil), validCreate...)
		replaceReleaseArgValue(args, "--label", "dev.swarm.owner=foreign")
		if err := validateReleaseDockerCommand(root, args); err == nil {
			t.Fatal("wrong create identity was accepted")
		}
	})
	t.Run("create bundle scope mismatch", func(t *testing.T) {
		args := append([]string(nil), validCreate...)
		replaceReleaseArgValuePrefix(args, "--label", "dev.swarm.bundle_hash=", "dev.swarm.bundle_hash=bundle-v2:sha256:"+strings.Repeat("b", 64))
		if err := validateReleaseDockerCommand(root, args); err == nil {
			t.Fatal("create name and bundle hash with different scopes were accepted")
		}
	})
	for name, mutate := range map[string]func([]string) []string{
		"missing data mount": func(args []string) []string {
			return removeReleaseCreateMount(args, "/data")
		},
		"writable data mount": func(args []string) []string {
			return replaceReleaseCreateMount(args, "/data", filepath.Join(os.TempDir(), "swarm", "data-projections", "a_"+strings.Repeat("b", 64))+":/data:rw")
		},
		"wrong projection source": func(args []string) []string {
			return replaceReleaseCreateMount(args, "/data", filepath.Join(os.TempDir(), "swarm", "data-projections", "a_"+strings.Repeat("c", 64))+":/data:ro")
		},
		"missing projection identity": func(args []string) []string {
			return removeReleaseOptionPair(args, "--label", "dev.swarm.data_projection_id=")
		},
		"malformed projection identity": func(args []string) []string {
			replaceReleaseArgValuePrefix(args, "--label", "dev.swarm.data_projection_id=", "dev.swarm.data_projection_id=data-projection-v1:sha256:deadbeef")
			return args
		},
		"projection without run": func(args []string) []string {
			return removeReleaseOptionPair(args, "--label", "dev.swarm.run_id=")
		},
	} {
		t.Run("run-bound agent "+name, func(t *testing.T) {
			args := mutate(append([]string(nil), validExecutionAgent...))
			if err := validateReleaseDockerCommand(root, args); err == nil {
				t.Fatalf("run-bound agent create with %s was accepted", name)
			}
		})
	}

	projectMounts := map[string]string{
		"/opt/swarm/source": projectionRoot,
	}
	for target, source := range projectMounts {
		t.Run("missing "+target, func(t *testing.T) {
			args := removeReleaseCreateMount(append([]string(nil), validCreate...), target)
			if err := validateReleaseDockerCommand(root, args); err == nil {
				t.Fatalf("create without %s was accepted", target)
			}
		})
		t.Run("wrong source "+target, func(t *testing.T) {
			args := replaceReleaseCreateMount(append([]string(nil), validCreate...), target, filepath.Join(releaseRoot, "wrong")+":"+target+":ro")
			if err := validateReleaseDockerCommand(root, args); err == nil {
				t.Fatalf("create with wrong %s source was accepted", target)
			}
		})
		t.Run("writable "+target, func(t *testing.T) {
			args := replaceReleaseCreateMount(append([]string(nil), validCreate...), target, source+":"+target+":rw")
			if err := validateReleaseDockerCommand(root, args); err == nil {
				t.Fatalf("create with writable %s was accepted", target)
			}
		})
		t.Run("duplicate "+target, func(t *testing.T) {
			args := insertReleaseCreateMount(append([]string(nil), validCreate...), source+":"+target+":ro")
			if err := validateReleaseDockerCommand(root, args); err == nil {
				t.Fatalf("create with duplicate %s was accepted", target)
			}
		})
	}
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
				if arg == releaseE2EFixtureAgent {
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
		"wrong builtin tools": func(args []string) []string {
			replaceReleaseArgValue(args, "--tools", "Bash")
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
	for name, mutate := range map[string]func([]string) []string{
		"missing managed model": func(args []string) []string {
			return removeReleaseOptionPair(args, "--model", "")
		},
		"wrong managed model": func(args []string) []string {
			replaceReleaseArgValue(args, "--model", "hostile-model")
			return args
		},
	} {
		t.Run(name, func(t *testing.T) {
			args := mutate(append([]string(nil), valid...))
			if _, err := validateReleaseDockerExec(args, []byte("complete the authored work")); err == nil {
				t.Fatalf("live invocation with %s was accepted: %q", name, args)
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
	for _, class := range []string{"claude_startup", "claude_live", "mcp_notify", "mcp_emit"} {
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
	for name, mutate := range map[string]func(*fakeDockerRecord){
		"wrong notice status": func(record *fakeDockerRecord) { record.ToolStatus = "ok" },
		"missing mailbox id":  func(record *fakeDockerRecord) { record.MailboxID = "" },
	} {
		t.Run(name, func(t *testing.T) {
			records := append([]fakeDockerRecord(nil), base...)
			for index := range records {
				if records[index].Class == "mcp_notify" {
					mutate(&records[index])
					break
				}
			}
			if err := validateReleaseDockerEvidence(records); err == nil || !strings.Contains(err.Error(), "want exact queued result") {
				t.Fatalf("mutated notice-result validation error = %v, want exact-result rejection", err)
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
		releaseE2EFixtureAgent, "claude",
		"-p",
		"--session-id", "11111111-1111-1111-1111-111111111111",
		"--output-format", "stream-json",
		"--include-partial-messages",
		"--verbose",
		"--system-prompt", "release worker",
		"--tools", strings.Join(releaseE2EBuiltinTools(), ","),
		"--allowedTools", strings.Join(releaseE2EStartupAllowedTools(), ","),
		"--mcp-config", string(rawConfig),
		"--strict-mcp-config",
		"--model", releaseE2EManagedModel,
	}
}

func validReleaseScaffoldCreateArgs(sourceProjection string) []string {
	name := "swarm-" + releaseE2EFixtureScope + "-scaffold"
	return []string{
		"create",
		"--name", name,
		"--network", releaseE2ENetwork,
		"--label", "dev.swarm.bundle_hash=bundle-v2:sha256:" + strings.Repeat("a", 64),
		"--label", "dev.swarm.container.kind=scaffold",
		"--label", "dev.swarm.container.name=" + name,
		"--label", "dev.swarm.creation_source=workspace.EnsureSystemWorkspaces",
		"--label", "dev.swarm.owner=runtime",
		"--label", "dev.swarm.reset.eligible=false",
		"--label", "dev.swarm.source_projection=" + releaseE2EProjectionID,
		"--label", "dev.swarm.workspace.scope=scaffold",
		"-v", sourceProjection + ":/opt/swarm/source:ro",
		"-v", name + ":/opt/swarm/scaffold",
		"-w", "/opt/swarm/scaffold",
		releaseE2EWorkspaceImage, "sleep", "infinity",
	}
}

func validReleaseAgentCreateArgs(sourceProjection string, runBound bool) []string {
	args := []string{
		"create",
		"--name", releaseE2EFixtureAgent,
		"--network", releaseE2ENetwork,
		"--label", "dev.swarm.bundle_hash=bundle-v2:sha256:" + strings.Repeat("a", 64),
		"--label", "dev.swarm.agent_flow_instance_id=worker",
		"--label", "dev.swarm.agent_flow_instance_path=worker",
		"--label", "dev.swarm.agent_flow_scope_key=worker",
		"--label", "dev.swarm.agent_id=release-worker",
		"--label", "dev.swarm.agent_name_owner=swarm://worker/release-worker",
		"--label", "dev.swarm.agent_name_source=declared",
		"--label", "dev.swarm.agent_route_presence=present",
		"--label", "dev.swarm.container.kind=agent",
		"--label", "dev.swarm.container.name=" + releaseE2EFixtureAgent,
		"--label", "dev.swarm.creation_source=workspace.ResolveWorkspace",
		"--label", "dev.swarm.owner=runtime",
		"--label", "dev.swarm.reset.eligible=true",
		"--label", "dev.swarm.source_projection=" + releaseE2EProjectionID,
		"--label", "dev.swarm.workspace.scope=per-agent",
	}
	if runBound {
		digest := strings.Repeat("b", 64)
		args = append(args,
			"--label", "dev.swarm.data_projection_id=data-projection-v1:sha256:"+digest,
			"--label", "dev.swarm.run_id=11111111-1111-4111-8111-111111111111",
		)
	}
	args = append(args,
		"-v", sourceProjection+":/opt/swarm/source:ro",
	)
	if runBound {
		args = append(args, "-v", filepath.Join(os.TempDir(), "swarm", "data-projections", "a_"+strings.Repeat("b", 64))+":/data:ro")
	}
	return append(args,
		"-v", releaseE2EFixtureAgentVol+":"+releaseE2EAgentWorkdir,
		"-w", releaseE2EAgentWorkdir,
		releaseE2EWorkspaceImage, "sleep", "infinity",
	)
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
			Args:      []string{"claude", "--tools", strings.Join(releaseE2EBuiltinTools(), ","), "--allowedTools", strings.Join(releaseE2EStartupAllowedTools(), ",")},
			SessionID: "11111111-1111-1111-1111-111111111111",
			RawMCPURL: releaseE2ERawMCPURL,
			MCPURL:    releaseE2EHostMCPURL,
		},
		{Class: "container_remove"},
		{
			Class:     "claude_live",
			Args:      []string{"claude", "--tools", strings.Join(releaseE2EBuiltinTools(), ","), "--allowedTools", strings.Join(releaseE2ELiveAllowedTools(), ","), "--model", releaseE2EManagedModel},
			SessionID: "22222222-2222-2222-2222-222222222222",
			RawMCPURL: releaseE2ERawMCPURL,
			MCPURL:    releaseE2EHostMCPURL,
		},
		{
			Class:      "mcp_notify",
			ToolName:   "notify_human",
			ToolStatus: "queued",
			MailboxID:  "33333333-3333-3333-3333-333333333333",
			RawMCPURL:  releaseE2ERawMCPURL,
			MCPURL:     releaseE2EHostMCPURL,
		},
		{
			Class:     "mcp_emit",
			ToolName:  "emit_agent_completed",
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

func removeReleaseCreateMount(args []string, target string) []string {
	for index := 0; index+1 < len(args); index++ {
		if args[index] == "-v" && releaseMountTarget(args[index+1]) == target {
			return append(args[:index:index], args[index+2:]...)
		}
	}
	return args
}

func replaceReleaseCreateMount(args []string, target, value string) []string {
	for index := 0; index+1 < len(args); index++ {
		if args[index] == "-v" && releaseMountTarget(args[index+1]) == target {
			args[index+1] = value
			return args
		}
	}
	return args
}

func insertReleaseCreateMount(args []string, value string) []string {
	commandIndex := len(args) - 3
	out := append([]string(nil), args[:commandIndex]...)
	out = append(out, "-v", value)
	return append(out, args[commandIndex:]...)
}

func releaseMountTarget(value string) string {
	parts := strings.Split(value, ":")
	if len(parts) < 2 {
		return ""
	}
	return parts[1]
}
