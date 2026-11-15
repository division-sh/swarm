package releasee2e

import (
	"fmt"
	"io"
	"os"
	"strings"
)

const releaseAdmissionImage = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

// Handle only the disposable boot observer, separately from execution backing.
// The existing emulator continues to validate every ordinary workspace command.
func fakeAdmissionDocker(root string, args []string) (bool, int) {
	if len(args) == 7 && args[0] == "exec" && args[5] == "/opt/swarm/bin/swarm" {
		return false, 0 // The existing emulator executes the real mounted worker.
	}
	if equalStrings(args, []string{"image", "inspect", "--format", "{{.Id}}", releaseE2EWorkspaceImage}) {
		fmt.Fprintln(os.Stdout, releaseAdmissionImage)
		recordFakeDocker(root, fakeDockerRecord{Class: "image_inspect", Args: args})
		return true, 0
	}
	if equalStrings(args, []string{"image", "inspect", "--format", "{{json .Config.Volumes}}", releaseAdmissionImage}) {
		fmt.Fprintln(os.Stdout, "null")
		return true, 0
	}
	if args[0] == "create" && strings.HasPrefix(dockerOptionValue(args, "--name"), "swarm-admission-probe-") {
		return true, fakeAdmissionCreate(root, args)
	}
	if args[0] == "container" && len(args) == 10 && args[1] == "ls" && strings.HasPrefix(args[5], "name=^/swarm-admission-probe-") {
		name := strings.TrimSuffix(strings.TrimPrefix(args[5], "name=^/"), "$")
		id := strings.TrimPrefix(name, "swarm-admission-probe-")
		want := []string{"container", "ls", "--all", "--no-trunc", "--filter", "name=^/" + name + "$", "--filter", "label=sh.swarm.admission-probe=" + id, "--format", "{{json .ID}}"}
		if !equalStrings(args, want) || !validReleaseUUID(id) {
			return true, fakeDockerUnexpected(root, args, "invalid disposable inventory")
		}
		withFakeDockerState(root, func(state *fakeDockerState) {
			if container, ok := state.Containers[name]; ok {
				fmt.Fprintf(os.Stdout, "%q\n", container.ID)
			}
		})
		return true, 0
	}
	var name string
	withFakeDockerState(root, func(state *fakeDockerState) {
		for _, value := range args[1:] {
			candidate := releaseDockerTargetName(state, value)
			if container, ok := state.Containers[candidate]; ok && container.Labels["sh.swarm.admission-probe"] != "" {
				name = candidate
			}
		}
	})
	if name == "" {
		return false, 0
	}
	switch {
	case equalStrings(args, []string{"start", "--attach", name}):
		recordFakeDocker(root, fakeDockerRecord{Class: "cli_preflight", Args: args})
		fmt.Fprintln(os.Stdout, "claude fixture version")
		return true, 0
	case equalStrings(args, []string{"start", name}):
		return true, fakeDockerSetRunning(root, args, true)
	case equalStrings(args, []string{"inspect", "--format", "{{json .State}}", name}):
		fmt.Fprintln(os.Stdout, `{"Status":"exited","Running":false,"ExitCode":0,"OOMKilled":false}`)
		return true, 0
	case args[0] == "rm":
		if len(args) != 3 || args[1] != "--force" {
			return true, fakeDockerUnexpected(root, args, "invalid disposable removal")
		}
		withFakeDockerState(root, func(state *fakeDockerState) { delete(state.Containers, name) })
		recordFakeDocker(root, fakeDockerRecord{Class: "admission_remove", Args: args})
		return true, 0
	case len(args) == 8 && args[0] == "exec" && args[1] == "--user" && args[2] == "0" && args[3] == name && args[4] == "node" && args[5] == "-e":
		return true, 0
	case args[0] == "exec":
		return true, fakeAdmissionClaude(root, args, name)
	default:
		return true, fakeDockerUnexpected(root, args, "unsupported disposable command")
	}
}

func fakeAdmissionCreate(root string, args []string) int {
	name := dockerOptionValue(args, "--name")
	id := strings.TrimPrefix(name, "swarm-admission-probe-")
	prefix := []string{"create", "--name", name, "--label", "sh.swarm.admission-probe=" + id, "--pull=never"}
	version := append(append([]string{}, prefix...), "--read-only", "--network", "none", "--no-healthcheck", "--log-driver", "none", "--cap-drop", "ALL", "--security-opt", "no-new-privileges:true", "--memory", "256m", "--cpus", "1", "--pids-limit", "64", "--entrypoint", "claude", releaseAdmissionImage, "--version")
	startup := append(append([]string{}, prefix...), "--no-healthcheck", "--log-driver", "none", "--cap-drop", "ALL", "--cap-add", "CHOWN", "--security-opt", "no-new-privileges:true", "--tmpfs", "/opt/swarm/provider/claude:rw,mode=0700,uid=10001,gid=10001", "--tmpfs", "/tmp:rw,mode=1777")
	if !equalStrings(args, version) {
		// Source and worker paths are per-invocation immutable read-only mounts.
		index := len(startup)
		for index+1 < len(args) && (args[index] == "--mount" || args[index] == "--add-host") {
			if args[index] == "--mount" && (!strings.HasPrefix(args[index+1], "type=bind,source=") || !strings.HasSuffix(args[index+1], ",readonly")) || args[index] == "--add-host" && args[index+1] != "host.docker.internal:host-gateway" {
				return fakeDockerUnexpected(root, args, "invalid disposable read-only mount")
			}
			startup = append(startup, args[index:index+2]...)
			index += 2
		}
		startup = append(startup, "--network", releaseE2ENetwork, "--workdir", dockerOptionValue(args, "--workdir"), "--entrypoint", "sleep", releaseAdmissionImage, "infinity")
		if !equalStrings(args, startup) {
			return fakeDockerUnexpected(root, args, "invalid disposable startup shape")
		}
	}
	if !validReleaseUUID(id) {
		return fakeDockerUnexpected(root, args, "invalid disposable identity")
	}
	object := strings.ReplaceAll(id, "-", "") + strings.ReplaceAll(id, "-", "")
	var worker string
	for index := 0; index+1 < len(args); index++ {
		if args[index] == "--mount" && strings.HasSuffix(args[index+1], ",destination=/opt/swarm/bin/swarm,readonly") {
			worker = strings.TrimSuffix(strings.TrimPrefix(args[index+1], "type=bind,source="), ",destination=/opt/swarm/bin/swarm,readonly")
		}
	}
	withFakeDockerState(root, func(state *fakeDockerState) {
		if state.ContainerIDs == nil {
			state.ContainerIDs = map[string]string{}
		}
		state.ContainerIDs[object] = name
		state.Containers[name] = fakeDockerContainer{ID: object, Labels: map[string]string{"sh.swarm.admission-probe": id}, WorkerPath: worker}
	})
	recordFakeDocker(root, fakeDockerRecord{Class: "admission_create", Args: args, ContainerID: object})
	fmt.Fprintln(os.Stdout, object)
	return 0
}

func fakeAdmissionClaude(root string, args []string, name string) int {
	input, err := io.ReadAll(os.Stdin)
	if err != nil || strings.TrimSpace(string(input)) != "Startup validation probe. Do not call any tools. Reply with the exact text ok." || dockerOptionValue(args, "--session-id") == "" {
		return fakeDockerUnexpected(root, args, "invalid disposable Claude probe")
	}
	if dockerOptionValue(args, "--output-format") == "json" && dockerOptionValue(args, "--tools") == "" && dockerOptionValue(args, "--allowedTools") == "" {
		recordFakeDocker(root, fakeDockerRecord{Class: "admission_answer", Args: redactDockerArgs(args)})
		fmt.Fprintln(os.Stdout, `{"type":"result","is_error":false,"result":"ok"}`)
		return 0
	}
	index := 1
	for index < len(args) && args[index] != name {
		index++
	}
	if index+2 >= len(args) || args[index+1] != "claude" {
		return fakeDockerUnexpected(root, args, "missing disposable Claude command")
	}
	if err := validateReleaseClaudeArgs(args[index+2:], true); err != nil {
		return fakeDockerUnexpected(root, args, err.Error())
	}
	rawURL, hostURL, _, err := validateReleaseMCPConfig(dockerOptionValue(args, "--mcp-config"), true)
	if err != nil {
		return fakeDockerUnexpected(root, args, err.Error())
	}
	recordFakeDocker(root, fakeDockerRecord{Class: "claude_startup", Args: redactDockerArgs(args), SessionID: dockerOptionValue(args, "--session-id"), RawMCPURL: rawURL, MCPURL: hostURL, StartupObservation: true})
	writeClaudeInit(dockerOptionValue(args, "--session-id"), splitAllowedTools(dockerOptionValue(args, "--allowedTools")))
	return 0
}
