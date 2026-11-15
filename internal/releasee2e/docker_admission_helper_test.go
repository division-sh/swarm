package releasee2e

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
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
	var probe fakeDockerContainer
	withFakeDockerState(root, func(state *fakeDockerState) {
		for _, value := range args[1:] {
			candidate := releaseDockerTargetName(state, value)
			if container, ok := state.Containers[candidate]; ok && container.Labels["sh.swarm.admission-probe"] != "" {
				name, probe = candidate, container
			}
		}
	})
	if name == "" {
		return false, 0
	}
	switch {
	case equalStrings(args, []string{"start", "--attach", name}) && probe.AdmissionKind == "version":
		withFakeDockerState(root, func(state *fakeDockerState) {
			probe.AdmissionExited = true
			state.Containers[name] = probe
		})
		recordFakeDocker(root, fakeDockerRecord{Class: "cli_preflight", Args: args})
		fmt.Fprintln(os.Stdout, "claude fixture version")
		return true, 0
	case equalStrings(args, []string{"start", name}) && probe.AdmissionKind == "startup":
		return true, fakeDockerSetRunning(root, args, true)
	case equalStrings(args, []string{"inspect", "--format", "{{json .State}}", name}) && probe.AdmissionKind == "version" && probe.AdmissionExited:
		fmt.Fprintln(os.Stdout, `{"Status":"exited","Running":false,"ExitCode":0,"OOMKilled":false}`)
		return true, 0
	case args[0] == "rm":
		if !equalStrings(args, []string{"rm", "--force", name}) && !equalStrings(args, []string{"rm", "--force", probe.ID}) {
			return true, fakeDockerUnexpected(root, args, "invalid disposable removal")
		}
		withFakeDockerState(root, func(state *fakeDockerState) { delete(state.Containers, name) })
		recordFakeDocker(root, fakeDockerRecord{Class: "admission_remove", Args: args, ContainerID: probe.ID})
		return true, 0
	case probe.Running && equalStrings(args, []string{"exec", "--user", "0", name, "node", "-e", `const fs=require('fs');fs.mkdirSync(process.argv[1],{recursive:true});fs.chownSync(process.argv[1],10001,10001);`, probe.Workdir}):
		return true, 0
	case args[0] == "exec" && probe.Running:
		return true, fakeAdmissionClaude(root, args, name, probe.Workdir)
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
	kind, workdir, worker := "version", "", ""
	if !equalStrings(args, version) {
		kind, workdir = "startup", dockerOptionValue(args, "--workdir")
		if workdir != releaseE2EAgentWorkdir && workdir != releaseE2ESystemWorkdir {
			return fakeDockerUnexpected(root, args, "wrong disposable workdir")
		}
		index := len(startup)
		if len(args) <= index+1 || args[index] != "--mount" || !strings.HasPrefix(args[index+1], "type=bind,source=") || !strings.HasSuffix(args[index+1], ",destination=/opt/swarm/source,readonly") {
			return fakeDockerUnexpected(root, args, "missing disposable source mount")
		}
		source := strings.TrimSuffix(strings.TrimPrefix(args[index+1], "type=bind,source="), ",destination=/opt/swarm/source,readonly")
		marker, err := os.ReadFile(filepath.Join(filepath.Dir(source), "identity.json"))
		var identity releaseSourceProjectionMarker
		if err != nil || json.Unmarshal(marker, &identity) != nil || validateReleaseSourceProjection(source, filepath.Join(filepath.Dir(root), "contracts"), identity.BundleHash, identity.Identity) != nil {
			return fakeDockerUnexpected(root, args, "invalid disposable fixture source projection")
		}
		startup = append(startup, "--mount", "type=bind,source="+source+",destination=/opt/swarm/source,readonly")
		if runtime.GOOS == "linux" {
			if len(args) < len(startup)+4 {
				return fakeDockerUnexpected(root, args, "missing disposable worker mount")
			}
			worker = strings.TrimSuffix(strings.TrimPrefix(dockerOptionValue(args[len(startup)+2:], "--mount"), "type=bind,source="), ",destination=/opt/swarm/bin/swarm,readonly")
			info, err := os.Stat(worker)
			if err != nil || !info.Mode().IsRegular() || info.Mode()&0o111 == 0 || filepath.Base(worker) != "swarm" {
				return fakeDockerUnexpected(root, args, "invalid disposable worker")
			}
			startup = append(startup, "--add-host", "host.docker.internal:host-gateway", "--mount", "type=bind,source="+worker+",destination=/opt/swarm/bin/swarm,readonly")
		}
		startup = append(startup, "--network", releaseE2ENetwork, "--workdir", workdir, "--entrypoint", "sleep", releaseAdmissionImage, "infinity")
		if !equalStrings(args, startup) {
			return fakeDockerUnexpected(root, args, "invalid disposable startup shape")
		}
	}
	if !validReleaseUUID(id) {
		return fakeDockerUnexpected(root, args, "invalid disposable identity")
	}
	object := strings.ReplaceAll(id, "-", "") + strings.ReplaceAll(id, "-", "")
	withFakeDockerState(root, func(state *fakeDockerState) {
		if state.ContainerIDs == nil {
			state.ContainerIDs = map[string]string{}
		}
		state.ContainerIDs[object] = name
		state.Containers[name] = fakeDockerContainer{ID: object, Labels: map[string]string{"sh.swarm.admission-probe": id}, WorkerPath: worker, AdmissionKind: kind, Workdir: workdir}
	})
	recordFakeDocker(root, fakeDockerRecord{Class: "admission_create", Args: args, ContainerID: object})
	fmt.Fprintln(os.Stdout, object)
	return 0
}

func fakeAdmissionClaude(root string, args []string, name, workdir string) int {
	input, err := io.ReadAll(os.Stdin)
	if err != nil || strings.TrimSpace(string(input)) != "Startup validation probe. Do not call any tools. Reply with the exact text ok." || dockerOptionValue(args, "--session-id") == "" {
		return fakeDockerUnexpected(root, args, "invalid disposable Claude probe")
	}
	envelope := []string{"exec", "-i", "-e", "CLAUDE_CONFIG_DIR=/opt/swarm/provider/claude", "-e", "CLAUDE_CODE_OAUTH_TOKEN=" + releaseE2EOAuthToken, "-w", workdir, name, "claude"}
	if len(args) < len(envelope) || !equalStrings(args[:len(envelope)], envelope) {
		return fakeDockerUnexpected(root, args, "invalid disposable credential/workdir/command envelope")
	}
	command := args[len(envelope):]
	if equalStrings(command, []string{"-p", "--session-id", dockerOptionValue(command, "--session-id"), "--output-format", "json", "--tools", "", "--allowedTools", "", "--model", releaseE2EManagedModel}) && validReleaseUUID(dockerOptionValue(command, "--session-id")) {
		recordFakeDocker(root, fakeDockerRecord{Class: "admission_answer", Args: redactDockerArgs(args)})
		fmt.Fprintln(os.Stdout, `{"type":"result","is_error":false,"result":"ok"}`)
		return 0
	}
	if err := validateReleaseClaudeArgs(command, true); err != nil {
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
