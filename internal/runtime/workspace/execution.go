package workspace

import (
	"errors"
	"fmt"
	"path"
	"path/filepath"
	"strings"
)

const (
	LogicalWorkspaceMount = "/workspace"
	LogicalDataMount      = "/data"
	LogicalContractsMount = "/opt/swarm/contracts"
)

type ExecutionMode string

const (
	ExecutionModeUnsupported     ExecutionMode = "unsupported"
	ExecutionModeDockerContainer ExecutionMode = "docker_container"
	ExecutionModeHostLocal       ExecutionMode = "host_local"
)

type ExecutionCapability string

const (
	ExecutionCapabilityNativeCommand   ExecutionCapability = "native_command"
	ExecutionCapabilityFileRead        ExecutionCapability = "file_read"
	ExecutionCapabilityFileWrite       ExecutionCapability = "file_write"
	ExecutionCapabilityToolResultRelay ExecutionCapability = "tool_result_relay"
	ExecutionCapabilityClaudeCLI       ExecutionCapability = "claude_cli"
)

type MountAccess string

const (
	MountAccessReadOnly  MountAccess = "read_only"
	MountAccessReadWrite MountAccess = "read_write"
)

type ExecutionMount struct {
	LogicalPath string
	HostPath    string
	Access      MountAccess
}

type ExecutionTarget struct {
	Backend   string
	Mode      ExecutionMode
	Container string
	Workdir   string
	Mounts    []ExecutionMount

	capabilities map[ExecutionCapability]struct{}
}

type PathAccess string

const (
	PathAccessRead  PathAccess = "read"
	PathAccessWrite PathAccess = "write"
)

type ResolvedPath struct {
	LogicalPath string
	HostPath    string
	Access      PathAccess
}

func (t *Target) ExecutionTarget() ExecutionTarget {
	if t == nil {
		return unsupportedExecutionTarget("")
	}
	backend := strings.ToLower(strings.TrimSpace(t.Backend))
	container := strings.TrimSpace(t.Container)
	switch backend {
	case BackendDocker:
		if container == "" {
			return unsupportedExecutionTarget(BackendDocker)
		}
		return dockerExecutionTarget(container, strings.TrimSpace(t.Workdir), t.Mounts)
	case BackendHost:
		return hostExecutionTarget(strings.TrimSpace(t.Workdir), t.Mounts)
	case "":
		if container != "" {
			return dockerExecutionTarget(container, strings.TrimSpace(t.Workdir), t.Mounts)
		}
		return unsupportedExecutionTarget("")
	default:
		return unsupportedExecutionTarget(backend)
	}
}

func unsupportedExecutionTarget(backend string) ExecutionTarget {
	return ExecutionTarget{
		Backend: strings.TrimSpace(backend),
		Mode:    ExecutionModeUnsupported,
		Workdir: LogicalWorkspaceMount,
		Mounts:  defaultLogicalExecutionMounts(),
	}
}

func dockerExecutionTarget(container, workdir string, mounts []ExecutionMount) ExecutionTarget {
	if workdir == "" {
		workdir = LogicalWorkspaceMount
	}
	return ExecutionTarget{
		Backend:   BackendDocker,
		Mode:      ExecutionModeDockerContainer,
		Container: container,
		Workdir:   cleanLogicalPath(workdir),
		Mounts:    executionMountsOrDefault(mounts),
		capabilities: capabilitySet(
			ExecutionCapabilityNativeCommand,
			ExecutionCapabilityFileRead,
			ExecutionCapabilityFileWrite,
			ExecutionCapabilityToolResultRelay,
			ExecutionCapabilityClaudeCLI,
		),
	}
}

func hostExecutionTarget(workdir string, mounts []ExecutionMount) ExecutionTarget {
	return ExecutionTarget{
		Backend: BackendHost,
		Mode:    ExecutionModeHostLocal,
		Workdir: LogicalWorkspaceMount,
		Mounts:  executionMountsOrDefault(mounts),
		capabilities: capabilitySet(
			ExecutionCapabilityFileRead,
			ExecutionCapabilityFileWrite,
			ExecutionCapabilityToolResultRelay,
		),
	}
}

func capabilitySet(caps ...ExecutionCapability) map[ExecutionCapability]struct{} {
	out := make(map[ExecutionCapability]struct{}, len(caps))
	for _, cap := range caps {
		if cap == "" {
			continue
		}
		out[cap] = struct{}{}
	}
	return out
}

func executionMountsOrDefault(mounts []ExecutionMount) []ExecutionMount {
	if len(mounts) == 0 {
		return defaultLogicalExecutionMounts()
	}
	out := make([]ExecutionMount, 0, len(mounts))
	for _, mount := range mounts {
		logical := cleanLogicalPath(mount.LogicalPath)
		if logical == "." || logical == "" {
			continue
		}
		access := mount.Access
		if access == "" {
			access = MountAccessReadOnly
		}
		out = append(out, ExecutionMount{
			LogicalPath: logical,
			HostPath:    strings.TrimSpace(mount.HostPath),
			Access:      access,
		})
	}
	if len(out) == 0 {
		return defaultLogicalExecutionMounts()
	}
	return out
}

func defaultLogicalExecutionMounts() []ExecutionMount {
	return []ExecutionMount{
		{LogicalPath: LogicalWorkspaceMount, Access: MountAccessReadWrite},
		{LogicalPath: LogicalDataMount, Access: MountAccessReadOnly},
		{LogicalPath: LogicalContractsMount, Access: MountAccessReadOnly},
	}
}

func (e ExecutionTarget) Supports(capability ExecutionCapability) bool {
	if e.capabilities == nil {
		return false
	}
	_, ok := e.capabilities[capability]
	return ok
}

func (e ExecutionTarget) Require(capability ExecutionCapability) error {
	if e.Supports(capability) {
		return nil
	}
	return errors.New(e.UnsupportedMessage(capability))
}

func (e ExecutionTarget) UnsupportedMessage(capability ExecutionCapability) string {
	if strings.EqualFold(strings.TrimSpace(e.Backend), BackendHost) {
		switch capability {
		case ExecutionCapabilityClaudeCLI:
			return "host workspace backend does not support Claude CLI execution yet"
		default:
			return "host workspace backend does not support native tool execution yet"
		}
	}
	switch capability {
	case ExecutionCapabilityClaudeCLI:
		return "claude sessions must run in a container workspace"
	case ExecutionCapabilityToolResultRelay:
		return "workspace target does not support tool result relay"
	default:
		return "workspace target does not support native tool execution"
	}
}

func (e ExecutionTarget) ResolvePath(raw string, access PathAccess) (string, error) {
	resolved, _, err := e.resolveLogicalPath(raw, access)
	if err != nil {
		return "", err
	}
	return resolved, nil
}

func (e ExecutionTarget) ResolveExecutionPath(raw string, access PathAccess) (ResolvedPath, error) {
	clean, mount, err := e.resolveLogicalPath(raw, access)
	if err != nil {
		return ResolvedPath{}, err
	}
	resolved := ResolvedPath{LogicalPath: clean, Access: access}
	if e.Mode == ExecutionModeHostLocal {
		hostPath, err := resolveHostBackingPath(clean, mount)
		if err != nil {
			return ResolvedPath{}, err
		}
		resolved.HostPath = hostPath
	}
	return resolved, nil
}

func (e ExecutionTarget) resolveLogicalPath(raw string, access PathAccess) (string, ExecutionMount, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", ExecutionMount{}, fmt.Errorf("path is required")
	}
	clean := cleanLogicalPath(raw)
	if !strings.HasPrefix(clean, "/") {
		clean = path.Join(LogicalWorkspaceMount, clean)
	}
	clean = cleanLogicalPath(clean)
	for _, mount := range e.Mounts {
		if !logicalPathWithinRoot(clean, mount.LogicalPath) {
			continue
		}
		if access == PathAccessWrite && mount.Access != MountAccessReadWrite {
			return "", ExecutionMount{}, fmt.Errorf("write_file path %s is outside the writable workspace", clean)
		}
		return clean, mount, nil
	}
	if access == PathAccessWrite {
		return "", ExecutionMount{}, fmt.Errorf("write_file path is outside the writable workspace")
	}
	return "", ExecutionMount{}, fmt.Errorf("read_file path is outside the allowed workspace mounts")
}

func (e ExecutionTarget) ResolveHostPath(raw string, access PathAccess) (ResolvedPath, error) {
	resolved, err := e.ResolveExecutionPath(raw, access)
	if err != nil {
		return ResolvedPath{}, err
	}
	if e.Mode != ExecutionModeHostLocal {
		return ResolvedPath{}, fmt.Errorf("workspace target is not a host-local execution target")
	}
	if strings.TrimSpace(resolved.HostPath) == "" {
		return ResolvedPath{}, fmt.Errorf("host backing path for %s is unavailable", resolved.LogicalPath)
	}
	return resolved, nil
}

func (e ExecutionTarget) WorkspacePath(rel string) (string, error) {
	rel = strings.TrimSpace(rel)
	if rel == "" {
		return "", fmt.Errorf("workspace relative path is required")
	}
	clean := cleanLogicalPath(rel)
	if strings.HasPrefix(clean, "/") {
		if !logicalPathWithinRoot(clean, LogicalWorkspaceMount) {
			return "", fmt.Errorf("workspace path %s is outside %s", rel, LogicalWorkspaceMount)
		}
		return clean, nil
	}
	return path.Join(LogicalWorkspaceMount, clean), nil
}

func cleanLogicalPath(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	return path.Clean(strings.ReplaceAll(raw, "\\", "/"))
}

func logicalPathWithinRoot(value, root string) bool {
	value = cleanLogicalPath(value)
	root = cleanLogicalPath(root)
	if value == "" || root == "" || !strings.HasPrefix(value, "/") || !strings.HasPrefix(root, "/") {
		return false
	}
	return value == root || strings.HasPrefix(value, strings.TrimRight(root, "/")+"/")
}

func resolveHostBackingPath(logicalPath string, mount ExecutionMount) (string, error) {
	rootRaw := strings.TrimSpace(mount.HostPath)
	if rootRaw == "" {
		return "", fmt.Errorf("host backing path for %s is unavailable", cleanLogicalPath(mount.LogicalPath))
	}
	root, err := canonicalPathForOverlap(rootRaw, "host backing path")
	if err != nil {
		return "", fmt.Errorf("host backing path for %s is unavailable", cleanLogicalPath(mount.LogicalPath))
	}
	rel := logicalPathRelativeToRoot(logicalPath, mount.LogicalPath)
	hostPath := root
	if rel != "" {
		hostPath = filepath.Join(root, filepath.FromSlash(rel))
	}
	resolved, err := canonicalPathForOverlap(hostPath, "host execution path")
	if err != nil {
		return "", fmt.Errorf("host execution path %s is unavailable", cleanLogicalPath(logicalPath))
	}
	if !pathWithinRoot(resolved, root) {
		return "", fmt.Errorf("host execution path %s escapes %s", cleanLogicalPath(logicalPath), cleanLogicalPath(mount.LogicalPath))
	}
	return resolved, nil
}

func logicalPathRelativeToRoot(value, root string) string {
	value = cleanLogicalPath(value)
	root = cleanLogicalPath(root)
	if value == root {
		return ""
	}
	return strings.TrimPrefix(value, strings.TrimRight(root, "/")+"/")
}
