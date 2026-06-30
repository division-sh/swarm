package swarm

import _ "embed"

//go:embed platform-spec.yaml
var platformSpecYAML []byte

//go:embed Dockerfile.workspace
var workspaceDockerfile []byte

func EmbeddedPlatformSpecYAML() []byte {
	out := make([]byte, len(platformSpecYAML))
	copy(out, platformSpecYAML)
	return out
}

func EmbeddedWorkspaceDockerfile() []byte {
	out := make([]byte, len(workspaceDockerfile))
	copy(out, workspaceDockerfile)
	return out
}
