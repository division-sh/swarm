package swarm

import (
	"embed"
	"io/fs"
)

//go:embed platform-spec.yaml
var platformSpecYAML []byte

//go:embed Dockerfile.workspace
var workspaceDockerfile []byte

//go:embed all:examples/integrations/telegram-agent
var telegramAgentExample embed.FS

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

func EmbeddedTelegramAgentExample() fs.FS {
	example, err := fs.Sub(telegramAgentExample, "examples/integrations/telegram-agent")
	if err != nil {
		panic(err)
	}
	return example
}
