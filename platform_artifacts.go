package swarm

import _ "embed"

//go:embed platform-spec.yaml
var platformSpecYAML []byte

func EmbeddedPlatformSpecYAML() []byte {
	out := make([]byte, len(platformSpecYAML))
	copy(out, platformSpecYAML)
	return out
}
