package eventidentity

import (
	"fmt"
	"strings"

	runtimepaths "github.com/division-sh/swarm/internal/runtime/core/paths"
)

// QualifyProviderEvent projects a short declaration or reference, never a route
// or authority. The caller supplies its captured provider directory key.
func QualifyProviderEvent(key, local string) (string, error) {
	if err := runtimepaths.ValidateFlowSegment(key); err != nil {
		return "", fmt.Errorf("provider event requires an exact portable directory key: %w", err)
	}
	qualified := "inbound." + key + "." + local
	if strings.HasPrefix(local, "inbound.") || strings.Contains(local, "/") || !IsCanonicalName(qualified) {
		return "", fmt.Errorf("event %q must use a short local name such as text_message; the provider directory supplies inbound.%s.", local, key)
	}
	for _, token := range strings.Split(local, ".") {
		if token != strings.TrimSpace(token) {
			return "", fmt.Errorf("short event names must retain exact tokens without surrounding whitespace")
		}
	}
	return qualified, nil
}
