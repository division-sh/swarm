package manager

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/google/uuid"
)

func MustJSON(v any) []byte {
	raw, err := json.Marshal(v)
	if err != nil || len(raw) == 0 {
		return []byte("{}")
	}
	return raw
}

func mustJSON(v any) []byte {
	return MustJSON(v)
}

func DeterministicOutputEventID(inbound events.Event, identity runtimeagentidentity.Identity, index int, out events.Event) (string, error) {
	fingerprint, err := identity.Fingerprint()
	if err != nil {
		return "", fmt.Errorf("deterministic output event identity is invalid: %w", err)
	}
	seed := strings.Join([]string{
		strings.TrimSpace(inbound.ID()),
		fingerprint,
		fmt.Sprintf("%d", index),
		strings.TrimSpace(string(out.Type())),
		strings.TrimSpace(out.EntityID()),
	}, "|")
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(seed)).String(), nil
}

func nativeToolConfigFromMap(values map[string]bool) runtimeactors.NativeToolConfig {
	return runtimeactors.NativeToolConfig{
		Bash:      values["bash"],
		WebSearch: values["web_search"],
		FileIO:    values["file_io"],
	}
}
