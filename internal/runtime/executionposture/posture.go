package executionposture

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/executionmode"
)

// Posture is the immutable process ceiling for runtime execution authority.
type Posture string

const (
	Live     Posture = "live"
	MockOnly Posture = "mock_only"
)

func (p Posture) Valid() bool {
	return p == Live || p == MockOnly
}

func Parse(raw string) (Posture, bool) {
	posture := Posture(strings.TrimSpace(raw))
	if !posture.Valid() {
		return "", false
	}
	return posture, true
}

func (p Posture) RootMode() executionmode.Mode {
	if p == MockOnly {
		return executionmode.Mock
	}
	if p == Live {
		return executionmode.Live
	}
	return ""
}

func (p Posture) Admit(mode executionmode.Mode, operation string) error {
	if !p.Valid() {
		return fmt.Errorf("runtime execution posture is invalid: %q", p)
	}
	if !mode.Valid() {
		return fmt.Errorf("%s has invalid execution mode %q", strings.TrimSpace(operation), mode)
	}
	if p == MockOnly && mode == executionmode.Live {
		return fmt.Errorf("runtime.execution_posture=mock_only rejects live execution before %s", strings.TrimSpace(operation))
	}
	return nil
}
