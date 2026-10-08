package timeridentity

import (
	"fmt"
	"strings"
	"time"
)

// TurnTimeout is an authored bound on one logical provider turn. Admission and
// queue timestamps do not start its clock; the effects owner records launch.
type TurnTimeout struct {
	After time.Duration `json:"after_ns"`
	Emit  string        `json:"emit"`
}

func (t TurnTimeout) Validate() error {
	if t.After <= 0 || t.Emit == "" || strings.TrimSpace(t.Emit) != t.Emit {
		return fmt.Errorf("turn_timeout requires positive after duration and non-empty canonical emit")
	}
	return nil
}

func CloneTurnTimeout(value *TurnTimeout) *TurnTimeout {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}
