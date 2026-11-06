package fanoutobligation

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
)

// RunSettlement describes current obligations, not historical issuance totals.
type RunSettlement struct {
	RunID                          string
	Open, Blocked, Owed, Unsettled int
	BarrierArmed, BarrierPending   int
}

func (s RunSettlement) Validate() error {
	if _, err := uuid.Parse(strings.TrimSpace(s.RunID)); err != nil {
		return fmt.Errorf("fan-out settlement requires run identity: %w", err)
	}
	if s.Open < 0 || s.Blocked < 0 || s.Owed < 0 || s.Unsettled < 0 || s.BarrierArmed < 0 || s.BarrierPending < 0 || (s.Open+s.Blocked == 0 && s.Owed != 0) {
		return fmt.Errorf("fan-out settlement has contradictory current obligations")
	}
	return nil
}

func (s RunSettlement) BlocksCompletion() bool {
	return s.Open+s.Blocked+s.Owed+s.Unsettled+s.BarrierArmed+s.BarrierPending > 0
}
