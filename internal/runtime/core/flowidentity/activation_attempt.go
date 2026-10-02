package flowidentity

import (
	"fmt"
	"strconv"
)

// ParseActivationAttemptID validates the selected store's monotonically increasing
// per-instance ordinal. Generation and source grants remain separate authority.
func ParseActivationAttemptID(id string) (uint64, error) {
	value, err := strconv.ParseUint(id, 10, 63)
	if err != nil || value == 0 || strconv.FormatUint(value, 10) != id {
		return 0, fmt.Errorf("flow activation attempt requires a canonical positive ordinal")
	}
	return value, nil
}
