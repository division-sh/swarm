package paths

import (
	"fmt"
	"strings"
)

const MaxFlowSegmentBytes = 100

var windowsReservedBasenames = map[string]struct{}{
	"CON": {}, "PRN": {}, "AUX": {}, "NUL": {},
	"COM1": {}, "COM2": {}, "COM3": {}, "COM4": {}, "COM5": {}, "COM6": {}, "COM7": {}, "COM8": {}, "COM9": {},
	"LPT1": {}, "LPT2": {}, "LPT3": {}, "LPT4": {}, "LPT5": {}, "LPT6": {}, "LPT7": {}, "LPT8": {}, "LPT9": {},
}

// ValidateFlowSegment also governs provider names, without normalizing either.
func ValidateFlowSegment(segment string) error {
	if len(segment) == 0 || len(segment) > MaxFlowSegmentBytes {
		return fmt.Errorf("flow segment %q must be 1..%d bytes", segment, MaxFlowSegmentBytes)
	}
	if IsReservedPortableBasename(segment) {
		return fmt.Errorf("flow segment %q is a reserved portable basename", segment)
	}
	for index, char := range []byte(segment) {
		if char > 0x7f || !((char >= 'a' && char <= 'z') || (char >= '0' && char <= '9') || (index > 0 && (char == '_' || char == '-'))) {
			return fmt.Errorf("flow segment %q must match [a-z0-9][a-z0-9_-]*", segment)
		}
	}
	return nil
}

func IsReservedPortableBasename(segment string) bool {
	base, _, _ := strings.Cut(segment, ".")
	_, ok := windowsReservedBasenames[strings.ToUpper(base)]
	return ok
}
