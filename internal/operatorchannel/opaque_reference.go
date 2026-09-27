package operatorchannel

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
)

// OpaqueReference projects one admitted provider reference into the stable
// provider-neutral value used by facts and delivery receipts.
func OpaqueReference(value any) (string, bool, error) {
	switch value := value.(type) {
	case string:
		if strings.TrimSpace(value) == "" {
			return "", false, nil
		}
		return value, true, nil
	case map[string]any:
		if len(value) == 0 {
			return "", false, fmt.Errorf("opaque reference object is empty")
		}
		encoded, err := canonicaljson.Bytes(value)
		if err != nil {
			return "", false, err
		}
		return string(encoded), true, nil
	default:
		return "", false, nil
	}
}
