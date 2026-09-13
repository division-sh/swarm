package pinrouting

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// DescriptorAddressFields projects the same scalar selection evidence from
// committed state and an exact prepared state. Structured fields are not keys.
func DescriptorAddressFields(values map[string]any) (map[string]string, error) {
	out := make(map[string]string, len(values))
	for key, value := range values {
		key = strings.TrimSpace(key)
		if key == "" {
			return nil, fmt.Errorf("entity field name is empty")
		}
		var scalar string
		switch typed := value.(type) {
		case string:
			scalar = strings.TrimSpace(typed)
		case bool:
			scalar = strconv.FormatBool(typed)
		case float64:
			scalar = strconv.FormatFloat(typed, 'g', -1, 64)
		case json.Number:
			scalar = typed.String()
		default:
			continue
		}
		out["entity."+key] = scalar
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}
