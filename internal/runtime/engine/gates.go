package engine

import (
	"strconv"
	"strings"
)

func boolMapToAnyMap(in map[string]bool) map[string]any {
	if len(in) == 0 {
		return map[string]any{}
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		out[key] = value
	}
	return out
}

func anyMapToBoolMap(in map[string]any) map[string]bool {
	if len(in) == 0 {
		return map[string]bool{}
	}
	out := make(map[string]bool, len(in))
	for key, raw := range in {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if value, ok := gateBool(raw); ok {
			out[key] = value
		}
	}
	return out
}

func gateBool(raw any) (bool, bool) {
	switch t := raw.(type) {
	case bool:
		return t, true
	case string:
		parsed, err := strconv.ParseBool(strings.TrimSpace(t))
		return parsed, err == nil
	default:
		return false, false
	}
}

func mapsClone(in map[string]bool) map[string]bool {
	if len(in) == 0 {
		return map[string]bool{}
	}
	out := make(map[string]bool, len(in))
	for key, value := range in {
		key = strings.TrimSpace(key)
		if key != "" {
			out[key] = value
		}
	}
	return out
}

func ensureMetadataGates(metadata map[string]any) map[string]bool {
	if metadata == nil {
		metadata = map[string]any{}
	}
	current, _ := metadata["gates"].(map[string]any)
	gates := anyMapToBoolMap(current)
	metadata["gates"] = boolMapToAnyMap(gates)
	return gates
}

func normalizeSnapshotGates(snapshot *StateSnapshot) {
	if snapshot == nil {
		return
	}
	if snapshot.Metadata == nil {
		snapshot.Metadata = map[string]any{}
	}
	metadataGates := map[string]bool{}
	if raw, ok := snapshot.Metadata["gates"].(map[string]any); ok {
		metadataGates = anyMapToBoolMap(raw)
	}
	if len(snapshot.Gates) == 0 {
		snapshot.Gates = metadataGates
	} else {
		for key, value := range metadataGates {
			if _, ok := snapshot.Gates[key]; !ok {
				snapshot.Gates[key] = value
			}
		}
	}
	snapshot.Metadata["gates"] = boolMapToAnyMap(snapshot.Gates)
}

func normalizeMutationGates(mutation *StateMutation) {
	if mutation == nil {
		return
	}
	if mutation.Metadata == nil {
		mutation.Metadata = map[string]any{}
	}
	metadataGates := map[string]bool{}
	if raw, ok := mutation.Metadata["gates"].(map[string]any); ok {
		metadataGates = anyMapToBoolMap(raw)
	}
	if len(mutation.Gates) == 0 {
		mutation.Gates = metadataGates
	} else {
		for key, value := range metadataGates {
			if _, ok := mutation.Gates[key]; !ok {
				mutation.Gates[key] = value
			}
		}
	}
	mutation.Metadata["gates"] = boolMapToAnyMap(mutation.Gates)
}
