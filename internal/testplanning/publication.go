package testplanning

import (
	"fmt"
	"sort"
	"strings"
)

const (
	GeneratedWeightModelPath = ".github/test-timing-weights.json"
	GeneratedModelBranch     = "automation/test-timing-model"
)

func ValidatePublicationDiff(paths []string) error {
	var normalized []string
	seen := map[string]bool{}
	for _, path := range paths {
		path = strings.TrimSpace(path)
		if path == "" || seen[path] {
			continue
		}
		seen[path] = true
		normalized = append(normalized, path)
	}
	sort.Strings(normalized)
	if len(normalized) != 1 || normalized[0] != GeneratedWeightModelPath {
		return fmt.Errorf("generated publication diff must contain only %s; got %v", GeneratedWeightModelPath, normalized)
	}
	return nil
}
