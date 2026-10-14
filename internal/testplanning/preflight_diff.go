package testplanning

import (
	"bytes"
	"fmt"
	"path"
	"strconv"
	"strings"
)

// Consume git's NUL-framed name-status stream, including both rename/copy sides.
func PreflightDebtRequired(nameStatus []byte) (bool, error) {
	if len(nameStatus) == 0 {
		return false, nil
	}
	if nameStatus[len(nameStatus)-1] != 0 {
		return false, fmt.Errorf("preflight diff is not a complete NUL-framed stream")
	}
	fields := bytes.Split(nameStatus[:len(nameStatus)-1], []byte{0})
	required := false
	for i := 0; i < len(fields); {
		status := string(fields[i])
		i++
		count := 1
		switch status {
		case "A", "M", "D", "T":
		default:
			if len(status) < 2 || (status[0] != 'R' && status[0] != 'C') {
				return false, fmt.Errorf("unknown or unresolved preflight diff status %q", status)
			}
			score, err := strconv.Atoi(status[1:])
			if err != nil || score < 0 || score > 100 {
				return false, fmt.Errorf("invalid preflight rename/copy score %q", status)
			}
			count = 2
		}
		if i+count > len(fields) {
			return false, fmt.Errorf("preflight diff lacks old/new path evidence")
		}
		for _, raw := range fields[i : i+count] {
			name := string(raw)
			if name == "." || name == "" || path.IsAbs(name) || path.Clean(name) != name || strings.HasPrefix(name, "../") {
				return false, fmt.Errorf("invalid preflight diff path %q", name)
			}
			required = required || preflightDebtPath(name)
		}
		i += count
	}
	return required, nil
}

func preflightDebtPath(name string) bool {
	if strings.HasSuffix(name, ".go") || strings.HasPrefix(name, "internal/store/") || strings.HasPrefix(name, "internal/storetest/") {
		return true
	}
	// Module/workspace inputs and the actual plan, semantic and authority-policy inputs.
	switch name {
	case "go.mod", "go.sum", "go.work", "go.work.sum", "platform-spec.yaml", ".github/test-proof-plan.yaml":
		return true
	}
	return false
}
