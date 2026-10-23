package testplanning

import (
	"bytes"
	"fmt"
	"path"
	"strconv"
	"strings"

	"github.com/division-sh/swarm/internal/testchanged"
)

// Consume git's NUL-framed name-status stream, including both rename/copy sides.
func PreflightDebtRequired(nameStatus []byte) (bool, error) {
	files, err := preflightChangedFiles(nameStatus)
	if err != nil {
		return false, err
	}
	for _, file := range files {
		if preflightDebtPath(file.Path) {
			return true, nil
		}
	}
	return false, nil
}

func preflightChangedFiles(nameStatus []byte) ([]testchanged.ChangedFile, error) {
	if len(nameStatus) == 0 {
		return nil, nil
	}
	if nameStatus[len(nameStatus)-1] != 0 {
		return nil, fmt.Errorf("preflight diff is not a complete NUL-framed stream")
	}
	fields := bytes.Split(nameStatus[:len(nameStatus)-1], []byte{0})
	var files []testchanged.ChangedFile
	for i := 0; i < len(fields); {
		status := string(fields[i])
		i++
		count := 1
		switch status {
		case "A", "M", "D", "T":
		default:
			if len(status) < 2 || len(status) > 4 || (status[0] != 'R' && status[0] != 'C') || strings.Trim(status[1:], "0123456789") != "" {
				return nil, fmt.Errorf("unknown or unresolved preflight diff status %q", status)
			}
			score, err := strconv.Atoi(status[1:])
			if err != nil || score < 0 || score > 100 {
				return nil, fmt.Errorf("invalid preflight rename/copy score %q", status)
			}
			count = 2
		}
		if i+count > len(fields) {
			return nil, fmt.Errorf("preflight diff lacks old/new path evidence")
		}
		for _, raw := range fields[i : i+count] {
			name := string(raw)
			if name == "." || name == "" || path.IsAbs(name) || path.Clean(name) != name || strings.HasPrefix(name, "../") {
				return nil, fmt.Errorf("invalid preflight diff path %q", name)
			}
			files = append(files, testchanged.ChangedFile{Path: name, Status: status})
		}
		i += count
	}
	return files, nil
}

// AffectedFastRoots projects canonical unit obligations, never a local tier or
// a full-suite fallback. Source-reading consumers declare inputs in that policy.
func AffectedFastRoots(repo string, policy Policy, native RunPlan, inventory RootInventory, diff []byte) ([]TestRoot, error) {
	files, err := preflightChangedFiles(diff)
	if err != nil {
		return nil, err
	}
	impact, err := testchanged.PlanChanged(repo, inventory.ImpactPackages, files)
	if err != nil {
		return nil, err
	}
	packages := map[string]bool{}
	for _, pkg := range impact.Packages {
		packages[pkg.ImportPath] = true
	}
	var roots []TestRoot
	for _, unit := range native.Units {
		definition := policy.Units[unit.ID]
		if !definition.PreflightFast {
			continue
		}
		affected := impact.FullSuite
		for _, pkg := range unit.Packages {
			affected = affected || packages[pkg]
		}
		for _, file := range files {
			for _, input := range definition.InputPaths {
				affected = affected || file.Path == input || strings.HasPrefix(file.Path, input+"/")
			}
		}
		if affected {
			for _, required := range unit.RequiredTests {
				roots = append(roots, required.TestRoot)
			}
		}
	}
	return roots, nil
}

func preflightDebtPath(name string) bool {
	if strings.HasSuffix(name, ".go") || strings.HasPrefix(name, "internal/store/") || strings.HasPrefix(name, "internal/storetest/") {
		return true
	}
	switch path.Base(name) {
	case "go.mod", "go.sum", "go.work", "go.work.sum":
		return true
	}
	// Module/workspace inputs and the actual plan, semantic and authority-policy inputs.
	switch name {
	case "platform-spec.yaml", ".github/test-proof-plan.yaml":
		return true
	}
	return false
}
