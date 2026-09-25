package testplanning

import (
	"bytes"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/division-sh/swarm/internal/testchanged"
)

type ChangedPath struct {
	Status  string
	OldPath string
	Path    string
}

// ParseNameStatusZ retains status and both paths for copy/rename entries.
func ParseNameStatusZ(raw []byte) ([]ChangedPath, error) {
	if len(raw) == 0 || raw[len(raw)-1] != 0 {
		return nil, fmt.Errorf("PR change record is empty or not NUL terminated")
	}
	fields := bytes.Split(raw[:len(raw)-1], []byte{0})
	var changes []ChangedPath
	for i := 0; i < len(fields); {
		status := string(fields[i])
		i++
		if status == "" || i >= len(fields) {
			return nil, fmt.Errorf("incomplete PR change status %q", status)
		}
		change := ChangedPath{Status: status, Path: string(fields[i])}
		i++
		if strings.HasPrefix(status, "R") || strings.HasPrefix(status, "C") {
			if i >= len(fields) {
				return nil, fmt.Errorf("PR %s has no destination", status)
			}
			change.OldPath, change.Path = change.Path, string(fields[i])
			i++
		}
		if change.Path == "" || filepath.IsAbs(change.Path) || strings.HasPrefix(change.Path, "../") || change.OldPath != "" && (filepath.IsAbs(change.OldPath) || strings.HasPrefix(change.OldPath, "../")) {
			return nil, fmt.Errorf("invalid PR changed path %+v", change)
		}
		changes = append(changes, change)
	}
	if len(changes) == 0 {
		return nil, fmt.Errorf("PR change record has no paths")
	}
	return changes, nil
}

func PRChangeOptions(baseRoot, headRoot string, base, head RootInventory, changes []ChangedPath) (BuildOptions, error) {
	if len(changes) == 0 {
		return BuildOptions{}, fmt.Errorf("PR changed paths are empty")
	}
	options := BuildOptions{}
	var baseFiles, headFiles []testchanged.ChangedFile
	for _, change := range changes {
		if change.Status != "M" || !pureProsePath(change.Path) {
			options.IncludeParityFull = true
		}
		if change.Status != "M" {
			options.IncludeSoak = true
		}
		if pureProsePath(change.Path) && change.Status == "M" {
			continue
		}
		path := filepath.ToSlash(change.Path)
		if !strings.HasSuffix(path, ".go") || sharedSoakInput(path) {
			options.IncludeSoak = true
			continue
		}
		baseFiles = append(baseFiles, testchanged.ChangedFile{Path: path, Status: change.Status})
		headFiles = append(headFiles, testchanged.ChangedFile{Path: path, Status: change.Status})
	}
	if !options.IncludeSoak && len(baseFiles) != 0 {
		for _, snapshot := range []struct {
			root      string
			inventory RootInventory
			files     []testchanged.ChangedFile
		}{{baseRoot, base, baseFiles}, {headRoot, head, headFiles}} {
			impact, err := testchanged.PlanChanged(snapshot.root, snapshot.inventory.ImpactPackages, snapshot.files)
			if err != nil {
				options.IncludeSoak = true
				break
			}
			if impact.FullSuite {
				options.IncludeSoak = true
				break
			}
			for _, pkg := range impact.Packages {
				if pkg.ImportPath == SoakPackage {
					options.IncludeSoak = true
					break
				}
			}
		}
	}
	return options, nil
}

func pureProsePath(path string) bool {
	return path == "README.md" || path == "CONTRIBUTING.md"
}

func sharedSoakInput(path string) bool {
	for _, prefix := range []string{
		".github/", "cmd/swarm-test/", "cmd/swarm-test-timing/", "internal/testplanning/", "internal/testtiming/", "internal/testchanged/", "internal/testpostgres/", "internal/testutil/", "internal/runtime/testfixtures/",
	} {
		if strings.HasPrefix(path, prefix) {
			return true
		}
	}
	return false
}
