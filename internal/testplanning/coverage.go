package testplanning

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// ValidateGoProofPartition checks the finite repository census, not regex
// subtyping. Top-level selection preserves every selected proof's subtests.
func ValidateGoProofPartition(dir string, runs []string) error {
	if len(runs) == 0 {
		return fmt.Errorf("proof package %s has no execution units", dir)
	}
	patterns := make([]*regexp.Regexp, len(runs))
	for i, run := range runs {
		if strings.Contains(run, "/") {
			return fmt.Errorf("proof partition %d has partial-subtest filter %q", i, run)
		}
		if run == "" && len(runs) != 1 {
			return fmt.Errorf("unfiltered proof owner cannot coexist with partitions")
		}
		pattern, err := regexp.Compile(run)
		if err != nil {
			return fmt.Errorf("proof partition %d: %w", i, err)
		}
		patterns[i] = pattern
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	matched := make([]bool, len(runs))
	proofs := 0
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(dir, entry.Name()), nil, 0)
		if err != nil {
			return err
		}
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Recv != nil || function.Name == nil {
				continue
			}
			name := function.Name.Name
			if name == "TestMain" || (!strings.HasPrefix(name, "Test") && !strings.HasPrefix(name, "Example") && !strings.HasPrefix(name, "Fuzz")) {
				continue
			}
			proofs++
			owners := 0
			for i, pattern := range patterns {
				if pattern.MatchString(name) {
					owners++
					matched[i] = true
				}
			}
			if owners != 1 {
				return fmt.Errorf("proof %s matches %d execution units, want exactly one", name, owners)
			}
		}
	}
	if proofs == 0 {
		return fmt.Errorf("proof package %s has an empty census", dir)
	}
	for i, live := range matched {
		if !live {
			return fmt.Errorf("proof partition %d matches no proof", i)
		}
	}
	return nil
}
