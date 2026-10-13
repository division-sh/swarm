package testtiming

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/testplanning"
)

func ValidatePreflightReport(pkg testplanning.PreflightPackage, report Report) error {
	if report.Summary.MalformedLines+report.Summary.DuplicatePackageEvents+report.Summary.DuplicateTestEvents != 0 {
		return fmt.Errorf("malformed or duplicate preflight evidence")
	}
	if len(report.Packages) != 1 || report.Packages[0].Package != pkg.Package || report.Packages[0].Result != "pass" {
		return fmt.Errorf("preflight package %s did not complete successfully", pkg.Package)
	}
	required := map[string]bool{}
	selected := map[string]bool{}
	for _, root := range pkg.Required {
		required[root.Name] = true
		selected[root.Name] = true
		for _, child := range root.Children {
			required[root.Name+"/"+child] = true
		}
	}
	for _, result := range report.Tests {
		root, _, _ := strings.Cut(result.Test, "/")
		if result.Package != pkg.Package || !selected[root] || result.Result != "pass" {
			return fmt.Errorf("preflight proof %s/%s = %s (foreign, skipped or failed)", result.Package, result.Test, result.Result)
		}
		delete(required, result.Test)
	}
	if len(required) != 0 {
		return fmt.Errorf("preflight package %s lacks %d required root/child results", pkg.Package, len(required))
	}
	return nil
}
