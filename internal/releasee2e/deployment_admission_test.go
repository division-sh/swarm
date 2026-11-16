package releasee2e

import (
	"strings"
	"testing"
)

func assertReleaseDryRunAdmission(t *testing.T, result releaseCommandResult) {
	t.Helper()
	if result.err != nil || !strings.Contains(result.output, "admission checks passed; startup execution not performed") ||
		strings.Contains(result.output, "admission incomplete") || !strings.Contains(result.output, "startup not performed: startup_process_acquisition @") {
		t.Fatalf("release dry run lost complete checks or independent startup acquisition: %v\n%s", result.err, result.output)
	}
}
