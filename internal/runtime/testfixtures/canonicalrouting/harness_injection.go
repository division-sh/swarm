package canonicalrouting

import (
	"path/filepath"
	"testing"
)

// CopyHarnessInjectionWithoutSource creates the one closed negative mutation
// used to prove that the authored harness declaration is load-bearing.
func CopyHarnessInjectionWithoutSource(t testing.TB) string {
	t.Helper()
	root := CopyExample(t, HarnessInjection)
	applyClosedReplacement(
		t,
		filepath.Join(root, "flows", "worker", "schema.yaml"),
		"        source: harness\n",
		"",
	)
	return root
}

// CopyHarnessInjectionWithUnknownEvent creates the closed negative mutation
// used to prove that harness evidence cannot legitimize an undeclared event.
func CopyHarnessInjectionWithUnknownEvent(t testing.TB) string {
	t.Helper()
	root := CopyExample(t, HarnessInjection)
	applyClosedReplacement(
		t,
		filepath.Join(root, "flows", "worker", "schema.yaml"),
		"        event: work.requested\n",
		"        event: work.unknown\n",
	)
	return root
}

// CopyHarnessInjectionWithDuplicatePin creates the closed negative mutation
// used to prove that harness evidence cannot resolve ambiguous input pins.
func CopyHarnessInjectionWithDuplicatePin(t testing.TB) string {
	t.Helper()
	root := CopyExample(t, HarnessInjection)
	applyClosedReplacement(
		t,
		filepath.Join(root, "flows", "worker", "schema.yaml"),
		"        source: harness\n",
		"        source: harness\n      - name: work_requested_duplicate\n        event: work.requested\n        source: harness\n",
	)
	return root
}
