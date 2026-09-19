package canonicalrouting

import (
	"path/filepath"
	"testing"
)

// CopyServedFanOutReporter preserves the numeric reporter's emitted schema and
// adds the root ingress used by run.start and the supported serving surfaces.
func CopyServedFanOutReporter(t testing.TB) string {
	t.Helper()
	root := CopyNotifyAllChildren(t, NotifyAllChildrenOptions{
		NumericRegistrationRows: true, NumericReporterSink: true, RegistrationUUIDField: true,
	})
	copyTree(t, filepath.Join(RepoRoot(t), "internal/runtime/testfixtures/canonicalrouting/testdata/fan-out-execution/served-reporter"), root)
	return root
}

// CopyFanOutMixedAgentRoute adds real agent receivers to the mixed-cardinality
// source before admission; it never patches a compiled route or agent registry.
func CopyFanOutMixedAgentRoute(t testing.TB) string {
	t.Helper()
	root := CopyFanOutMixedRoute(t)
	copyTree(t, filepath.Join(RepoRoot(t), "internal/runtime/testfixtures/canonicalrouting/testdata/fan-out-execution/mixed-agent"), root)
	return root
}
