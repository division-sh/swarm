package canonicalrouting

import (
	"strings"
	"testing"
)

// CopyR2ValueSource exercises one expression field through nodes.yaml admission,
// without manufacturing a routing topology or an alternate decoder.
func CopyR2ValueSource(t testing.TB, value string) string {
	t.Helper()
	root := t.TempDir()
	writeClosedVariantFile(t, root, "nodes.yaml", "value-node:\n  execution_type: system_node\n  event_handlers:\n    value.requested:\n      emit:\n        event: value.completed\n        fields:\n          value:\n            "+strings.ReplaceAll(value, "\n", "\n            ")+"\n")
	return root
}
