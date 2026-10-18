package canonicalrouting

import "testing"

// WriteManagerExecutionProjection owns the source scopes observed by the
// manager's ordinary execution projection fixtures.
func WriteManagerExecutionProjection(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	for relative, source := range map[string]string{
		"schema.yaml":        "pins:\n  inputs: [test.old, test.new]\n",
		"events.yaml":        "test.old:\ntest.new:\n",
		"review/schema.yaml": "instance: review_id\npins:\n  inputs: [task.ready]\n",
		"review/entities.yaml": "review:\n" +
			"  review_id: {type: text, immutable: true}\n",
		"review/events.yaml": "task.ready:\n",
	} {
		writeClosedVariantFile(t, root, relative, source)
	}
	return root
}
