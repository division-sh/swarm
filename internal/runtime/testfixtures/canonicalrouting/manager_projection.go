package canonicalrouting

import "testing"

// WriteManagerExecutionProjection owns the fixed root/review event catalog
// observed by ordinary manager fixtures, including execution projections.
func WriteManagerExecutionProjection(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	const eventDeclarations = "review.ready:\ntask.ready:\ntest.in:\ntest.intervention:\ntest.intervention.settlement:\ntest.lane.child:\ntest.lane.root:\ntest.new:\ntest.old:\ntest.source_set_cancelled:\ntest.source_set_wait:\ntest.transition:\n"
	const inputs = "pins:\n  inputs: [review.ready, task.ready, test.in, test.intervention, test.intervention.settlement, test.lane.child, test.lane.root, test.new, test.old, test.source_set_cancelled, test.source_set_wait, test.transition]\n"
	for relative, source := range map[string]string{
		"schema.yaml":        inputs,
		"events.yaml":        eventDeclarations,
		"review/schema.yaml": "instance: review_id\n" + inputs,
		"review/entities.yaml": "review:\n" +
			"  review_id: {type: text, immutable: true}\n",
		"review/events.yaml": eventDeclarations,
	} {
		writeClosedVariantFile(t, root, relative, source)
	}
	return root
}
