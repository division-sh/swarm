package canonicalrouting

import "testing"

// CopyCrossFlowNodeHandler keeps the producer-owned input and the receiver's
// local emit names together for handler localization/readback proofs.
func CopyCrossFlowNodeHandler(t testing.TB, withOnSuccess bool) string {
	t.Helper()
	root := t.TempDir()
	consumerSchema := "name: validation\nstages: {researching: {}, done: {final: true}}\ninputs: [vertical.shortlisted]\noutputs: [validation.started]\n"
	consumerEvents := "validation.started:\n  entity_id: string\n"
	consumerNodes := "validation-orchestrator:\n  vertical.shortlisted:\n    emit: validation.started\n"
	if withOnSuccess {
		consumerSchema = "name: validation\nstages: {researching: {}, done: {final: true}}\ninputs: [vertical.shortlisted]\noutputs: [validation.rule, validation.started]\n"
		consumerEvents += "validation.rule:\n  entity_id: string\n"
		consumerNodes = "validation-orchestrator:\n  vertical.shortlisted:\n    rules:\n      - emit: validation.rule\n    on_success:\n      emit: validation.started\n"
	}
	for name, body := range map[string]string{
		"schema.yaml":            "name: cross-flow-handler\nconnect:\n  - {event: vertical.shortlisted, from: scoring, to: validation}\n",
		"state.yaml":             "item_id: string\n",
		"scoring/schema.yaml":    "name: scoring\nstages: {discovered: {}, done: {final: true}}\noutputs: [vertical.shortlisted]\n",
		"scoring/events.yaml":    "score.ready:\nvertical.shortlisted:\n  entity_id: string\n",
		"scoring/nodes.yaml":     "scoring-node:\n  score.ready:\n    emit: vertical.shortlisted\n",
		"validation/schema.yaml": consumerSchema,
		"validation/events.yaml": consumerEvents,
		"validation/nodes.yaml":  consumerNodes,
	} {
		writeClosedVariantFile(t, root, name, body)
	}
	return root
}
