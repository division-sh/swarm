package canonicalrouting

import "testing"

// CopyClockDeployment exercises real no-argument root-tree construction without
// a public input, provider binding or authored standing activation.
func CopyClockDeployment(t testing.TB, nested bool) string {
	t.Helper()
	root := t.TempDir()
	if !nested {
		writeClosedVariantFile(t, root, "schema.yaml", "name: clock-export\nstages: []\nschedules:\n  poll: {every: 250ms, emit: poll.tick}\npins:\n  outputs: [poll.tick]\n")
		writeClosedVariantFile(t, root, "events.yaml", "poll.tick:\n")
		return root
	}
	for path, body := range map[string]string{
		"schema.yaml":          "name: clock-connected\nstages: []\nconnect:\n  - {event: poll.tick, from: clock, to: consumer}\n",
		"clock/schema.yaml":    "stages: []\nschedules:\n  poll: {every: 250ms, emit: poll.tick}\npins:\n  outputs: [poll.tick]\n",
		"clock/events.yaml":    "poll.tick:\n",
		"consumer/schema.yaml": "stages: []\npins:\n  inputs: [poll.tick]\n",
		"consumer/nodes.yaml":  "observer:\n  execution_type: system_node\n  subscribes_to: [poll.tick]\n  event_handlers:\n    poll.tick:\n      guard: {id: admit, check: true}\n",
	} {
		writeClosedVariantFile(t, root, path, body)
	}
	return root
}

// CopyClockFiniteDeployment keeps the declared clock independent of ordinary
// finite-run completion and the private scenario that exercises it.
func CopyClockFiniteDeployment(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	for path, body := range map[string]string{
		"schema.yaml":       "name: clock-public-boundary\nstages:\n  pending: {initial: true}\n  done: {terminal: true}\nschedules:\n  poll: {every: 250ms, emit: poll.tick}\npins:\n  inputs: [start.requested]\n  outputs: [poll.tick]\n",
		"events.yaml":       "start.requested:\npoll.tick:\n",
		"entities.yaml":     "test_entity: {}\n",
		"nodes.yaml":        "complete:\n  execution_type: system_node\n  subscribes_to: [start.requested]\n  event_handlers:\n    start.requested:\n      advances_to: done\n",
		"tests/finite.yaml": "name: finite-clock-boundary\nsteps:\n  - publish: start.requested\n    payload: {}\nexpect:\n  events:\n    exact: [start.requested]\n  no_dead_letters: true\n",
	} {
		writeClosedVariantFile(t, root, path, body)
	}
	return root
}

// CopyParentConnectClock replaces the canonical reactive producer with a bare
// clock producer. The disconnected variant keeps an independent same-name
// receiver declaration, which is not subscription authority.
func CopyParentConnectClock(t testing.TB, connected bool) string {
	t.Helper()
	root := CopyExample(t, ParentConnect)
	removeInheritedScenarios(t, root)
	removeClosedVariantFiles(t, root, "events.yaml", "producer/nodes.yaml")
	rootSchema := "stages: []\n"
	if connected {
		rootSchema += "connect:\n  - {event: work.ready, from: producer, to: consumer}\n"
	} else {
		writeClosedVariantFile(t, root, "consumer/events.yaml", "work.ready:\n")
	}
	writeClosedVariantFile(t, root, "schema.yaml", rootSchema)
	writeClosedVariantFile(t, root, "producer/schema.yaml", "stages: []\nschedules:\n  poll: {every: 5m, emit: work.ready}\npins:\n  outputs: [work.ready]\n")
	writeClosedVariantFile(t, root, "producer/events.yaml", "work.ready:\n")
	writeClosedVariantFile(t, root, "consumer/schema.yaml", "stages: []\npins:\n  inputs: [work.ready]\n")
	return root
}
