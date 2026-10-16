package canonicalrouting

import "testing"

// FiniteClosureOffender selects a known constructor-closure counterexample.
type FiniteClosureOffender uint8

const (
	FiniteClosureEnded FiniteClosureOffender = iota
	FiniteClosureWorkerService
	FiniteClosureChildService
)

type DeploymentFeedRoot uint8

const (
	DeploymentFeedConnectedRoot DeploymentFeedRoot = iota
	DeploymentFeedSameFlowRoot
	DeploymentFeedKeyedRoot
	DeploymentFeedOutputOnlyRoot
	DeploymentFeedPrivateRoot
)

func CopyRootInputFeed(t testing.TB, kind DeploymentFeedRoot) string {
	t.Helper()
	files := map[string]string{
		"schema.yaml":            "stages: {done: {final: true}}\npins:\n  inputs: [item.registered]\nconnect:\n  - {event: item.registered, from: ., to: registry, resolution: create}\n",
		"events.yaml":            "item.registered:\n  key: item_id\n  item_id: text\n",
		"registry/schema.yaml":   "instance: item_id\nstages: {pending: {}, done: {final: true}}\npins:\n  inputs: [item.registered]\n",
		"registry/entities.yaml": "Item:\n  item_id: text\n",
		"registry/nodes.yaml":    "register:\n  event_handlers:\n    item.registered: {advances_to: done}\n",
	}
	switch kind {
	case DeploymentFeedConnectedRoot:
	case DeploymentFeedSameFlowRoot, DeploymentFeedKeyedRoot:
		delete(files, "registry/schema.yaml")
		delete(files, "registry/entities.yaml")
		delete(files, "registry/nodes.yaml")
		files["schema.yaml"] = "stages: {pending: {}, done: {final: true}}\npins:\n  inputs: [item.registered]\n"
		files["entities.yaml"] = "Item: {}\n"
		files["nodes.yaml"] = "register:\n  event_handlers:\n    item.registered: {advances_to: done}\n"
		if kind == DeploymentFeedKeyedRoot {
			files["schema.yaml"] = "instance: account_id\nstages: {pending: {}, done: {final: true}}\npins:\n  inputs: [account.opened, item.registered]\n"
			files["events.yaml"] += "account.opened:\n  account_id: text\n"
			files["entities.yaml"] = "Account:\n  account_id: text\n"
			files["nodes.yaml"] += "open:\n  event_handlers:\n    account.opened: {}\n"
		}
	case DeploymentFeedOutputOnlyRoot:
		files["schema.yaml"] = "stages: {done: {final: true}}\npins:\n  outputs: [item.registered]\nconnect:\n  - {event: item.registered, from: ., to: registry, resolution: create}\n"
	case DeploymentFeedPrivateRoot:
		files["schema.yaml"] = "stages: {done: {final: true}}\n"
	default:
		t.Fatalf("unknown deployment feed root %d", kind)
	}
	return copyFiniteSource(t, files)
}

func CopyFiniteInitiation(t testing.TB, finite bool) string {
	t.Helper()
	stages, target := "stages: {waiting: {}}\n", "waiting"
	if finite {
		stages, target = "stages: {waiting: {}, done: {final: true}}\n", "done"
	}
	return copyFiniteSource(t, map[string]string{
		"schema.yaml":           "name: initiation\n" + stages + "pins:\n  inputs: [scan.requested]\n  outputs: [scan.requested]\nconnect:\n  - {event: scan.requested, from: ., to: discovery}\n",
		"events.yaml":           "scan.requested:\n  topic: text\n",
		"nodes.yaml":            "scanner:\n  execution_type: system_node\n  event_handlers:\n    scan.requested:\n      advances_to: " + target + "\n",
		"discovery/schema.yaml": "name: discovery\n" + stages + "pins:\n  inputs: [scan.requested]\n",
		"discovery/nodes.yaml":  "scanner:\n  execution_type: system_node\n  event_handlers:\n    scan.requested:\n      advances_to: " + target + "\n",
	})
}

func CopyDeploymentRunStart(t testing.TB) string {
	t.Helper()
	// Only data-created templates receive work; an empty feed constructs the
	// ended root, not an eager worker that would strand completion.
	return copyFiniteSource(t, map[string]string{
		"schema.yaml":             "name: deployment\nstages: {done: {final: true}}\npins:\n  inputs: [scan.requested, score.observed]\nconnect:\n  - {event: scan.requested, from: ., to: discovery, resolution: create}\n  - {event: score.observed, from: ., to: scores, resolution: create}\n",
		"events.yaml":             "scan.requested:\n  topic: text\nscore.observed:\n  key: label\n  label: text\nportfolio.opened:\n  topic: text\n",
		"discovery/schema.yaml":   "instance: topic\nstages: {ready: {}, done: {final: true}}\npins:\n  inputs: [scan.requested]\n",
		"discovery/entities.yaml": "Scan:\n  topic: text\n",
		"discovery/nodes.yaml":    "scanner:\n  execution_type: system_node\n  event_handlers:\n    scan.requested:\n      advances_to: done\n",
		"scores/schema.yaml":      "instance: label\nstages: {ready: {}, done: {final: true}}\npins:\n  inputs: [score.observed]\n",
		"scores/entities.yaml":    "Score:\n  label: text\n",
		"scores/nodes.yaml":       "scorer:\n  event_handlers:\n    score.observed: {advances_to: done}\n",
	})
}

func CopyPublicCompletionInitiation(t testing.TB) string {
	t.Helper()
	root := CopyPublicCompletionFeedOnly(t)
	writeClosedVariantFile(t, root, "schema.yaml", "name: deployment\nstages: {ready: {}, done: {final: true}}\npins:\n  inputs: [scan.requested, score.observed]\n  outputs: [scan.requested, score.observed]\nconnect:\n  - {event: scan.requested, from: ., to: discovery, resolution: create}\n  - {event: score.observed, from: ., to: scores, resolution: create}\n")
	writeClosedVariantFile(t, root, "nodes.yaml", "scanner:\n  execution_type: system_node\n  event_handlers:\n    scan.requested:\n      advances_to: done\n")
	return root
}

func CopyPublicCompletionFeedOnly(t testing.TB) string {
	t.Helper()
	root := CopyDeploymentRunStart(t)
	// portfolio.opened belongs to the component's unpinned-resource rejection
	// control; the served completion journey carries only active declarations.
	writeClosedVariantFile(t, root, "events.yaml", "scan.requested:\n  topic: text\nscore.observed:\n  key: label\n  label: text\n")
	return root
}

func CopyPublicCompletionFeedOnlyActivation(t testing.TB) string {
	t.Helper()
	root := CopyPublicCompletionInitiation(t)
	writeClosedVariantFile(t, root, "entities.yaml", "Deployment:\n  topic: {type: 'text?', _unused_reason: public completion attachment proof}\n")
	return root
}

func CopyPublicCompletionReplacement(t testing.TB) string {
	t.Helper()
	root := CopyPublicCompletionFeedOnly(t)
	writeClosedVariantFile(t, root, "schema.yaml", "name: replacement\nstages: {done: {final: true}}\npins:\n  inputs: [scan.requested, score.observed]\n  outputs: [scan.requested, score.observed]\nconnect:\n  - {event: scan.requested, from: ., to: discovery, resolution: create}\n  - {event: score.observed, from: ., to: scores, resolution: create}\n")
	return root
}

func CopyFiniteEagerClosure(t testing.TB, childFinal bool) string {
	t.Helper()
	child := "stages: {waiting: {}}\n"
	if childFinal {
		child = "stages: {waiting: {}, done: {final: true}}\n"
	}
	return copyFiniteSource(t, map[string]string{
		"schema.yaml":          "stages: {waiting: {}, done: {final: true}}\n",
		"unrouted/schema.yaml": child,
	})
}

func CopyFiniteStatelessContainer(t testing.TB) string {
	t.Helper()
	return copyFiniteSource(t, map[string]string{
		"schema.yaml":      "name: container\n",
		"leaf/schema.yaml": "stages: {done: {final: true}}\n",
	})
}

func CopyFiniteDormantTemplate(t testing.TB) string {
	t.Helper()
	return copyFiniteSource(t, map[string]string{
		"schema.yaml":           "stages: {done: {final: true}}\n",
		"dormant/schema.yaml":   "instance: item_id\nstages: {waiting: {}}\n",
		"dormant/entities.yaml": "Item:\n  item_id: {type: text, _unused_reason: dormant constructor}\n",
	})
}

func CopyFiniteConnectedClosure(t testing.TB, offender FiniteClosureOffender) string {
	t.Helper()
	worker, child := finiteClosureStages(t, offender)
	return copyFiniteSource(t, map[string]string{
		"schema.yaml":                "stages: {active: {}, done: {final: true}}\npins:\n  inputs: [work.requested]\n  outputs: [work.requested]\nconnect:\n  - {event: work.requested, from: ., to: worker, resolution: create}\n",
		"events.yaml":                "work.requested:\n  worker_id: text\n",
		"worker/schema.yaml":         "instance: worker_id\n" + worker + "pins:\n  inputs: [work.requested]\n",
		"worker/entities.yaml":       "Worker:\n  worker_id: {type: text, _unused_reason: constructor identity}\n",
		"worker/nodes.yaml":          "worker:\n  execution_type: system_node\n  event_handlers:\n    work.requested: {}\n",
		"worker/support/schema.yaml": child,
	})
}

func CopyFiniteConstructorControl(t testing.TB) string {
	t.Helper()
	return copyFiniteSource(t, map[string]string{
		"schema.yaml":       "stages: {done: {final: true}}\n",
		"child/schema.yaml": "stages: {done: {final: true}}\n",
	})
}

func CopyFiniteNestedServiceFeed(t testing.TB) string {
	t.Helper()
	return copyFiniteSource(t, map[string]string{
		"schema.yaml":            "stages: {done: {final: true}}\nconnect:\n  - {event: work.requested, from: producer, to: worker, resolution: create}\n",
		"producer/schema.yaml":   "instance: producer_id\nstages: {done: {final: true}}\npins:\n  outputs: [work.requested]\n",
		"producer/entities.yaml": "Producer:\n  producer_id: {type: text, _unused_reason: dormant identity}\n",
		"producer/events.yaml":   "work.requested:\n  worker_id: text\n",
		"worker/schema.yaml":     "instance: worker_id\nstages: {active: {}}\npins:\n  inputs: [work.requested]\n",
		"worker/entities.yaml":   "Worker:\n  worker_id: {type: text, _unused_reason: constructor identity}\n",
		"worker/nodes.yaml":      "worker:\n  execution_type: system_node\n  event_handlers:\n    work.requested: {}\n",
	})
}

func CopyFiniteSelectedFeedClosure(t testing.TB, offender FiniteClosureOffender) string {
	t.Helper()
	worker, leaf := finiteClosureStages(t, offender)
	if offender != FiniteClosureWorkerService {
		worker = "stages: {done: {final: true}}\n"
	}
	files := finiteSelectedFeedFiles("stages: {done: {final: true}}\n", "stages: {active: {}}\n", worker)
	files["worker/support/schema.yaml"] = "stages: {done: {final: true}}\n"
	files["worker/support/leaf/schema.yaml"] = leaf
	return copyFiniteSource(t, files)
}

func CopyFiniteAPIServiceFeed(t testing.TB) string {
	t.Helper()
	files := finiteSelectedFeedFiles("stages: {ready: {}, done: {final: true}}\npins:\n  inputs: [start.requested]\n", "stages: {done: {final: true}}\n", "stages: {active: {}}\n")
	files["events.yaml"] = "start.requested:\n"
	files["nodes.yaml"] = "starter:\n  execution_type: system_node\n  event_handlers:\n    start.requested: {advances_to: done}\n"
	return copyFiniteSource(t, files)
}

func finiteClosureStages(t testing.TB, offender FiniteClosureOffender) (worker, child string) {
	t.Helper()
	worker, child = "stages: {active: {}, done: {final: true}}\n", "stages: {done: {final: true}}\n"
	switch offender {
	case FiniteClosureEnded:
	case FiniteClosureWorkerService:
		worker = "stages: {active: {}}\n"
	case FiniteClosureChildService:
		child = "stages: {active: {}}\n"
	default:
		t.Fatalf("unknown finite closure offender %d", offender)
	}
	return worker, child
}

func finiteSelectedFeedFiles(rootStages, producerStages, workerStages string) map[string]string {
	return map[string]string{
		"schema.yaml":            rootStages + "connect:\n  - {event: work.requested, from: producer, to: worker, resolution: create}\n  - {event: work.safe, from: producer, to: safe, resolution: create}\n",
		"producer/schema.yaml":   "instance: producer_id\n" + producerStages + "pins:\n  outputs: [work.requested, work.safe]\n",
		"producer/entities.yaml": "Producer:\n  producer_id: {type: text, _unused_reason: dormant identity}\n",
		"producer/events.yaml":   "work.requested:\n  worker_id: text\nwork.safe:\n  worker_id: text\n",
		"worker/schema.yaml":     "instance: worker_id\n" + workerStages + "pins:\n  inputs: [work.requested]\n",
		"worker/entities.yaml":   "Worker:\n  worker_id: {type: text, _unused_reason: constructor identity}\n",
		"worker/nodes.yaml":      "worker:\n  execution_type: system_node\n  event_handlers:\n    work.requested: {}\n",
		"safe/schema.yaml":       "instance: worker_id\nstages: {done: {final: true}}\npins:\n  inputs: [work.safe]\n",
		"safe/entities.yaml":     "Worker:\n  worker_id: {type: text, _unused_reason: constructor identity}\n",
		"safe/nodes.yaml":        "worker:\n  execution_type: system_node\n  event_handlers:\n    work.safe: {}\n",
	}
}

func copyFiniteSource(t testing.TB, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	for path, body := range files {
		writeClosedVariantFile(t, root, path, body)
	}
	return root
}
