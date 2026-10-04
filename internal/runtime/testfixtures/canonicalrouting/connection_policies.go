package canonicalrouting

import (
	"path/filepath"
	"strings"
	"testing"
)

// CopyConnectionPolicies uses two edges to one input and one producer schema.
// Different source keys select distinct instances, not competing schema owners.
func CopyConnectionPolicies(t testing.TB, reverse bool) string {
	t.Helper()
	root := t.TempDir()
	create := "  - {event: work.ready, from: ., to: worker, resolution: create, key_from: payload.creation_id}\n"
	reuse := "  - {event: work.ready, from: ., to: worker, resolution: select-or-create, key_from: payload.reuse_id}\n"
	edges := create + reuse
	if reverse {
		edges = reuse + create
	}
	for path, source := range map[string]string{
		"schema.yaml":          "name: edge-policy\npins:\n  inputs: [work.requested]\n  outputs: [work.ready]\nconnect:\n" + edges,
		"events.yaml":          "work.requested:\n  creation_id: text\n  reuse_id: text\nwork.ready:\n  creation_id: text\n  reuse_id: text\n",
		"entities.yaml":        "run: {}\n",
		"nodes.yaml":           "relay:\n  execution_type: system_node\n  event_handlers:\n    work.requested:\n      create_entity: true\n      emit:\n        event: work.ready\n        fields:\n          creation_id: ${payload.creation_id}\n          reuse_id: ${payload.reuse_id}\n",
		"worker/schema.yaml":   "name: worker\ninstance: worker_id\npins:\n  inputs:\n    - work.ready\n",
		"worker/entities.yaml": "work:\n  worker_id: {type: text, _unused_reason: receiver identity}\n",
		"worker/nodes.yaml":    "worker:\n  execution_type: system_node\n  event_handlers:\n    work.ready:\n      guard: {check: \"payload.creation_id != ''\"}\n",
	} {
		writeClosedVariantFile(t, root, path, source)
	}
	return root
}

func CopyInstanceDeclarations(t testing.TB, flows ...string) string {
	t.Helper()
	root := t.TempDir()
	writeClosedVariantFile(t, root, "schema.yaml", "name: instance-declarations\n")
	for _, flow := range flows {
		writeClosedVariantFile(t, root, flow+"/schema.yaml", "instance: instance_key\n")
		writeClosedVariantFile(t, root, flow+"/entities.yaml", "work:\n  instance_key: {type: text, _unused_reason: fixture instance identity}\n")
	}
	return root
}

func CopyMixedConnectionProjections(t testing.TB, reverse bool, intrinsic string) string {
	t.Helper()
	if intrinsic != "generated.uuid" && intrinsic != "event.id" {
		t.Fatalf("unsupported intrinsic source %q", intrinsic)
	}
	root := CopyConnectionPolicies(t, reverse)
	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"), "key_from: payload.creation_id", "key_from: "+intrinsic)
	for range 2 {
		applyClosedReplacement(t, filepath.Join(root, "events.yaml"), "creation_id: text", "creation_id: uuid")
		applyClosedReplacement(t, filepath.Join(root, "events.yaml"), "reuse_id: text", "reuse_id: uuid")
	}
	applyClosedReplacement(t, filepath.Join(root, "worker/entities.yaml"), "type: text", "type: uuid")
	return root
}

func CopyNonCreatingInitialization(t testing.TB, connected bool) string {
	t.Helper()
	root := t.TempDir()
	files := map[string]string{
		"schema.yaml":        "name: init-test\npins:\n  inputs:\n    - work.ready\n  outputs:\n    - work.ready\nconnect:\n  - {event: work.ready, from: ., to: worker}\n",
		"events.yaml":        "work.ready:\n  label: text\n",
		"worker/schema.yaml": "name: worker\ninstance_variables:\n  variables:\n    label: text\npins:\n  inputs:\n    - event: work.ready\n      initialize:\n        label: payload.label\n",
		"worker/nodes.yaml":  "worker:\n  execution_type: system_node\n  event_handlers:\n    work.ready:\n      guard: {check: \"payload.label != ''\"}\n",
	}
	if !connected {
		files["schema.yaml"] = strings.Split(files["schema.yaml"], "connect:\n")[0]
		files["worker/events.yaml"] = "work.ready:\n  label: text\n"
	}
	for path, source := range files {
		writeClosedVariantFile(t, root, path, source)
	}
	return root
}
