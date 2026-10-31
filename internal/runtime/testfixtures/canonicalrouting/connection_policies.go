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
		"manifest.yaml":        "version: 1.0.0\nplatform_version: '*'\n",
		"worker/manifest.yaml": "version: 1.0.0\nplatform_version: '*'\n",
		"schema.yaml":          "# fixture: edge-policy\npins:\n  inputs: [work.requested]\n  outputs: [work.ready]\nconnect:\n" + edges,
		"events.yaml":          "work.requested:\n  creation_id: text\n  reuse_id: text\nwork.ready:\n  creation_id: text\n  reuse_id: text\n",
		"nodes.yaml":           "relay:\n  execution_type: system_node\n  event_handlers:\n    work.requested:\n      emit:\n        event: work.ready\n        fields:\n          creation_id: payload.creation_id\n          reuse_id: payload.reuse_id\n",
		"worker/schema.yaml":   "# fixture: worker\ninstance: worker_id\npins:\n  inputs:\n    - work.ready\n",
		"worker/state.yaml":    "worker_id: text\n",
		"worker/nodes.yaml":    "worker:\n  execution_type: system_node\n  event_handlers:\n    work.ready:\n      guard: {check: payload.creation_id != ''}\n",
	} {
		writeClosedVariantFile(t, root, path, source)
	}
	return root
}

func CopyIntegerConnectionPolicies(t testing.TB) string {
	t.Helper()
	root := CopyConnectionPolicies(t, false)
	for range 2 {
		applyClosedReplacement(t, filepath.Join(root, "events.yaml"), "creation_id: text", "creation_id: integer")
		applyClosedReplacement(t, filepath.Join(root, "events.yaml"), "reuse_id: text", "reuse_id: integer")
	}
	applyClosedReplacement(t, filepath.Join(root, "worker/state.yaml"), "worker_id: text", "worker_id: integer")
	applyClosedReplacement(t, filepath.Join(root, "worker/nodes.yaml"), "payload.creation_id != ''", "payload.creation_id > 0")
	return root
}

func CopyInstanceDeclarations(t testing.TB, flows ...string) string {
	t.Helper()
	root := t.TempDir()
	writeClosedVariantFile(t, root, "manifest.yaml", "version: 1.0.0\nplatform_version: '*'\n")
	writeClosedVariantFile(t, root, "schema.yaml", "stages: []\n# fixture: instance-declarations\n")
	for _, flow := range flows {
		writeClosedVariantFile(t, root, flow+"/manifest.yaml", "version: 1.0.0\nplatform_version: '*'\n")
		writeClosedVariantFile(t, root, flow+"/manifest.yaml", "version: 1.0.0\nplatform_version: '*'\n")
		writeClosedVariantFile(t, root, flow+"/manifest.yaml", "version: 1.0.0\nplatform_version: '*'\n")
		writeClosedVariantFile(t, root, flow+"/manifest.yaml", "version: 1.0.0\nplatform_version: '*'\n")
		writeClosedVariantFile(t, root, flow+"/manifest.yaml", "version: 1.0.0\nplatform_version: '*'\n")
		writeClosedVariantFile(t, root, flow+"/manifest.yaml", "version: 1.0.0\nplatform_version: '*'\n")
		writeClosedVariantFile(t, root, flow+"/manifest.yaml", "version: 1.0.0\nplatform_version: '*'\n")
		writeClosedVariantFile(t, root, flow+"/manifest.yaml", "version: 1.0.0\nplatform_version: '*'\n")
		writeClosedVariantFile(t, root, flow+"/manifest.yaml", "version: 1.0.0\nplatform_version: '*'\n")
		writeClosedVariantFile(t, root, flow+"/manifest.yaml", "version: 1.0.0\nplatform_version: '*'\n")
		writeClosedVariantFile(t, root, flow+"/manifest.yaml", "version: 1.0.0\nplatform_version: '*'\n")
		writeClosedVariantFile(t, root, flow+"/manifest.yaml", "version: 1.0.0\nplatform_version: '*'\n")
		writeClosedVariantFile(t, root, flow+"/schema.yaml", "instance: instance_key\n")
		writeClosedVariantFile(t, root, flow+"/state.yaml", "instance_key: text\n")
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
	applyClosedReplacement(t, filepath.Join(root, "worker/state.yaml"), "worker_id: text", "worker_id: uuid")
	return root
}

func CopyNonCreatingInitialization(t testing.TB, connected bool) string {
	t.Helper()
	root := t.TempDir()
	invalid, err := ReceiverInitializeParserSnippet(t).SourceBytes()
	if err != nil {
		t.Fatal(err)
	}
	worker := "# fixture: worker\npins:\n  inputs:\n" + "    " + strings.ReplaceAll(strings.TrimSuffix(string(invalid), "\n"), "\n", "\n    ") + "\n"
	files := map[string]string{
		"manifest.yaml":        "version: 1.0.0\nplatform_version: '*'\n",
		"worker/manifest.yaml": "version: 1.0.0\nplatform_version: '*'\n",
		"schema.yaml":          "# fixture: init-test\npins:\n  inputs:\n    - work.ready\n  outputs:\n    - work.ready\nconnect:\n  - {event: work.ready, from: ., to: worker}\n",
		"events.yaml":          "work.ready:\n  label: text\n",
		"worker/schema.yaml":   worker,
		"worker/nodes.yaml":    "worker:\n  execution_type: system_node\n  event_handlers:\n    work.ready:\n      guard: {check: payload.label != ''}\n",
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
