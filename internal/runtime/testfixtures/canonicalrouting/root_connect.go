package canonicalrouting

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

type RootConnectEmit uint8

const (
	RootConnectNoEmitter RootConnectEmit = iota
	RootConnectCanonicalEmit
)

// WritePublicTemplateInputRoute owns the API publication fixture with one root
// input and one private template input. Callers load the returned contract tree.
func WritePublicTemplateInputRoute(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	files := map[string]string{
		"manifest.yaml": `version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
`,
		"schema.yaml": `pins:
  inputs:
    - bootstrap.requested
`,
		"events.yaml": `bootstrap.requested:
  topic: text?
`,
		"nodes.yaml": `bootstrap-node:
  bootstrap.requested: {}
`,
		"operating/manifest.yaml": "version: 1.0.0\nplatform_version: '>=0.7.0 <0.8.0'\n",
		"operating/schema.yaml": `instance: operating_id
pins:
  inputs:
    - opco.product_initialization_requested
`,
		"operating/state.yaml": `operating_id: uuid
`,
		"operating/events.yaml": `opco.product_initialization_requested:
  topic: text?
`,
		"operating/nodes.yaml": `lifecycle-orchestrator:
  opco.product_initialization_requested:
    guard: instance.id != ""
`,
	}
	for relative, source := range files {
		writeClosedVariantFile(t, root, relative, source)
	}
	return root
}

// WriteFinitePublicTemplateInputRoute retains the private template while giving
// the public root a terminal stage for the run.start preflight journey.
func WriteFinitePublicTemplateInputRoute(t testing.TB) string {
	t.Helper()
	root := WritePublicTemplateInputRoute(t)
	writeClosedVariantFile(t, root, "schema.yaml", `stages:
  waiting: {}
  done: {final: true}
pins:
  inputs:
    - bootstrap.requested
`)
	writeClosedVariantFile(t, root, "nodes.yaml", `bootstrap-node:
  bootstrap.requested:
    advances_to: done
`)
	return root
}

// CopyRootOutputConnect derives the closed root-output-to-child route matrix
// from the checked-in parent-connect artifact.
func CopyRootOutputConnect(t testing.TB, emit RootConnectEmit) string {
	t.Helper()
	root := CopyExample(t, ParentConnect)
	if err := os.RemoveAll(filepath.Join(root, "producer")); err != nil {
		t.Fatalf("remove inherited producer flow: %v", err)
	}
	writeClosedVariantFile(t, root, "manifest.yaml", `version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
`)
	rootInput := ""
	rootNodes := ""
	if emit != RootConnectNoEmitter {
		rootInput = "  inputs: [root.start]\n"
		emitBody := "      emit:\n        event: root.ready\n        fields:\n          entity_id: payload.entity_id\n"
		if emit != RootConnectCanonicalEmit {
			t.Fatalf("unsupported root connect emitter %d", emit)
		}
		rootNodes = "root-node:\n  root.start:\n" + emitBody
	}
	writeClosedVariantFile(t, root, "schema.yaml", "pins:\n"+rootInput+"  outputs: [root.ready]\nconnect:\n  - event: root.ready\n    from: .\n    to: consumer\n")
	writeClosedVariantFile(t, root, "events.yaml", "root.start:\n  entity_id: text\nroot.ready:\n  entity_id: text\n")
	if rootNodes != "" {
		writeClosedVariantFile(t, root, "nodes.yaml", rootNodes)
	}
	writeLegacyInstanceFlow(t, root, "consumer", `pins:
  inputs:
    - root.ready
`, "", "", `consumer-node:
  root.ready:
    guard: instance.id != ""
`)
	return root
}

func CopyRootOutputConnectMissingReceiver(t testing.TB) string {
	t.Helper()
	root := CopyRootOutputConnect(t, RootConnectNoEmitter)
	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"), "    to: consumer\n", "    to: missing\n")
	removeClosedVariantFiles(t, root, "consumer/nodes.yaml")
	return root
}

func CopyRootOutputConnectWithChildPublisher(t testing.TB) string {
	t.Helper()
	root := CopyRootOutputConnect(t, RootConnectNoEmitter)
	writeClosedVariantFile(t, root, "child/manifest.yaml", "version: 1.0.0\nplatform_version: '>=0.7.0 <0.8.0'\n")
	writeClosedVariantFile(t, root, "child/schema.yaml", "pins:\n  outputs: [root.ready]\n")
	writeClosedVariantFile(t, root, "child/events.yaml", "root.ready:\n  entity_id: text\n")
	writeClosedVariantFile(t, root, "child/nodes.yaml", `child-node:
  root.ready:
    guard: instance.id != ""
`)
	return root
}

// CopyRootOutputSingletonConnect owns the canonical first-delivery
// root-to-singleton target-ownership proof.
func CopyRootOutputSingletonConnect(t testing.TB) string {
	t.Helper()
	root := CopyRootOutputConnect(t, RootConnectNoEmitter)
	writeClosedVariantFile(t, root, "manifest.yaml", `version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
`)
	writeClosedVariantFile(t, root, "schema.yaml", `pins:
  outputs:
    - root.ready
connect:
  - event: root.ready
    from: .
    to: consumer
`)
	writeLegacyInstanceFlow(t, root, "consumer", `pins:
  inputs:
    - root.ready
`, "", "", `consumer-node:
  root.ready: {}
`)
	return root
}

func CopyRootOutputSingletonArc(t testing.TB) string {
	t.Helper()
	root := CopyRootOutputSingletonConnect(t)
	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"), "    to: consumer\n", "    to: receiver\n")
	consumerDir := filepath.Join(root, "consumer")
	receiverDir := filepath.Join(root, "receiver")
	if err := os.Rename(consumerDir, receiverDir); err != nil {
		t.Fatalf("rename singleton receiver fixture: %v", err)
	}
	applyClosedReplacement(t, filepath.Join(receiverDir, "nodes.yaml"), "consumer-node", "arc-receiver")
	return root
}

// CopySingletonOutputRootConnect owns the reverse singleton-child-to-root
// routing proof through the compiler's root project view.
func CopySingletonOutputRootConnect(t testing.TB) string {
	t.Helper()
	return copySingletonOutputRootConnect(t, "scout")
}

func CopyNestedSingletonOutputRootConnect(t testing.TB) string {
	t.Helper()
	root := copySingletonOutputRootConnect(t, "left/child/scout")
	writeClosedVariantFile(t, root, "left/manifest.yaml", "version: 1.0.0\nplatform_version: '>=0.7.0 <0.8.0'\n")
	writeClosedVariantFile(t, root, "left/child/manifest.yaml", "version: 1.0.0\nplatform_version: '>=0.7.0 <0.8.0'\n")
	return root
}

func copySingletonOutputRootConnect(t testing.TB, scoutPath string) string {
	t.Helper()
	root := CopyRootOutputConnect(t, RootConnectNoEmitter)
	if err := os.RemoveAll(filepath.Join(root, "consumer")); err != nil {
		t.Fatalf("remove unused inherited consumer flow: %v", err)
	}
	writeClosedVariantFile(t, root, "manifest.yaml", `version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
`)
	writeClosedVariantFile(t, root, "schema.yaml", fmt.Sprintf(`pins:
  inputs: [scout.completed]
connect:
  - event: scout.completed
    from: %s
    to: .
`, scoutPath))
	writeClosedVariantFile(t, root, "nodes.yaml", `root-collector:
  scout.completed:
    guard: instance.id != ""
`)
	writeLegacyInstanceFlow(t, root, scoutPath, `pins:
  outputs:
    - scout.completed
`, "scout.completed:\n  proof: string\n", "", "")
	return root
}

// CopyRootSingletonBoomerang owns the reentrant root-to-child-to-root proof.
func CopyRootSingletonBoomerang(t testing.TB) string {
	t.Helper()
	root := CopyRootOutputConnect(t, RootConnectNoEmitter)
	if err := os.RemoveAll(filepath.Join(root, "consumer")); err != nil {
		t.Fatalf("remove unused inherited consumer flow: %v", err)
	}
	writeClosedVariantFile(t, root, "manifest.yaml", `version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
`)
	writeClosedVariantFile(t, root, "schema.yaml", `pins:
  inputs:
    - work.pong
  outputs:
    - work.ping
connect:
  - event: work.ping
    from: .
    to: boomerang
  - event: work.pong
    from: boomerang
    to: .
`)
	writeClosedVariantFile(t, root, "events.yaml", "work.ping:\n  turn: integer\n")
	writeClosedVariantFile(t, root, "nodes.yaml", `root-boomerang:
  work.pong:
    guard: instance.id != ""
`)
	writeLegacyInstanceFlow(t, root, "boomerang", `pins:
  inputs:
    - work.ping
  outputs:
    - work.pong
`, "work.pong:\n  turn: integer\n", "turn: integer\n", `boomerang-worker:
  work.ping:
    guard: instance.id != ""
`)
	return root
}
