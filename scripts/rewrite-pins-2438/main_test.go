package main

import (
	"bytes"
	"fmt"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/yamlsource"
	"gopkg.in/yaml.v3"
)

type inspectedSource struct{ root *yaml.Node }

func parseTestSource(data []byte) (*inspectedSource, error) {
	snapshot, err := yamlsource.Load(data)
	if err != nil {
		return nil, err
	}
	copy := snapshot.NodeCopy()
	return &inspectedSource{root: copy.Content[0]}, nil
}

func lookup(node *yaml.Node, key string) (*yaml.Node, error) {
	for i := 0; i < len(node.Content); i += 2 {
		if node.Content[i].Value == key {
			return node.Content[i+1], nil
		}
	}
	return nil, nil
}

func rewritten(t *testing.T, inputs map[string][]byte) (map[string][]byte, []change) {
	t.Helper()
	plan, err := planRewrite(inputs)
	if err != nil {
		t.Fatal(err)
	}
	result := map[string][]byte{}
	for name, data := range inputs {
		result[name] = bytes.Clone(data)
	}
	for _, item := range plan {
		if item.Delete {
			delete(result, item.Path)
		} else {
			result[item.Path] = item.After
		}
	}
	return result, plan
}

func TestRewriteNamesOnlyPreservesOtherSourceAndIsIdempotent(t *testing.T) {
	before := []byte(canonicalrouting.PinRewriteSyntaxSource(t, "RewriteNamesOnlyPreservesOtherSourceAndIsIdempotent-1"))
	after, plan := rewritten(t, map[string][]byte{"schema.yaml": before})
	if len(plan) != 1 {
		t.Fatalf("expected one rewrite, got %v", plan)
	}
	text := string(after["schema.yaml"])
	if !strings.HasPrefix(text, "# original heading\nname: 'unchanged'\n") || !strings.HasSuffix(text, canonicalrouting.PinRewriteSyntaxSource(t, "RewriteNamesOnlyPreservesOtherSourceAndIsIdempotent-2")) {
		t.Fatalf("unrelated source changed:\n%s", text)
	}
	doc, err := parseTestSource(after["schema.yaml"])
	if err != nil {
		t.Fatal(err)
	}
	pins, _ := lookup(doc.root, "pins")
	for _, direction := range []string{"inputs", "outputs"} {
		items, _ := lookup(pins, direction)
		if items.Kind != yaml.SequenceNode || len(items.Content) != 1 || items.Content[0].Kind != yaml.ScalarNode {
			t.Fatalf("%s not a names-only sequence", direction)
		}
	}
	_, repeat := rewritten(t, after)
	if len(repeat) != 0 {
		t.Fatalf("second rewrite is not empty: %v", repeat)
	}
}

func TestRewriteRejectsRetiredInitializePassenger(t *testing.T) {
	before := []byte("name: worker\npins:\n  inputs:\n    events:\n      - {event: worker.ready, initialize: {label: payload.label}}\n")
	inputs := map[string][]byte{"worker/schema.yaml": before}
	if _, err := planRewrite(inputs); err == nil {
		t.Fatal("retired initializer preserved by rewrite")
	}
	if !bytes.Equal(inputs["worker/schema.yaml"], before) {
		t.Fatal("rejected rewrite mutated its input")
	}
}

func replySources(t testing.TB, correlation bool) map[string][]byte {
	extra := ""
	if correlation {
		extra = "          correlation_key: request_id\n"
	}
	return map[string][]byte{
		replyRequester: []byte(canonicalrouting.PinRewriteSyntaxSource(t, "replySources-3") + extra + "  outputs:\n    events: [provider.requested]\n"),
		replyParent:    []byte(canonicalrouting.PinRewriteSyntaxSource(t, "replySources-4")),
	}
}

func TestRewriteMovesExactReplyAndCorrelationToResponseConnection(t *testing.T) {
	for _, correlation := range []bool{false, true} {
		t.Run(map[bool]string{false: "event_id", true: "explicit_correlation"}[correlation], func(t *testing.T) {
			after, plan := rewritten(t, replySources(t, correlation))
			if len(plan) != 2 {
				t.Fatalf("expected both schemas rewritten, got %v", plan)
			}
			if strings.Contains(string(after[replyRequester]), "replies_to") || strings.Contains(string(after[replyRequester]), "resolution") {
				t.Fatal("reply declaration survived on pin")
			}
			parent, err := parseTestSource(after[replyParent])
			if err != nil {
				t.Fatal(err)
			}
			connections, _ := lookup(parent.root, "connect")
			request, _ := lookup(connections.Content[0], "replies_to")
			response, _ := lookup(connections.Content[1], "replies_to")
			key, _ := lookup(connections.Content[1], "correlation_key")
			other, _ := lookup(connections.Content[2], "key_from")
			if request != nil || response == nil || response.Value != "provider.requested" || (key != nil) != correlation || other.Value != "payload.other_id" {
				t.Fatalf("incorrect connection rewrite: %s", after[replyParent])
			}
			_, repeat := rewritten(t, after)
			if len(repeat) != 0 {
				t.Fatal("reply relocation is not idempotent")
			}
		})
	}
}

func TestRewriteRejectsUnratifiedOrAmbiguousForms(t *testing.T) {
	for name, text := range map[string]string{
		"extra_document":    "pins: {inputs: {events: [work.start]}}\n---\nname: hidden\n",
		"duplicate":         "pins: {inputs: {events: [work.start], events: [other.start]}}\n",
		"null":              "pins: {inputs: null}\n",
		"alias":             "pins: {inputs: {events: &names [work.start]}, outputs: {events: *names}}\n",
		"unknown_grant":     "pins: {inputs: {events: [work.start], reads: [secret]}}\n",
		"unknown_option":    "pins: {inputs: {events: [{event: work.start, surprise: true}]}}\n",
		"wrong_sink":        "pins: {outputs: {events: [{event: work.done, sink: other}]}}\n",
		"output_init":       "pins: {outputs: {events: [{event: work.done, initialize: {x: payload.x}}]}}\n",
		"fan_out":           canonicalrouting.PinRewriteSyntaxSource(t, "RewriteRejectsUnratifiedOrAmbiguousForms-5"),
		"fan_in":            canonicalrouting.PinRewriteSyntaxSource(t, "RewriteRejectsUnratifiedOrAmbiguousForms-6"),
		"invalid_event":     "pins: {inputs: {events: [/work.start]}}\n",
		"qualified_event":   "pins: {inputs: {events: [worker/work.start]}}\n",
		"padded_event":      "pins: {inputs: {events: [' work.start ']}}\n",
		"duplicate_event":   "pins: {inputs: {events: [work.start, work.start]}}\n",
		"mixed_duplicate":   "pins: {outputs: {events: [work.done, {event: work.done, sink: harness}]}}\n",
		"empty_initialize":  "pins: {inputs: {events: [{event: work.start, initialize: {}}]}}\n",
		"redundant_mapping": "pins: {inputs: {events: [{event: work.start}]}}\n",
	} {
		t.Run(name, func(t *testing.T) {
			inputs := map[string][]byte{"valid/schema.yaml": []byte("name: valid\npins:\n  outputs:\n    events: [work.done]\n"), "invalid/schema.yaml": []byte(text)}
			if plan, err := planRewrite(inputs); err == nil || len(plan) != 0 {
				t.Fatalf("bad form generated a partial plan: %v %v", plan, err)
			}
		})
	}
	for name, replacement := range map[string]string{
		"missing_request": "  - {event: wrong.requested, from: requester, to: provider}",
		"other_provider":  "  - {event: provider.requested, from: requester, to: other}",
		"duplicate_reply": "  - {event: provider.requested, from: requester, to: provider}\n  - {event: provider.replied, from: other, to: requester}",
	} {
		t.Run(name, func(t *testing.T) {
			inputs := replySources(t, false)
			inputs[replyParent] = []byte(strings.Replace(string(inputs[replyParent]), "  - {event: provider.requested, from: requester, to: provider}", replacement, 1))
			if _, err := planRewrite(inputs); err == nil {
				t.Fatal("invalid paired topology admitted")
			}
		})
	}
	t.Run("padded_correlation", func(t *testing.T) {
		inputs := replySources(t, true)
		inputs[replyRequester] = []byte(strings.Replace(string(inputs[replyRequester]), "correlation_key: request_id", "correlation_key: ' request_id '", 1))
		if _, err := planRewrite(inputs); err == nil {
			t.Fatal("nonexact correlation key admitted")
		}
	})
	t.Run("multiple_reply_moves", func(t *testing.T) {
		parent, err := parse(replySources(t, false)[replyParent])
		if err != nil {
			t.Fatal(err)
		}
		item := reply{event: "provider.replied", request: "provider.requested"}
		if err := moveReply(parent, item); err != nil {
			t.Fatal(err)
		}
		if err := moveReply(parent, item); err == nil {
			t.Fatal("repeated reply move overwrote the first projection")
		}
	})
}

func TestRewriteDeletesOnlyExplicitRetiredFixtures(t *testing.T) {
	inputs := map[string][]byte{retiredHarness + "README.md": []byte("retired example")}
	for _, root := range retiredGrantFixtures {
		for _, name := range []string{"schema.yaml", "worker/schema.yaml", "tests/expected.yaml", "nodes.yaml", "manifest.yaml"} {
			inputs[root+name] = []byte("fixture marked for explicit deletion")
		}
	}
	inputs["other/schema.yaml"] = []byte("name: preserved\n")
	inputs["tests/tier11-flow-composition/test-data-pin-wiring-other/schema.yaml"] = []byte("name: unrelated\n")
	after, plan := rewritten(t, inputs)
	if len(plan) != len(inputs)-2 || len(after) != 2 || string(after["other/schema.yaml"]) != "name: preserved\n" {
		t.Fatalf("unexpected deletion set: %v", plan)
	}
	for name, data := range after {
		if !bytes.Equal(inputs[name], data) {
			t.Fatalf("unrelated source changed: %s", name)
		}
	}
}

func TestRewriteRetainsCommentsOnUntouchedSections(t *testing.T) {
	before := []byte("name: worker\n# boundary heading\npins:\n  inputs:\n    events: [work.start]\n\n# lifecycle heading\n# another lifecycle line\nstages: {idle: {initial: true}}\n")
	after, _ := rewritten(t, map[string][]byte{"schema.yaml": before})
	for _, expected := range []string{"name: worker\n# boundary heading\n", "\n# lifecycle heading\n# another lifecycle line\nstages: {idle: {initial: true}}\n"} {
		if !bytes.Contains(after["schema.yaml"], []byte(expected)) {
			t.Fatalf("unrelated comment changed:\n%s", after["schema.yaml"])
		}
	}
}

func TestRewritePreflightFailureMakesNoWrites(t *testing.T) {
	inputs := map[string][]byte{"schema.yaml": []byte("name: valid\npins:\n  outputs:\n    events: [work.done]\n"), "child/schema.yaml": []byte("pins: {inputs: {events: [invalid]}}\n---\nname: trailing\n")}
	root := trackedTestSources(t, inputs)
	if err := run(root, true); err == nil {
		t.Fatal("trailing document admitted")
	}
	for name, before := range inputs {
		after, err := os.ReadFile(filepath.Join(root, name))
		if err != nil || !bytes.Equal(before, after) {
			t.Fatalf("preflight failure mutated %s", name)
		}
	}
}

func trackedTestSources(t *testing.T, inputs map[string][]byte) string {
	t.Helper()
	root := t.TempDir()
	if result, err := exec.Command("git", "init", "-q", root).CombinedOutput(); err != nil {
		t.Fatalf("git init: %s %v", result, err)
	}
	for name, data := range inputs {
		file := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(file), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(file, data, 0644); err != nil {
			t.Fatal(err)
		}
	}
	if result, err := exec.Command("git", "-C", root, "add", ".").CombinedOutput(); err != nil {
		t.Fatalf("git add: %s %v", result, err)
	}
	return root
}

func TestRewriteApplicationPreservesModesAndIsIdempotent(t *testing.T) {
	inputs := replySources(t, true)
	inputs[retiredHarness+"README.md"] = []byte("retired example\n")
	for _, root := range retiredGrantFixtures {
		inputs[root+"child/schema.yaml"] = []byte("retired grant mechanism\n")
	}
	inputs["nodes.yaml"] = []byte("not part of this rewrite\n")
	root := trackedTestSources(t, inputs)
	requester := filepath.Join(root, replyRequester)
	if err := os.Chmod(requester, 0640); err != nil {
		t.Fatal(err)
	}
	if err := run(root, false); err != nil {
		t.Fatal(err)
	}
	for name, before := range inputs {
		after, err := os.ReadFile(filepath.Join(root, name))
		if err != nil || !bytes.Equal(before, after) {
			t.Fatalf("dry run mutated %s", name)
		}
	}
	if err := run(root, true); err != nil {
		t.Fatal(err)
	}
	selected := replySources(t, true)
	selected[retiredHarness+"README.md"] = inputs[retiredHarness+"README.md"]
	for _, root := range retiredGrantFixtures {
		selected[root+"child/schema.yaml"] = inputs[root+"child/schema.yaml"]
	}
	expected, _ := rewritten(t, selected)
	expected["nodes.yaml"] = inputs["nodes.yaml"]
	for name, data := range expected {
		actual, err := os.ReadFile(filepath.Join(root, name))
		if err != nil || !bytes.Equal(actual, data) {
			t.Fatalf("application did not match plan for %s: %v", name, err)
		}
	}
	if _, err := os.Lstat(filepath.Join(root, retiredHarness, "README.md")); !os.IsNotExist(err) {
		t.Fatalf("retired fixture survived: %v", err)
	}
	for _, prefix := range append([]string{retiredHarness}, retiredGrantFixtures...) {
		if _, err := os.Lstat(filepath.Join(root, filepath.FromSlash(prefix))); !os.IsNotExist(err) {
			t.Fatalf("retired fixture still discoverable: %s %v", prefix, err)
		}
	}
	info, err := os.Stat(requester)
	if err != nil || info.Mode().Perm() != 0640 {
		t.Fatalf("requester permissions changed: %v", err)
	}
	if err := run(root, true); err != nil {
		t.Fatalf("second application failed: %v", err)
	}
	for name, data := range expected {
		actual, err := os.ReadFile(filepath.Join(root, name))
		if err != nil || !bytes.Equal(actual, data) {
			t.Fatalf("second application changed %s: %v", name, err)
		}
	}
}

func TestRewriteRetirementRejectsUntrackedFilesBeforeWrites(t *testing.T) {
	inputs := map[string][]byte{
		"schema.yaml":                           []byte("pins: {inputs: {events: [work.start]}}\n"),
		retiredGrantFixtures[0] + "schema.yaml": []byte("retired fixture\n"),
	}
	root := trackedTestSources(t, inputs)
	untracked := filepath.Join(root, filepath.FromSlash(retiredGrantFixtures[0]), "operator-notes.txt")
	if err := os.WriteFile(untracked, []byte("keep me\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := run(root, true); err == nil || !strings.Contains(err.Error(), "untracked file") {
		t.Fatalf("untracked data not rejected before writes: %v", err)
	}
	for name, before := range inputs {
		after, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(name)))
		if err != nil || !bytes.Equal(before, after) {
			t.Fatalf("retirement preflight mutated %s: %v", name, err)
		}
	}
	if data, err := os.ReadFile(untracked); err != nil || string(data) != "keep me\n" {
		t.Fatalf("untracked operator data changed: %v", err)
	}
}

func TestRewriteRejectsTrackedSymlinkBeforeWriting(t *testing.T) {
	before := []byte("name: valid\npins:\n  outputs:\n    events: [work.done]\n")
	root := trackedTestSources(t, map[string][]byte{"schema.yaml": before})
	if err := os.Mkdir(filepath.Join(root, "child"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("../schema.yaml", filepath.Join(root, "child/schema.yaml")); err != nil {
		t.Fatal(err)
	}
	if result, err := exec.Command("git", "-C", root, "add", "child/schema.yaml").CombinedOutput(); err != nil {
		t.Fatalf("git add: %s %v", result, err)
	}
	if err := run(root, true); err == nil {
		t.Fatal("tracked symlink admitted")
	}
	after, err := os.ReadFile(filepath.Join(root, "schema.yaml"))
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("symlink rejection changed the real source")
	}
}

func TestRewriteActualCorpusPreservesNamesOnlyEvents(t *testing.T) {
	root := filepath.Join("..", "..")
	tracked, err := exec.Command("git", "-C", root, "ls-files", "-z").Output()
	if err != nil {
		t.Fatal(err)
	}
	inputs := map[string][]byte{}
	for _, name := range strings.Split(string(tracked), "\x00") {
		if !schemaFile(name) && !retiredArtifact(name) {
			continue
		}
		data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(name)))
		if os.IsNotExist(err) && retiredArtifact(name) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		inputs[name] = data
	}
	after, _ := rewritten(t, inputs)
	if err := checkGenericSchemaSurvivors(inputs, after); err != nil {
		t.Fatal(err)
	}
	checkGenericSurvivorMutations(t, inputs, after)
	for name, data := range after {
		before, err := parseTestSource(inputs[name])
		if err != nil {
			t.Fatal(err)
		}
		doc, err := parseTestSource(data)
		if err != nil {
			t.Fatal(err)
		}
		oldPins, _ := lookup(before.root, "pins")
		pins, _ := lookup(doc.root, "pins")
		if oldPins == nil {
			if !bytes.Equal(inputs[name], data) {
				t.Fatalf("no-pin source changed: %s", name)
			}
			continue
		}
		for i := 0; i < len(pins.Content); i += 2 {
			direction := pins.Content[i].Value
			oldList, _ := lookup(oldPins, direction)
			if oldList.Kind == yaml.MappingNode {
				oldList, _ = lookup(oldList, "events")
			}
			list := pins.Content[i+1]
			if list.Kind != yaml.SequenceNode || len(list.Content) != len(oldList.Content) {
				t.Fatalf("event cardinality changed: %s %s", name, direction)
			}
			for index, item := range list.Content {
				oldItem := oldList.Content[index]
				if oldItem.Kind == yaml.MappingNode {
					oldItem, _ = lookup(oldItem, "event")
				}
				if item.Kind != yaml.ScalarNode {
					t.Fatalf("unexpected pin passenger: %s", name)
				}
				if item.Value != oldItem.Value || item.Tag != oldItem.Tag {
					t.Fatalf("event changed: %s %s #%d", name, direction, index)
				}
			}
		}
	}
	_, repeat := rewritten(t, after)
	if len(repeat) != 0 {
		t.Fatalf("actual corpus rewrite not idempotent: %d extra edits", len(repeat))
	}
}

func checkGenericSurvivorMutations(t *testing.T, before, after map[string][]byte) {
	t.Helper()
	for name := range requiredGenericInputs {
		t.Run(name, func(t *testing.T) {
			original := after[name]
			defer func() { after[name] = original }()
			delete(after, name)
			if err := checkGenericSchemaSurvivors(before, after); err == nil {
				t.Fatal("restored old deletion selection escaped the required-survivor oracle")
			}
			after[name] = bytes.Replace(original, []byte("stages:"), []byte("lost_stages:"), 1)
			if err := checkGenericSchemaSurvivors(before, after); err == nil {
				t.Fatal("lost non-pin section escaped the preservation oracle")
			}
		})
	}
}

func TestRewriteRejectsUnaccountedGrantLocations(t *testing.T) {
	for name := range requiredGenericInputs {
		for _, pins := range []string{
			"inputs: {events: [work.start], writes: [field]}",
			"outputs: {reads: [field]}",
			"inputs: {events: [work.start], reads: null}",
			"inputs: {events: [work.start], reads: []}",
			"inputs: {events: [work.start], reads: [' padded ']}",
		} {
			if _, err := planRewrite(map[string][]byte{name: []byte("pins: {" + pins + "}\n")}); err == nil {
				t.Fatalf("unaccounted grant admitted at %s: %s", name, pins)
			}
		}
	}
	if _, err := planRewrite(map[string][]byte{"other/schema.yaml": []byte("pins: {outputs: {writes: [field]}}\n")}); err == nil {
		t.Fatal("grant at a new source location silently discarded")
	}
}

var requiredGenericInputs = map[string][]string{
	"internal/runtime/testdata/generic-swarm-bundle/intake/schema.yaml":     {"item.created", "item.processed", "item.rejected"},
	"internal/runtime/testdata/generic-swarm-bundle/processing/schema.yaml": {"item.review_requested", "item.rejected"},
	"internal/runtime/testdata/generic-swarm-bundle/delivery/schema.yaml":   {"item.completed", "timer.item.timeout"},
}

func TestRewriteGenericSchemasRetiresOnlyGrants(t *testing.T) {
	inputs := map[string][]byte{}
	for name, events := range requiredGenericInputs {
		data, err := os.ReadFile(filepath.Join("..", "..", filepath.FromSlash(name)))
		if err != nil {
			t.Fatal(err)
		}
		doc, err := parse(data)
		if err != nil {
			t.Fatal(err)
		}
		// Reapply the retired spelling to the live schemas so the regression
		// continues to exercise grant removal after the corpus is rewritten.
		doc.edit["pins"] = map[string]any{
			"inputs":  map[string]any{"events": events, "reads": []string{"retired_read"}},
			"outputs": map[string]any{"writes": []string{"retired_write"}},
		}
		inputs[name], err = render(doc)
		if err != nil {
			t.Fatal(err)
		}
	}
	after, plan := rewritten(t, inputs)
	if len(plan) != 3 {
		t.Fatalf("expected only the three preserved schemas rewritten: %v", plan)
	}
	if err := checkGenericSchemaSurvivors(inputs, after); err != nil {
		t.Fatal(err)
	}
	checkGenericSurvivorMutations(t, inputs, after)
}

func checkGenericSchemaSurvivors(before, after map[string][]byte) error {
	for name, events := range requiredGenericInputs {
		if len(before[name]) == 0 || len(after[name]) == 0 {
			return fmt.Errorf("required generic schema did not survive: %s", name)
		}
		doc, err := parseTestSource(after[name])
		if err != nil {
			return err
		}
		pins, _ := lookup(doc.root, "pins")
		if pins == nil || len(pins.Content) != 2 || pins.Content[0].Value != "inputs" {
			return fmt.Errorf("expected only real input events, no invented outputs: %s", name)
		}
		inputs := pins.Content[1]
		if inputs.Kind != yaml.SequenceNode || len(inputs.Content) != len(events) {
			return fmt.Errorf("required input cardinality changed: %s", name)
		}
		for index, event := range events {
			if inputs.Content[index].Kind != yaml.ScalarNode || inputs.Content[index].Value != event {
				return fmt.Errorf("required input changed: %s #%d", name, index)
			}
		}
		oldSections, err := nonPinSections(before[name])
		if err != nil {
			return err
		}
		newSections, err := nonPinSections(after[name])
		if err != nil {
			return err
		}
		if !bytes.Equal(oldSections, newSections) {
			return fmt.Errorf("non-pin semantic sections changed: %s", name)
		}
	}
	return nil
}

func nonPinSections(data []byte) ([]byte, error) {
	doc, err := parseTestSource(data)
	if err != nil {
		return nil, err
	}
	for i := 0; i < len(doc.root.Content); i += 2 {
		if doc.root.Content[i].Value == "pins" {
			doc.root.Content = append(doc.root.Content[:i], doc.root.Content[i+2:]...)
			break
		}
	}
	return yaml.Marshal(doc.root)
}
