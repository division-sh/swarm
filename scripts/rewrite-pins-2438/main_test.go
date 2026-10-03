package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

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
	before := []byte("# original heading\nname: 'unchanged'\npins:\n  inputs:\n    events: [work.started]\n  outputs:\n    events:\n      - event: work.done\n        sink: harness\n\nstages: {idle: {initial: true}}\nconnect:\n  - {event: work.started, from: ., to: child, resolution: create}\n")
	after, plan := rewritten(t, map[string][]byte{"schema.yaml": before})
	if len(plan) != 1 {
		t.Fatalf("expected one rewrite, got %v", plan)
	}
	text := string(after["schema.yaml"])
	if !strings.HasPrefix(text, "# original heading\nname: 'unchanged'\n") || !strings.HasSuffix(text, "\nstages: {idle: {initial: true}}\nconnect:\n  - {event: work.started, from: ., to: child, resolution: create}\n") {
		t.Fatalf("unrelated source changed:\n%s", text)
	}
	doc, err := parse(after["schema.yaml"])
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

func TestRewritePreservesInitializePassengerExactly(t *testing.T) {
	before := []byte("name: worker\ninstance: worker_id\npins:\n  inputs:\n    events:\n      - initialize: {label: payload.label, nested: payload.details}\n        event: worker.ready\n  outputs:\n    events: []\ninstance_variables: {variables: {label: text}}\n")
	after, _ := rewritten(t, map[string][]byte{"worker/schema.yaml": before})
	doc, err := parse(after["worker/schema.yaml"])
	if err != nil {
		t.Fatal(err)
	}
	pins, _ := lookup(doc.root, "pins")
	inputs, _ := lookup(pins, "inputs")
	item := inputs.Content[0]
	event, _ := lookup(item, "event")
	initialize, _ := lookup(item, "initialize")
	label, _ := lookup(initialize, "label")
	nested, _ := lookup(initialize, "nested")
	if event.Value != "worker.ready" || label.Value != "payload.label" || nested.Value != "payload.details" || len(item.Content) != 4 {
		t.Fatalf("initialization changed: %s", after["worker/schema.yaml"])
	}
	if !bytes.HasSuffix(after["worker/schema.yaml"], []byte("instance_variables: {variables: {label: text}}\n")) {
		t.Fatal("deferred instance variables changed")
	}
}

func replySources(correlation bool) map[string][]byte {
	extra := ""
	if correlation {
		extra = "          correlation_key: request_id\n"
	}
	return map[string][]byte{
		replyRequester: []byte("name: requester\npins:\n  inputs:\n    events:\n      - event: provider.replied\n        resolution:\n          mode: reply\n          replies_to: provider.requested\n" + extra + "  outputs:\n    events: [provider.requested]\n"),
		replyParent:    []byte("name: parent\npins:\n  inputs:\n    events: [request.start]\nconnect:\n  - {event: provider.requested, from: requester, to: provider}\n  - {event: provider.replied, from: provider, to: requester}\n  - {event: other.requested, from: requester, to: other, resolution: select, key_from: payload.other_id}\n"),
	}
}

func TestRewriteMovesExactReplyAndCorrelationToResponseConnection(t *testing.T) {
	for _, correlation := range []bool{false, true} {
		t.Run(map[bool]string{false: "event_id", true: "explicit_correlation"}[correlation], func(t *testing.T) {
			after, plan := rewritten(t, replySources(correlation))
			if len(plan) != 2 {
				t.Fatalf("expected both schemas rewritten, got %v", plan)
			}
			if strings.Contains(string(after[replyRequester]), "replies_to") || strings.Contains(string(after[replyRequester]), "resolution") {
				t.Fatal("reply declaration survived on pin")
			}
			parent, err := parse(after[replyParent])
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
		"extra_document": "pins: {inputs: {events: [work.start]}}\n---\nname: hidden\n",
		"duplicate":      "pins: {inputs: {events: [work.start], events: [other.start]}}\n",
		"null":           "pins: {inputs: null}\n",
		"alias":          "pins: {inputs: {events: &names [work.start]}, outputs: {events: *names}}\n",
		"unknown_grant":  "pins: {inputs: {events: [work.start], reads: [secret]}}\n",
		"unknown_option": "pins: {inputs: {events: [{event: work.start, surprise: true}]}}\n",
		"wrong_sink":     "pins: {outputs: {events: [{event: work.done, sink: other}]}}\n",
		"output_init":    "pins: {outputs: {events: [{event: work.done, initialize: {x: payload.x}}]}}\n",
		"fan_out":        "pins: {inputs: {events: [{event: work.start, resolution: {mode: fan-out}}]}}\n",
		"fan_in":         "pins: {inputs: {events: [{event: work.start, resolution: {mode: fan-in, aggregation: sum}}]}}\n",
		"invalid_event":  "pins: {inputs: {events: [/work.start]}}\n",
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
			inputs := replySources(false)
			inputs[replyParent] = []byte(strings.Replace(string(inputs[replyParent]), "  - {event: provider.requested, from: requester, to: provider}", replacement, 1))
			if _, err := planRewrite(inputs); err == nil {
				t.Fatal("invalid paired topology admitted")
			}
		})
	}
}

func TestRewriteDeletesOnlyExplicitRetiredFixtures(t *testing.T) {
	inputs := map[string][]byte{retiredHarness + "README.md": []byte("retired example")}
	for name := range retiredGrantSchemas {
		inputs[name] = []byte("fixture marked for explicit deletion")
	}
	inputs["other/schema.yaml"] = []byte("name: preserved\n")
	after, plan := rewritten(t, inputs)
	if len(plan) != len(retiredGrantSchemas)+1 || len(after) != 1 || string(after["other/schema.yaml"]) != "name: preserved\n" {
		t.Fatalf("unexpected deletion set: %v", plan)
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
	root := t.TempDir()
	command := exec.Command("git", "init", "-q", root)
	if result, err := command.CombinedOutput(); err != nil {
		t.Fatalf("git init: %s %v", result, err)
	}
	inputs := map[string][]byte{"schema.yaml": []byte("name: valid\npins:\n  outputs:\n    events: [work.done]\n"), "child/schema.yaml": []byte("pins: {inputs: {events: [invalid]}}\n---\nname: trailing\n")}
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

func TestRewriteActualCorpusPreservesEventsAndDeferredInitialization(t *testing.T) {
	root := filepath.Join("..", "..")
	tracked, err := exec.Command("git", "-C", root, "ls-files", "-z").Output()
	if err != nil {
		t.Fatal(err)
	}
	inputs := map[string][]byte{}
	for _, name := range strings.Split(string(tracked), "\x00") {
		if !schemaFile(name) && !strings.HasPrefix(name, retiredHarness) {
			continue
		}
		data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(name)))
		if os.IsNotExist(err) && (retiredGrantSchemas[name] || strings.HasPrefix(name, retiredHarness)) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		inputs[name] = data
	}
	after, _ := rewritten(t, inputs)
	initializers := 0
	for name, data := range after {
		before, err := parse(inputs[name])
		if err != nil {
			t.Fatal(err)
		}
		doc, err := parse(data)
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
				if item.Kind == yaml.MappingNode {
					initializer, _ := lookup(item, "initialize")
					if direction != "inputs" || len(item.Content) != 4 || initializer == nil {
						t.Fatalf("unexpected passenger: %s", name)
					}
					initializers++
					item, _ = lookup(item, "event")
				}
				if item.Value != oldItem.Value || item.Tag != oldItem.Tag {
					t.Fatalf("event changed: %s %s #%d", name, direction, index)
				}
			}
		}
	}
	if initializers != 3 {
		t.Fatalf("expected the three deferred initialization carriers, got %d", initializers)
	}
	_, repeat := rewritten(t, after)
	if len(repeat) != 0 {
		t.Fatalf("actual corpus rewrite not idempotent: %d extra edits", len(repeat))
	}
}
