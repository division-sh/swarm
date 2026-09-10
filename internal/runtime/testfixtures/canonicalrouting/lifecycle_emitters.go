package canonicalrouting

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// CopyGateCompletionDiagnostic is a source-only typed gate used on both the
// baseline and compiled-transition trees; it carries no transition test API.
func CopyGateCompletionDiagnostic(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	writeClosedVariantFile(t, root, "schema.yaml", `name: gate-completion-diagnostic
stages:
  waiting: {initial: true}
  review:
    gate:
      decision: review_decision
      outcomes:
        approve:
          advances_to: approved
          emit:
            event: work.completed
            fields: {result: {literal: approved}}
        reject:
          advances_to: approved
          emit:
            event: work.completed
            fields: {result: {literal: rejected}}
  approved: {}
  done: {terminal: true}
pins:
  inputs:
    events:
      - {event: work.requested, source: external}
`)
	writeClosedVariantFile(t, root, "events.yaml", "work.requested:\n  seed: boolean\nwork.completed:\n  result: text\n")
	writeClosedVariantFile(t, root, "entities.yaml", "work:\n  result: text\n")
	writeClosedVariantFile(t, root, "nodes.yaml", `requester:
  id: requester
  execution_type: system_node
  subscribes_to: [work.requested]
  event_handlers:
    work.requested:
      create_entity: true
      advances_to: waiting
      emit:
        event: work.completed
        fields: {result: {literal: ready}}
collector:
  id: collector
  execution_type: system_node
  subscribes_to: [work.completed]
  event_handlers:
    work.completed:
      data_accumulation:
        writes:
          - target_field: result
            expression: payload.result
      rules:
        enter_review:
          condition: "payload.result == 'ready'"
          advances_to: review
        finish:
          condition: "payload.result in ['approved', 'rejected']"
          advances_to: done
`)
	return root
}

// CopyLifecycleSelectedCarriers deliberately collides stage, node, event and
// rule labels across exact flows, with two same-pair rules per handler.
func CopyLifecycleSelectedCarriers(t testing.TB, reordered bool) string {
	t.Helper()
	root := t.TempDir()
	flows := []string{"", "child/", "sibling/", "child/nested/"}
	if reordered {
		flows = []string{"child/nested/", "sibling/", "child/", ""}
	}
	for _, prefix := range flows {
		writeClosedVariantFile(t, root, prefix+"schema.yaml", `name: selected-carriers
stages:
  waiting: {initial: true}
  active: {}
  done: {terminal: true}
pins:
  inputs:
    events:
      - {event: work.seeded, source: external}
      - {event: work.first, source: external}
      - {event: work.second, source: external}
`)
		writeClosedVariantFile(t, root, prefix+"events.yaml", "work.seeded:\n  seed: boolean\nwork.first:\n  choice: text\nwork.second:\n  choice: text\nwork.completed:\n  result: text\n")
		writeClosedVariantFile(t, root, prefix+"entities.yaml", "work:\n  result: text\n")
		nodes := `controller:
  id: controller
  execution_type: system_node
  subscribes_to: [work.seeded, work.first, work.second]
  event_handlers:
    work.seeded:
      create_entity: true
      advances_to: waiting
`
		handlers := []string{"first", "second"}
		if reordered {
			handlers = []string{"second", "first"}
		}
		for _, handler := range handlers {
			nodes += fmt.Sprintf(`    work.%s:
      guard:
        checks:
          - {id: %s_choice, check: "payload.choice in ['alpha', 'beta']"}
          - {id: %s_nonempty, check: "payload.choice != ''"}
      rules:
`, handler, handler, handler)
			for _, choice := range []string{"alpha", "beta"} {
				nodes += fmt.Sprintf(`        %s:
          condition: "payload.choice == '%s'"
          advances_to: active
          emit:
            event: work.completed
            fields: {result: {literal: '%s%s/%s'}}
`, choice, choice, prefix, handler, choice)
			}
		}
		nodes += `collector:
  id: collector
  execution_type: system_node
  subscribes_to: [work.completed]
  event_handlers:
    work.completed:
      data_accumulation:
        writes:
          - target_field: result
            expression: payload.result
      advances_to: done
`
		writeClosedVariantFile(t, root, prefix+"nodes.yaml", nodes)
	}
	return root
}

// CopyLifecycleChangedGate is a valid but incompatible ambient source. Pending
// cards from the original source must never acquire this target/event/schema.
func CopyLifecycleChangedGate(t testing.TB) string {
	t.Helper()
	root := CopyLifecycleEmitter(t, LifecycleGateSharedEvent)
	for _, path := range []string{"schema.yaml", "events.yaml", "nodes.yaml", "entities.yaml"} {
		raw, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		text := strings.ReplaceAll(string(raw), "work.completed", "work.changed")
		text = strings.ReplaceAll(text, "approved", "changed")
		text = strings.ReplaceAll(text, "rejected", "changed_reject")
		text = strings.ReplaceAll(text, "result", "changed_result")
		writeClosedVariantFile(t, root, path, text)
	}
	return root
}

func CopyLifecycleGateAdvanceOnly(t testing.TB) string {
	t.Helper()
	root := CopyLifecycleEmitter(t, LifecycleGateNoEmit)
	raw, err := os.ReadFile(filepath.Join(root, "schema.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	text := strings.Replace(string(raw), "  approved: {}\n  done: {terminal: true}", "  approved: {terminal: true}", 1)
	writeClosedVariantFile(t, root, "schema.yaml", text)
	writeClosedVariantFile(t, root, "events.yaml", "work.requested:\n  seed: boolean\n")
	writeClosedVariantFile(t, root, "entities.yaml", "work: {}\n")
	return root
}

// CopyLifecycleNestedCascade keeps both nested siblings on the same source,
// with reused node/stage/loop/decision/event names and actual parent connects.
func CopyLifecycleNestedCascade(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	writeClosedVariantFile(t, root, "schema.yaml", "name: cascade\n")
	writeClosedVariantFile(t, root, "outer/schema.yaml", "name: outer\n")
	for _, side := range []string{"left", "right"} {
		prefix := "outer/" + side + "/"
		loopRoot := CopyLifecycleEmitter(t, LifecycleLoopConnected)
		for _, path := range []string{"schema.yaml", "events.yaml", "entities.yaml", "nodes.yaml", "sink/entities.yaml", "sink/nodes.yaml"} {
			raw, err := os.ReadFile(filepath.Join(loopRoot, path))
			if err != nil {
				t.Fatal(err)
			}
			text := string(raw)
			if path == "sink/nodes.yaml" {
				text = strings.Replace(text, "advances_to: done", "advances_to: review", 1)
			}
			writeClosedVariantFile(t, root, prefix+path, text)
		}
		writeClosedVariantFile(t, root, prefix+"sink/schema.yaml", fmt.Sprintf(`name: review
stages:
  waiting: {initial: true}
  review:
    gate:
      decision: review_decision
      outcomes:
        approve:
          advances_to: approved
          emit:
            event: work.completed
            fields: {result: {literal: %s}}
  approved: {terminal: true}
pins:
  inputs:
    events: [loop.escaped]
  outputs:
    events: [work.completed]
connect:
  - {event: work.completed, from: ., to: final}
`, side))
		writeClosedVariantFile(t, root, prefix+"sink/events.yaml", "work.completed:\n  result: text\n")
		writeClosedVariantFile(t, root, prefix+"sink/final/schema.yaml", "name: final\nstages:\n  waiting: {initial: true}\n  done: {terminal: true}\npins:\n  inputs:\n    events: [work.completed]\n")
		writeClosedVariantFile(t, root, prefix+"sink/final/entities.yaml", "receipt:\n  result: text\n")
		writeClosedVariantFile(t, root, prefix+"sink/final/nodes.yaml", `collector:
  id: collector
  execution_type: system_node
  subscribes_to: [work.completed]
  event_handlers:
    work.completed:
      create_entity: true
      data_accumulation:
        writes:
          - target_field: result
            expression: payload.result
      advances_to: done
`)
	}
	return root
}

// CopyLifecycleNestedTemplates uses the same scalar key in sibling loop
// templates and creates a second template level from each escaped revision.
// Every command crosses a real parent connect; no instance state is seeded.
func CopyLifecycleNestedTemplates(t testing.TB) string {
	t.Helper()
	root := CopyLifecycleNestedCascade(t)
	inputs, outputs, connects := "", "", ""
	events := "work.observed:\n  seed: boolean\nwork.finished:\n  seed: boolean\n"
	nodes := "controller:\n  id: controller\n  execution_type: system_node\n  subscribes_to: [work.observed, work.finished"
	handlers := "  event_handlers:\n    work.finished:\n      advances_to: done\n    work.observed:\n      guard:\n        id: frontier_noop\n        check: payload.seed\n"
	commands := []struct{ name, event, field, typ string }{
		{"seed", "loop.start", "seed", "boolean"},
		{"admit", "loop.admit", "revision_id", "text"},
		{"repeat", "loop.repeat", "revision_id", "text"},
		{"close", "loop.close", "revision_id", "text"},
	}
	for _, side := range []string{"left", "right"} {
		prefix := "outer/" + side + "/"
		loopInputs := ""
		for _, command := range commands {
			input := side + ".command." + command.name
			output := side + "." + command.event
			inputs += fmt.Sprintf("      - {event: %s, source: external}\n", input)
			outputs += "      - " + output + "\n"
			connects += fmt.Sprintf("  - {event: %s, from: ., to: outer/%s, rename: %s}\n", output, side, command.event)
			for _, event := range []string{input, output} {
				events += fmt.Sprintf("%s:\n  key: case_id\n  case_id: text\n  %s: %s\n", event, command.field, command.typ)
			}
			nodes += ", " + input
			handlers += fmt.Sprintf("    %s:\n      advances_to: active\n      emit:\n        event: %s\n        fields:\n          case_id: payload.case_id\n          %s: payload.%s\n", input, output, command.field, command.field)
			mode := "select"
			if command.name == "seed" {
				mode = "select-or-create"
			}
			loopInputs += fmt.Sprintf("      - event: %s\n        resolution: {mode: %s}\n", command.event, mode)
		}
		raw, err := os.ReadFile(filepath.Join(root, prefix+"schema.yaml"))
		if err != nil {
			t.Fatal(err)
		}
		schema := strings.Replace(string(raw), "name: lifecycle-loop\n", "name: lifecycle-loop\nmode: template\ninstance: case_id\n", 1)
		begin := strings.Index(schema, "      - {event: work.requested")
		end := strings.Index(schema, "  outputs:")
		if begin < 0 || end <= begin {
			t.Fatal("loop template input boundary missing")
		}
		schema = schema[:begin] + loopInputs + schema[end:]
		writeClosedVariantFile(t, root, prefix+"schema.yaml", schema)
		writeClosedVariantFile(t, root, prefix+"events.yaml", "loop.escaped:\n  key: revision_id\n  revision_id: text\n")
		writeClosedVariantFile(t, root, prefix+"entities.yaml", "work:\n  case_id: {type: text, _unused_reason: receiver instance identity}\n")
		for _, file := range []string{"nodes.yaml", "sink/nodes.yaml"} {
			raw, err := os.ReadFile(filepath.Join(root, prefix+file))
			if err != nil {
				t.Fatal(err)
			}
			text := strings.ReplaceAll(string(raw), "      create_entity: true\n", "")
			if file == "nodes.yaml" {
				text = strings.Replace(text, "[work.requested, loop.start,", "[loop.start,", 1)
				text = strings.Replace(text, "    work.requested:\n      advances_to: waiting\n", "", 1)
			}
			writeClosedVariantFile(t, root, prefix+file, text)
		}
		raw, err = os.ReadFile(filepath.Join(root, prefix+"sink/schema.yaml"))
		if err != nil {
			t.Fatal(err)
		}
		sink := strings.Replace(string(raw), "name: review\n", "name: review\nmode: template\ninstance: revision_id\n", 1)
		sink = strings.Replace(sink, "    events: [loop.escaped]\n", "    events:\n      - event: loop.escaped\n        resolution: {mode: select-or-create}\n", 1)
		writeClosedVariantFile(t, root, prefix+"sink/schema.yaml", sink)
	}
	writeClosedVariantFile(t, root, "schema.yaml", "name: template-driver\nstages:\n  active: {initial: true}\n  done: {terminal: true}\npins:\n  inputs:\n    events:\n      - {event: work.observed, source: external}\n      - {event: work.finished, source: external}\n"+inputs+"  outputs:\n    events:\n"+outputs+"connect:\n"+connects)
	writeClosedVariantFile(t, root, "events.yaml", events)
	writeClosedVariantFile(t, root, "entities.yaml", "driver: {}\n")
	writeClosedVariantFile(t, root, "nodes.yaml", nodes+"]\n"+handlers)
	return root
}

// CopyLifecycleForkSource gives a pending lifecycle a public, non-transitioning
// frontier event, so fork activation does not require synthetic store seeding.
func CopyLifecycleForkSource(t testing.TB, gate bool) string {
	t.Helper()
	variant := LifecycleLoopConnected
	if gate {
		variant = LifecycleGateSharedEvent
	}
	root := CopyLifecycleEmitter(t, variant)
	if !gate {
		return root
	}
	for _, path := range []string{"schema.yaml", "events.yaml", "nodes.yaml"} {
		raw, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		text := string(raw)
		switch path {
		case "schema.yaml":
			text = strings.Replace(text, "      - {event: work.requested, source: external}", "      - {event: work.requested, source: external}\n      - {event: work.observed, source: external}", 1)
		case "events.yaml":
			text += "work.observed:\n  seed: boolean\n"
		case "nodes.yaml":
			text += `frontier:
  id: frontier
  execution_type: system_node
  subscribes_to: [work.observed]
  event_handlers:
    work.observed:
      guard: {id: observed, check: "payload.seed == true"}
`
		}
		writeClosedVariantFile(t, root, path, text)
	}
	return root
}

// CopyLifecycleEmitterMetadata adds one deliberately invalid internal-role claim.
func CopyLifecycleEmitterMetadata(t testing.TB, variant LifecycleEmitterVariant, field string, site bool) string {
	t.Helper()
	root := CopyLifecycleEmitter(t, variant)
	event, role := "work.completed", "review_decision"
	if site {
		role = "stages.review.gate.outcomes.approve.emit"
	}
	switch variant {
	case LifecycleGateLocal:
	case LifecycleLoopConnected:
		event, role = "loop.escaped", "revision"
		if site {
			role = "loops.revision.escape.emit"
		}
	default:
		t.Fatalf("unsupported metadata fixture %d", variant)
	}
	value := role
	switch field {
	case "source":
	case "producer", "consumer":
		value = "[" + role + "]"
	default:
		t.Fatalf("unsupported metadata field %s", field)
	}
	raw, err := os.ReadFile(filepath.Join(root, "events.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	text := strings.Replace(string(raw), event+":\n", event+":\n  swarm:\n    "+field+": "+value+"\n", 1)
	writeClosedVariantFile(t, root, "events.yaml", text)
	return root
}

type LifecycleEmitterVariant uint8

const (
	LifecycleGateLocal LifecycleEmitterVariant = iota
	LifecycleGateDangling
	LifecycleGateSharedEvent
	LifecycleGateNoEmit
	LifecycleGateOutputDisconnected
	LifecycleGateNested
	LifecycleLoopConnected
	LifecycleLoopRepeatEmits
)

// CopyLifecycleEmitter constructs only the source-backed lifecycle census cases.
func CopyLifecycleEmitter(t testing.TB, variant LifecycleEmitterVariant) string {
	t.Helper()
	root := t.TempDir()
	schema := `name: lifecycle-emitter
stages:
  waiting: {initial: true}
  review:
    gate:
      decision: review_decision
      outcomes:
        approve:
          advances_to: approved
          emit:
            event: work.completed
            fields: {result: {literal: approved}}
  approved: {}
  done: {terminal: true}
pins:
  inputs:
    events:
      - {event: work.requested, source: external}
`
	events := "work.requested:\n  seed: boolean\nwork.completed:\n  result: text\n"
	nodes := `requester:
  id: requester
  execution_type: system_node
  subscribes_to: [work.requested]
  event_handlers:
    work.requested:
      create_entity: true
      advances_to: review
`
	consumer := `collector:
  id: collector
  execution_type: system_node
  subscribes_to: [work.completed]
  event_handlers:
    work.completed:
      data_accumulation:
        writes:
          - target_field: result
            expression: payload.result
      advances_to: done
`
	prefix := ""
	switch variant {
	case LifecycleGateLocal:
		nodes += consumer
	case LifecycleGateDangling:
	case LifecycleGateOutputDisconnected:
		schema += "  outputs:\n    events: [work.completed]\n"
	case LifecycleGateSharedEvent:
		schema = strings.Replace(schema, "  approved: {}", "        reject:\n          advances_to: approved\n          emit:\n            event: work.completed\n            fields: {result: {literal: rejected}}\n  approved: {}", 1)
		nodes += consumer
	case LifecycleGateNoEmit:
		schema = strings.Replace(schema, "          emit:\n            event: work.completed\n            fields: {result: {literal: approved}}\n", "", 1)
	case LifecycleGateNested:
		prefix = "outer/inner/"
		writeClosedVariantFile(t, root, "schema.yaml", "name: lifecycle-parent\n")
		writeClosedVariantFile(t, root, "outer/schema.yaml", "name: lifecycle-middle\n")
		nodes += consumer
	case LifecycleLoopConnected:
		writeLifecycleLoopConnected(t, root)
		return root
	case LifecycleLoopRepeatEmits:
		writeLifecycleLoopConnected(t, root)
		for _, edit := range []struct{ path, old, replacement string }{
			{"schema.yaml", "events: [loop.escaped]", "events: [loop.escaped, ordinary.repeated]"},
			{"schema.yaml", "    to: sink\n", "    to: sink\n  - event: ordinary.repeated\n    from: .\n    to: ordinary\n"},
			{"events.yaml", "work.requested:\n", "ordinary.repeated:\n  token: text\n  revision_id: text\nwork.requested:\n"},
			{"nodes.yaml", "loop: {repeat: revision, from: review}\n      advances_to: drafting", "loop: {repeat: revision, from: review}\n      advances_to: drafting\n      emit:\n        event: ordinary.repeated\n        fields: {token: {literal: ordinary}, revision_id: {cel: loop.revision_id}}"},
		} {
			raw, err := os.ReadFile(filepath.Join(root, edit.path))
			if err != nil {
				t.Fatal(err)
			}
			if strings.Count(string(raw), edit.old) != 1 {
				t.Fatalf("fixture edit %s is not unique", edit.path)
			}
			writeClosedVariantFile(t, root, edit.path, strings.Replace(string(raw), edit.old, edit.replacement, 1))
		}
		writeClosedVariantFile(t, root, "ordinary/schema.yaml", "name: ordinary\nstages:\n  waiting: {initial: true}\n  observed: {terminal: true}\npins:\n  inputs:\n    events: [ordinary.repeated]\n")
		writeClosedVariantFile(t, root, "ordinary/entities.yaml", "receipt:\n  token: text\n  revision_id: text\n")
		writeClosedVariantFile(t, root, "ordinary/nodes.yaml", `collector:
  id: collector
  execution_type: system_node
  subscribes_to: [ordinary.repeated]
  event_handlers:
    ordinary.repeated:
      create_entity: true
      data_accumulation:
        writes:
          - target_field: token
            expression: payload.token
          - target_field: revision_id
            expression: payload.revision_id
      advances_to: observed
`)
		return root
	default:
		t.Fatalf("unsupported lifecycle emitter fixture %d", variant)
	}
	writeClosedVariantFile(t, root, prefix+"schema.yaml", schema)
	writeClosedVariantFile(t, root, prefix+"events.yaml", events)
	writeClosedVariantFile(t, root, prefix+"nodes.yaml", nodes)
	writeClosedVariantFile(t, root, prefix+"entities.yaml", "work:\n  result: text\n")
	return root
}

func writeLifecycleLoopConnected(t testing.TB, root string) {
	writeClosedVariantFile(t, root, "schema.yaml", `name: lifecycle-loop
stages:
  waiting: {initial: true}
  drafting: {}
  review: {}
  escaped: {terminal: true}
  done: {terminal: true}
loops:
  revision:
    revision_field: revision_id
    max_attempts: 2
    escape:
      advances_to: escaped
      emit:
        event: loop.escaped
        fields: {revision_id: {cel: loop.revision_id}}
pins:
  inputs:
    events:
      - {event: work.requested, source: external}
      - {event: loop.start, source: external}
      - {event: loop.admit, source: external}
      - {event: loop.repeat, source: external}
      - {event: loop.close, source: external}
  outputs:
    events: [loop.escaped]
connect:
  - event: loop.escaped
    from: .
    to: sink
`)
	writeClosedVariantFile(t, root, "events.yaml", "work.requested:\n  seed: boolean\nloop.start:\n  seed: boolean\nloop.admit:\n  revision_id: text\nloop.repeat:\n  revision_id: text\nloop.close:\n  revision_id: text\nloop.escaped:\n  revision_id: text\n")
	writeClosedVariantFile(t, root, "entities.yaml", "work: {}\n")
	writeClosedVariantFile(t, root, "nodes.yaml", `controller:
  id: controller
  execution_type: system_node
  subscribes_to: [work.requested, loop.start, loop.admit, loop.repeat, loop.close]
  event_handlers:
    work.requested:
      create_entity: true
      advances_to: waiting
    loop.start:
      loop: {start: revision, from: waiting}
      advances_to: drafting
    loop.admit:
      loop: {admit: revision, from: drafting}
      advances_to: review
    loop.repeat:
      loop: {repeat: revision, from: review}
      advances_to: drafting
    loop.close:
      loop: {close: revision, from: review}
      advances_to: done
`)
	writeClosedVariantFile(t, root, "sink/schema.yaml", `name: sink
stages:
  waiting: {initial: true}
  done: {terminal: true}
pins:
  inputs:
    events: [loop.escaped]
`)
	writeClosedVariantFile(t, root, "sink/entities.yaml", "receipt:\n  revision_id: text\n")
	writeClosedVariantFile(t, root, "sink/nodes.yaml", `collector:
  id: collector
  execution_type: system_node
  subscribes_to: [loop.escaped]
  event_handlers:
    loop.escaped:
      create_entity: true
      data_accumulation:
        writes:
          - target_field: revision_id
            expression: payload.revision_id
      advances_to: done
`)
}
