package canonicalrouting

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type LifecycleEmitterStaticVariant uint8

const (
	LifecycleStaticLoopLocal LifecycleEmitterStaticVariant = iota
	LifecycleStaticLoopDangling
	LifecycleStaticLoopNoEmit
	LifecycleStaticGateTwoGates
	LifecycleStaticGateSharedDangling
	LifecycleStaticScopeMatrix
	LifecycleStaticWildcardScope
	LifecycleStaticForeignOnly
	LifecycleStaticMixedOutput
	LifecycleStaticMixedDisconnected
	LifecycleStaticActorCollision
	LifecycleStaticHandlerAssertion
	LifecycleStaticGateUnknownEvent
	LifecycleStaticGateBadField
	LifecycleStaticGateReserved
	LifecycleStaticLoopReserved
	LifecycleStaticGateExternalProof
	LifecycleStaticTimerCollision
	LifecycleStaticLoopScopeMatrix
	LifecycleStaticGateLoopShared
	LifecycleStaticLoopLocalNodeInvalid
	LifecycleStaticGateWrongFlow
	LifecycleStaticLoopUnknownEvent
	LifecycleStaticLoopBadField
	LifecycleStaticLoopBadCap
	LifecycleStaticTwoLoopsShared
	LifecycleStaticTimerSelfOnly
	LifecycleStaticGateDanglingStrict
	LifecycleStaticHandlerCycle
	LifecycleStaticHandlerSelfCycle
	LifecycleStaticLoopWrongFlow
	LifecycleStaticGateMissingField
	LifecycleStaticLoopMissingField
	LifecycleStaticGateMalformed
	LifecycleStaticLoopMalformed
)

// CopyLifecycleEmitterStatic owns the finite C1 source variants, separately from
// the served lifecycle journey fixtures. Callers cannot inject routing YAML.
func CopyLifecycleEmitterStatic(t testing.TB, variant LifecycleEmitterStaticVariant) string {
	t.Helper()
	base := LifecycleGateLocal
	switch variant {
	case LifecycleStaticLoopLocal, LifecycleStaticLoopDangling, LifecycleStaticLoopNoEmit, LifecycleStaticLoopReserved, LifecycleStaticLoopScopeMatrix, LifecycleStaticGateLoopShared, LifecycleStaticLoopLocalNodeInvalid, LifecycleStaticLoopUnknownEvent, LifecycleStaticLoopBadField, LifecycleStaticLoopBadCap, LifecycleStaticTwoLoopsShared, LifecycleStaticLoopWrongFlow, LifecycleStaticLoopMissingField, LifecycleStaticLoopMalformed:
		base = LifecycleLoopConnected
	case LifecycleStaticGateTwoGates, LifecycleStaticGateSharedDangling:
		base = LifecycleGateSharedEvent
	case LifecycleStaticMixedOutput, LifecycleStaticMixedDisconnected, LifecycleStaticGateWrongFlow, LifecycleStaticGateDanglingStrict:
		base = LifecycleGateDangling
	case LifecycleStaticTimerSelfOnly:
		base = LifecycleGateNoEmit
	}
	root := CopyLifecycleEmitter(t, base)
	schema := lifecycleStaticRead(t, root, "schema.yaml")
	events := lifecycleStaticRead(t, root, "events.yaml")
	nodes := lifecycleStaticRead(t, root, "nodes.yaml")
	switch variant {
	case LifecycleStaticLoopLocal, LifecycleStaticLoopDangling, LifecycleStaticLoopNoEmit, LifecycleStaticLoopLocalNodeInvalid:
		schema = strings.Split(schema, "  outputs:\n")[0]
		removeClosedVariantFiles(t, root, "sink/schema.yaml", "sink/nodes.yaml", "sink/entities.yaml")
		if err := os.Remove(filepath.Join(root, "sink")); err != nil {
			t.Fatal(err)
		}
		if variant == LifecycleStaticLoopLocal {
			writeClosedVariantFile(t, root, "agents.yaml", "collector:\n  role: collector\n  intent: prompts/collector.md\n  subscriptions: [loop.escaped]\n")
			writeClosedVariantFile(t, root, "prompts/collector.md", "Observe the completed loop escape.\n")
		}
		if variant == LifecycleStaticLoopLocalNodeInvalid {
			nodes += "collector:\n  id: collector\n  execution_type: system_node\n  subscribes_to: [loop.escaped]\n  event_handlers:\n    loop.escaped: {}\n"
		}
		if variant == LifecycleStaticLoopNoEmit {
			schema = strings.Replace(schema, "      emit:\n        event: loop.escaped\n        fields: {revision_id: {cel: loop.revision_id}}\n", "", 1)
		}
	case LifecycleStaticGateTwoGates:
		gate := schema[strings.Index(schema, "  review:\n"):strings.Index(schema, "  approved: {}")]
		gate = strings.Replace(gate, "  review:\n", "  second_review:\n", 1)
		gate = strings.ReplaceAll(gate, "review_decision", "second_decision")
		schema = strings.Replace(schema, "  approved: {}", gate+"  approved: {}", 1)
	case LifecycleStaticGateSharedDangling:
		nodes = strings.Split(nodes, "collector:\n")[0]
	case LifecycleStaticScopeMatrix, LifecycleStaticWildcardScope, LifecycleStaticForeignOnly:
		if variant == LifecycleStaticWildcardScope {
			nodes = strings.ReplaceAll(nodes, "[work.completed]", "[work.*]")
			nodes = strings.ReplaceAll(nodes, "    work.completed:\n", "    work.*:\n")
		}
		for _, scope := range []string{"left", "right", "left/deep"} {
			writeClosedVariantFile(t, root, scope+"/schema.yaml", schema)
			writeClosedVariantFile(t, root, scope+"/events.yaml", events)
			writeClosedVariantFile(t, root, scope+"/nodes.yaml", nodes)
			writeClosedVariantFile(t, root, scope+"/entities.yaml", lifecycleStaticRead(t, root, "entities.yaml"))
		}
		if variant == LifecycleStaticForeignOnly {
			nodes = strings.Split(nodes, "collector:\n")[0]
		}
	case LifecycleStaticMixedOutput, LifecycleStaticMixedDisconnected:
		// A real handler outcome shares the gate event. This is a mixed-site
		// counterexample, not an extra producer to satisfy a gate-only fixture.
		nodes += "worker:\n  id: worker\n  execution_type: system_node\n  subscribes_to: [work.handled]\n  event_handlers:\n    work.handled:\n      emit:\n        event: work.completed\n        fields: {result: {literal: handled}}\n"
		events += "work.handled: {}\n"
		schema += "      - {event: work.handled, source: external}\n"
		schema += "  outputs:\n    events: [work.completed]\n"
		if variant == LifecycleStaticMixedOutput {
			schema += "connect:\n  - {event: work.completed, from: ., to: sink}\n"
			writeClosedVariantFile(t, root, "sink/schema.yaml", "name: sink\npins:\n  inputs:\n    events: [work.completed]\n")
			writeClosedVariantFile(t, root, "sink/nodes.yaml", "collector:\n  id: collector\n  execution_type: system_node\n  subscribes_to: [work.completed]\n  event_handlers:\n    work.completed: {}\n")
		}
	case LifecycleStaticActorCollision:
		nodes = strings.ReplaceAll(nodes, "requester", "review_decision")
		writeClosedVariantFile(t, root, "agents.yaml", "reviewer:\n  role: review_decision\n  intent: prompts/reviewer.md\n  subscriptions: [work.requested]\n")
		writeClosedVariantFile(t, root, "prompts/reviewer.md", "Observe work requests.\n")
	case LifecycleStaticHandlerAssertion:
		nodes = strings.Replace(nodes, "  id: requester\n", "  id: requester\n  produces: [work.completed]\n", 1)
	case LifecycleStaticGateUnknownEvent:
		schema = strings.Replace(schema, "event: work.completed", "event: absent.event", 1)
	case LifecycleStaticGateBadField:
		schema = strings.Replace(schema, "fields: {result:", "fields: {unknown:", 1)
	case LifecycleStaticGateReserved:
		schema = strings.Replace(schema, "event: work.completed", "event: platform.stage_timer", 1)
	case LifecycleStaticLoopReserved:
		schema = strings.Replace(schema, "event: loop.escaped", "event: platform.stage_timer", 1)
	case LifecycleStaticGateExternalProof:
		events = strings.Replace(events, "work.requested:\n", "work.requested:\n  swarm: {source: external}\n", 1)
	case LifecycleStaticTimerCollision, LifecycleStaticTimerSelfOnly:
		schema = strings.Replace(schema, "  review:\n", "  review:\n    timers:\n      - {id: reminder, after: 1h, emit: work.completed}\n", 1)
	case LifecycleStaticLoopScopeMatrix:
		for _, scope := range []string{"left", "right", "left/deep"} {
			for _, file := range []string{"schema.yaml", "events.yaml", "nodes.yaml", "entities.yaml", "sink/schema.yaml", "sink/nodes.yaml", "sink/entities.yaml"} {
				writeClosedVariantFile(t, root, scope+"/"+file, lifecycleStaticRead(t, root, file))
			}
		}
	case LifecycleStaticGateLoopShared:
		schema = strings.Replace(schema, "  waiting: {initial: true}", "  waiting:\n    initial: true\n    gate:\n      decision: review_decision\n      outcomes:\n        approve:\n          advances_to: escaped\n          emit:\n            event: loop.escaped\n            fields: {revision_id: {literal: gate}}", 1)
	case LifecycleStaticGateWrongFlow:
		events = "work.requested:\n  seed: boolean\n"
		writeClosedVariantFile(t, root, "child/schema.yaml", "name: child\n")
		writeClosedVariantFile(t, root, "child/events.yaml", "work.completed:\n  result: text\n")
	case LifecycleStaticGateDanglingStrict:
		// Unlike the journey shell with its removed collector, this minimal
		// dangling contract has no unrelated unused state or entity fields.
		schema = strings.Replace(schema, "  approved: {}\n  done: {terminal: true}", "  approved: {terminal: true}", 1)
		writeClosedVariantFile(t, root, "entities.yaml", "work: {}\n")
	case LifecycleStaticLoopUnknownEvent:
		schema = strings.Replace(schema, "event: loop.escaped", "event: absent.event", 1)
	case LifecycleStaticLoopBadField:
		schema = strings.Replace(schema, "fields: {revision_id:", "fields: {unknown:", 1)
	case LifecycleStaticLoopBadCap:
		schema = strings.Replace(schema, "max_attempts: 2", "max_attempts: 0", 1)
	case LifecycleStaticTwoLoopsShared:
		declaration := schema[strings.Index(schema, "  revision:\n"):strings.Index(schema, "pins:\n")]
		second := strings.NewReplacer("  revision:\n", "  second:\n", "revision_id", "second_revision_id").Replace(declaration)
		// Both sites share an event schema but carry their own required revision
		// field from the active loop, never from the other loop's state.
		second = strings.Replace(second, "loop.second_revision_id", "loop.revision_id", 1)
		second = strings.Replace(second, "fields: {", "fields: {revision_id: {literal: second}, ", 1)
		schema = strings.Replace(schema, declaration, strings.Replace(declaration, "fields: {", "fields: {second_revision_id: {literal: first}, ", 1)+second, 1)
		schema = strings.Replace(schema, "  drafting: {}", "  drafting: {}\n  drafting_second: {}\n  review_second: {}", 1)
		for _, event := range []string{"start", "admit", "repeat", "close"} {
			schema = strings.Replace(schema, "  outputs:\n", "      - {event: second."+event+", source: external}\n  outputs:\n", 1)
		}
		events = strings.Replace(events, "loop.escaped:\n", "loop.escaped:\n  second_revision_id: text\n", 1)
		events += "second.start:\n  seed: boolean\nsecond.admit:\n  second_revision_id: text\nsecond.repeat:\n  second_revision_id: text\nsecond.close:\n  second_revision_id: text\n"
		operations := nodes[strings.Index(nodes, "    loop.start:\n"):]
		operations = strings.NewReplacer("loop.", "second.", "revision_id", "second_revision_id", ": revision", ": second", "drafting", "drafting_second", "review", "review_second").Replace(operations)
		nodes += "second-controller:\n  id: second-controller\n  execution_type: system_node\n  subscribes_to: [second.start, second.admit, second.repeat, second.close]\n  event_handlers:\n" + operations
	case LifecycleStaticHandlerCycle:
		// The handler chain closes independently of the gate's shared output.
		nodes = strings.Replace(nodes, "      advances_to: review\n", "      advances_to: review\n      emit: {event: work.completed, fields: {result: {literal: handled}}}\n", 1)
		nodes = strings.Replace(nodes, "      advances_to: done\n", "      advances_to: done\n      emit: {event: work.requested, fields: {seed: {literal: true}}}\n", 1)
	case LifecycleStaticHandlerSelfCycle:
		nodes = strings.Replace(nodes, "      advances_to: done\n", "      advances_to: done\n      emit: {event: work.completed, fields: {result: {literal: repeated}}}\n", 1)
	case LifecycleStaticLoopWrongFlow:
		events = strings.Replace(events, "loop.escaped:\n  revision_id: text\n", "", 1)
		writeClosedVariantFile(t, root, "foreign/schema.yaml", "name: foreign\n")
		writeClosedVariantFile(t, root, "foreign/events.yaml", "loop.escaped:\n  revision_id: text\n")
	case LifecycleStaticGateMissingField:
		schema = strings.Replace(schema, "fields: {result: {literal: approved}}", "fields: {}", 1)
	case LifecycleStaticLoopMissingField:
		schema = strings.Replace(schema, "fields: {revision_id: {cel: loop.revision_id}}", "fields: {}", 1)
	case LifecycleStaticGateMalformed:
		schema = strings.Replace(schema, "decision: review_decision", "decision: review_decision\n      unknown_gate_field: true", 1)
	case LifecycleStaticLoopMalformed:
		schema = strings.Replace(schema, "max_attempts: 2", "max_attempts: 2\n    unknown_loop_field: true", 1)
	default:
		t.Fatalf("unsupported lifecycle static fixture %d", variant)
	}
	writeClosedVariantFile(t, root, "schema.yaml", schema)
	writeClosedVariantFile(t, root, "events.yaml", events)
	writeClosedVariantFile(t, root, "nodes.yaml", nodes)
	return root
}

func lifecycleStaticRead(t testing.TB, root, file string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(root, file))
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

// RewriteLifecycleEmitterAmbientSource removes the live gate after admission.
// The previously loaded bundle must remain the only census authority.
func RewriteLifecycleEmitterAmbientSource(t testing.TB, root string) {
	t.Helper()
	writeClosedVariantFile(t, root, "schema.yaml", "name: replaced-after-admission\n")
}

// CopyLifecycleEmitterHandlerFamilies adds genuine handler outcomes in a
// separate flow so the gate remains its event's only producer.
func CopyLifecycleEmitterHandlerFamilies(t testing.TB) string {
	t.Helper()
	root := CopyLifecycleEmitter(t, LifecycleGateLocal)
	writeClosedVariantFile(t, root, "families/schema.yaml", "name: handler-families\n")
	events := "direct: {}\nrouted: {}\naudit: {}\nescalated: {}\nspecialized:\n  bucket: text\nitem:\n  id: text\ncommit.ok: {}\ncommit.failed: {}\n"
	nodes := `worker:
  id: worker
  execution_type: system_node
  event_handlers:
    request:
      emit: direct
      guard:
        id: guard
        check: "payload.score > 0"
        on_fail: "escalate:escalated"
router:
  id: router
  execution_type: system_node
  event_handlers:
    request:
      on_success: {emit: audit}
      rules:
        routed: {condition: "else", emit: routed}
template:
  id: template
  execution_type: system_node
  event_handlers:
    request:
      emit: {event: specialized}
      rules:
        high:
          condition: "payload.score > 0"
          emit: {fields: {bucket: '"high"'}}
        low:
          condition: "else"
          emit: {fields: {bucket: '"low"'}}
dispatcher:
  id: dispatcher
  execution_type: system_node
  event_handlers:
    request:
      fan_out:
        items_from: payload.items
        as: element
        identity: element
        emit: {event: item, fields: {id: {cel: element}}}
rule-dispatcher:
  id: rule-dispatcher
  execution_type: system_node
  event_handlers:
    request:
      rules:
        dispatch:
          condition: "else"
          fan_out:
            items_from: payload.items
            as: element
            identity: element
            emit: {event: item, fields: {id: {cel: element}}}
completion:
  id: completion
  execution_type: system_node
  event_handlers:
    request:
      on_complete:
        - id: completed
          condition: "else"
          emit: direct
completion-dispatcher:
  id: completion-dispatcher
  execution_type: system_node
  event_handlers:
    request:
      on_complete:
        - id: dispatch
          condition: "else"
          fan_out:
            items_from: payload.items
            as: element
            identity: element
            emit: {event: item, fields: {id: {cel: element}}}
committer:
  id: committer
  execution_type: system_node
  event_handlers:
    request:
      action:
        id: artifact_repo_commit
        artifact_repo:
          success_event: commit.ok
          failure_event: commit.failed
`
	for _, node := range []string{"worker", "router", "template", "dispatcher", "rule-dispatcher", "completion", "completion-dispatcher", "committer"} {
		request := "request." + strings.ReplaceAll(node, "-", "_")
		events += request + ":\n  score: integer\n  items: \"[text]\"\n"
		nodes = strings.Replace(nodes, "    request:\n", "    "+request+":\n", 1)
	}
	writeClosedVariantFile(t, root, "families/events.yaml", events)
	writeClosedVariantFile(t, root, "families/nodes.yaml", nodes)
	return root
}

// CopyLifecycleEmitterActivityOutcomes uses actual compiled activity sites with
// proposed-effect approval, preserving the existing generated-schema owner.
func CopyLifecycleEmitterActivityOutcomes(t testing.TB, nested bool) string {
	t.Helper()
	root := CopyGeneratedActivity(t, nested, !nested)
	prefix := ""
	if nested {
		prefix = "child/"
	}
	nodes := lifecycleStaticRead(t, root, prefix+"nodes.yaml")
	nodes = strings.Replace(nodes, "        tool: send\n", "        tool: send\n        approval: {decision: approve_send}\n", 1)
	nodes += "    send.revision_requested: {}\n    send.rejected: {}\n"
	writeClosedVariantFile(t, root, prefix+"nodes.yaml", nodes)
	tool := lifecycleStaticRead(t, root, prefix+"tools.yaml")
	writeClosedVariantFile(t, root, prefix+"tools.yaml", strings.Replace(tool, "effect_class: read_only", "effect_class: non_idempotent_write", 1))
	return root
}

func CopyLifecycleEmitterExistingLifecycleFamilies(t testing.TB) string {
	t.Helper()
	root := CopyServedJoinProof(t)
	nodes := lifecycleStaticRead(t, root, "nodes.yaml")
	nodes = strings.Replace(nodes, "on_complete: {advances_to: ready}", "on_complete: {advances_to: ready, emit: join.completed}", 1)
	nodes = strings.Replace(nodes, "timeout: {after: 1h, advances_to: attention}", "timeout: {after: 1h, advances_to: attention, emit: join.expired}", 1)
	writeClosedVariantFile(t, root, "nodes.yaml", nodes)
	events := lifecycleStaticRead(t, root, "events.yaml") + "join.completed: {}\njoin.expired: {}\ncreated: {}\nreminder: {}\nexpired: {}\n"
	writeClosedVariantFile(t, root, "events.yaml", events)
	schema := lifecycleStaticRead(t, root, "schema.yaml")
	schema = strings.Replace(schema, "  dispatching: {}", "  dispatching:\n    timers:\n      - {id: reminder, after: 1h, emit: reminder}\n      - {id: expired, after: 2h, emit: expired, advances_to: attention}\n      - {id: internal, after: 3h, advances_to: attention}", 1)
	schema += "auto_emit_on_create: {event: created}\n"
	writeClosedVariantFile(t, root, "schema.yaml", schema)
	writeClosedVariantFile(t, root, "child/schema.yaml", "name: child\nauto_emit_on_create: {event: created}\n")
	writeClosedVariantFile(t, root, "child/events.yaml", "created: {}\n")
	return root
}

type LifecycleEmitterMetadataCoordinate uint8

const (
	LifecycleMetadataStage LifecycleEmitterMetadataCoordinate = iota
	LifecycleMetadataVerdict
)

func CopyLifecycleEmitterMetadataCoordinate(t testing.TB, coordinate LifecycleEmitterMetadataCoordinate, field string) string {
	t.Helper()
	root := CopyLifecycleEmitter(t, LifecycleGateLocal)
	role := ""
	switch coordinate {
	case LifecycleMetadataStage:
		role = "review"
	case LifecycleMetadataVerdict:
		role = "approve"
	default:
		t.Fatalf("unsupported lifecycle coordinate %d", coordinate)
	}
	value := role
	switch field {
	case "source":
	case "producer", "consumer":
		value = "[" + role + "]"
	default:
		t.Fatalf("unsupported lifecycle metadata field %q", field)
	}
	events := lifecycleStaticRead(t, root, "events.yaml")
	events = strings.Replace(events, "work.completed:\n", "work.completed:\n  swarm:\n    "+field+": "+value+"\n", 1)
	writeClosedVariantFile(t, root, "events.yaml", events)
	return root
}

// Human decisions publish fixed platform events; they are not authored HTTP
// activity result sites and must not acquire a fabricated node producer.
func CopyLifecycleEmitterHumanTaskObserver(t testing.TB, nested bool) string {
	t.Helper()
	root := CopyLifecycleEmitter(t, LifecycleGateLocal)
	prefix := ""
	if nested {
		prefix = "child/"
		writeClosedVariantFile(t, root, prefix+"schema.yaml", "name: human-observer\n")
	}
	writeClosedVariantFile(t, root, prefix+"agents.yaml", `operator:
  role: operator
  intent: prompts/operator.md
  permissions: [ask_human]
  subscriptions: [human_task.approved, human_task.rejected, human_task.deferred, human_task.expired]
`)
	writeClosedVariantFile(t, root, prefix+"prompts/operator.md", "Request and observe human decisions.\n")
	return root
}

func CopyLifecycleEmitterRuleActivityOutcomes(t testing.TB, nested bool) string {
	t.Helper()
	root := CopyLifecycleEmitterActivityOutcomes(t, nested)
	prefix := ""
	if nested {
		prefix = "child/"
	}
	nodes := lifecycleStaticRead(t, root, prefix+"nodes.yaml")
	start := strings.Index(nodes, "      activity:\n")
	if start < 0 {
		t.Fatal("activity fixture site missing")
	}
	end := strings.Index(nodes[start:], "    send.")
	if observer := strings.Index(nodes[start:], "observer-node:"); observer >= 0 && (end < 0 || observer < end) {
		end = observer
	}
	if end < 0 {
		t.Fatal("activity fixture site missing")
	}
	end += start
	activity := strings.TrimSuffix(nodes[start:end], "\n")
	activity = "    " + strings.ReplaceAll(activity, "\n", "\n    ") + "\n"
	nodes = nodes[:start] + "      rules:\n        send_rule:\n          condition: \"else\"\n" + activity + nodes[end:]
	writeClosedVariantFile(t, root, prefix+"nodes.yaml", nodes)
	return root
}

// CopyLifecycleEmitterImportedApprovalReferences retains the actual release
// source's imported connector and adds the other generated approval subscriber.
func CopyLifecycleEmitterImportedApprovalReferences(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	copyTree(t, filepath.Join(RepoRoot(t), "internal/releasee2e/testdata/full_lifecycle/standing_telegram"), root)
	nodes := lifecycleStaticRead(t, root, "telegram-chat/nodes.yaml")
	nodes += "telegram-rejected:\n  execution_type: system_node\n  subscribes_to: [telegram_send_message.rejected]\n  event_handlers:\n    telegram_send_message.rejected: {}\n"
	writeClosedVariantFile(t, root, "telegram-chat/nodes.yaml", nodes)
	return root
}
