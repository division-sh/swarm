package contracts

import (
	"fmt"
	"strings"
	"testing"
)

// These profiles are the ruled field contract, independent of projector output.
// M/N/E/S/O/P/Z/Q denote missing/null/empty-text/scalar/empty-map/map/empty-list/list.
func TestSchemaAdmissionFieldPresenceMatrix(t *testing.T) {
	type row struct{ path, template, key, scalar, object, list, admit string }
	root := "name: presence\nstages: []\n%s\n"
	stage := "stages:\n  waiting:\n    final: false\n    description: ''\n    %s\n  done: {final: true}\n"
	gate := "stages:\n  waiting:\n    gate:\n      decision: approval\n      outcomes: {approve: {advances_to: done}}\n      %s\n"
	outcome := "stages:\n  waiting:\n    gate:\n      decision: approval\n      outcomes:\n        approve:\n          advances_to: done\n          %s\n"
	input := "stages:\n  waiting:\n    gate:\n      decision: approval\n      outcomes:\n        approve:\n          advances_to: done\n          input:\n            note:\n              type: text\n              %s\n"
	timer := "stages:\n  waiting:\n    timers:\n      - after: 1s\n        emit: work.expired\n        advances_to: done\n        %s\n"
	loop := "loops:\n  revision:\n    revision_field: revision\n    max_attempts: 3\n    escape: {advances_to: done}\n    %s\n"
	required := "required_agents:\n  - role: worker\n    %s\n"
	connect := "connect:\n  - event: work.requested\n    from: source\n    to: worker\n    %s\n"
	outputPin := "pins:\n  outputs:\n    - event: work.completed\n      %s\n"
	ingress := "ingress:\n  alias: hooks\n  providers: [{provider: partner}]\n  %s\n"
	provider := "ingress:\n  alias: hooks\n  providers:\n    - provider: partner\n      %s\n"
	admission := "ingress:\n  alias: hooks\n  providers:\n    - provider: partner\n      admission:\n        kind: pack\n        pack: {id: partner}\n        %s\n"
	rawAdmission := "ingress:\n  alias: hooks\n  providers:\n    - provider: partner\n      admission:\n        kind: raw\n        authentication: {kind: token, header: X-Token}\n        delivery_id: {source: body_sha256}\n        event: work.received\n        payload: json\n        %s\n"
	authentication := "ingress:\n  alias: hooks\n  providers:\n    - provider: partner\n      admission:\n        kind: raw\n        delivery_id: {source: body_sha256}\n        event: work.received\n        payload: json\n        authentication:\n          kind: hmac_sha256\n          header: X-Signature\n          %s\n"
	delivery := "ingress:\n  alias: hooks\n  providers:\n    - provider: partner\n      admission:\n        kind: raw\n        authentication: {kind: token, header: X-Token}\n        event: work.received\n        payload: json\n        delivery_id:\n          source: header\n          header: X-Id\n          %s\n"
	rows := []row{
		{"name", root, "name", "Example", "", "", "MES"},
		{"mode", root, "mode", "static", "", "", "M"},
		{"instance", root, "instance", "work_id", "", "", "MS"},
		{"stages", root, "stages", "x", "{waiting: {}, done: {final: true}}", "[x]", "MPZ"},
		{"stages.waiting", "stages:\n  %s\n", "waiting", "x", "{final: true}", "[x]", "OP"},
		{"retired.stages.waiting.initial", stage, "initial", "false", "", "", "M"},
		{"retired.stages.waiting.terminal", stage, "terminal", "true", "", "", "M"},
		{"stages.waiting.final", stage, "final", "true", "", "", "MS"},
		{"stages.waiting.description", stage, "description", "Description", "", "", "MES"},
		{"stages.waiting.timers", stage, "timers", "x", "", "[{after: 1s, emit: work.expired}]", "MZQ"},
		{"timer.id", timer, "id", "timeout", "", "", "MS"},
		{"timer.after", timer, "after", "2s", "", "", "S"},
		{"timer.emit", timer, "emit", "work.expired", "", "", "MS"},
		{"timer.advances_to", timer, "advances_to", "done", "", "", "MS"},
		{"stage.gate", stage, "gate", "x", "{decision: approval, outcomes: {approve: {advances_to: done}}}", "", "MP"},
		{"gate.decision", gate, "decision", "approval", "", "", "S"},
		{"gate.title", gate, "title", "Review", "", "", "MES"},
		{"gate.context", gate, "context", "x", "{zero: 0, null: null}", "", "MOP"},
		{"gate.outcomes", gate, "outcomes", "x", "{approve: {advances_to: done}}", "", "P"},
		{"outcome.label", outcome, "label", "Approve", "", "", "MES"},
		{"outcome.input", outcome, "input", "x", "{note: {type: text}}", "", "MOP"},
		{"outcome.advances_to", outcome, "advances_to", "done", "", "", "S"},
		{"outcome.emit", outcome, "emit", "work.completed", "{event: work.completed, fields: {zero: 0}}", "", "MSP"},
		{"input.type", input, "type", "text", "", "", "S"},
		{"input.required", input, "required", "false", "", "", "MS"},
		{"input.label", input, "label", "Note", "", "", "MES"},
		{"loops", root, "loops", "x", "{revision: {revision_field: revision, max_attempts: 3, escape: {advances_to: done}}}", "", "MOP"},
		{"loop.revision_field", loop, "revision_field", "revision", "", "", "S"},
		{"loop.max_attempts", loop, "max_attempts", "10", "", "", "S"},
		{"loop.escape", loop, "escape", "x", "{advances_to: done}", "", "P"},
		{"required_agents", root, "required_agents", "x", "", "[{role: worker}]", "MZQ"},
		{"required.role", required, "role", "worker", "", "", "S"},
		{"required.description", required, "description", "Worker", "", "", "MES"},
		{"required.subscribes_to", required, "subscribes_to", "x", "", "[work.requested]", "MZQ"},
		{"required.emits", required, "emits", "x", "", "[work.completed]", "MZQ"},
		{"retired.instance_variables", root, "instance_variables", "x", "{variables: {note: text}}", "[text]", "M"},
		{"auto_emit", root, "auto_emit_on_create", "x", "{event: work.started}", "", "MP"},
		{"auto_emit.event", "auto_emit_on_create:\n  description: Start\n  %s\n", "event", "work.started", "", "", "S"},
		{"auto_emit.description", "auto_emit_on_create:\n  event: work.started\n  %s\n", "description", "Start", "", "", "MES"},
		{"connect", root, "connect", "x", "", "[{event: work.requested, from: source, to: worker}]", "MZQ"},
		{"connect.event", connect, "event", "work.requested", "", "", "S"},
		{"connect.from", connect, "from", "source", "", "", "S"},
		{"connect.to", connect, "to", "worker", "", "", "S"},
		{"connect.rename", connect, "rename", "work.received", "", "", "MS"},
		{"connect.resolution", connect, "resolution", "select", "", "", "MS"},
		{"connect.replies_to", connect, "replies_to", "work.sent", "", "", "MS"},
		{"connect.correlation_key", strings.ReplaceAll(connect, "    %s", "    replies_to: work.sent\n    %s"), "correlation_key", "work_id", "", "", "MS"},
		{"connect.key_from", strings.ReplaceAll(connect, "    %s", "    resolution: create\n    %s"), "key_from", "event.id", "", "", "MS"},
		{"imports", root, "imports", "x", "{connector_packs: [{provider: telegram, tool: telegram.send_message}]}", "", "MP"},
		{"imports.connector_packs", "imports:\n  %s\n", "connector_packs", "x", "", "[{provider: telegram, tool: telegram.send_message}]", "Q"},
		{"imports.provider_trigger_events", "imports:\n  %s\n", "provider_trigger_events", "x", "", "[{provider: telegram, event: inbound.telegram.text_message}]", "Q"},
		{"import.connector.provider", "imports:\n  connector_packs:\n    - tool: telegram.send_message\n      %s\n", "provider", "telegram", "", "", "S"},
		{"import.connector.tool", "imports:\n  connector_packs:\n    - provider: telegram\n      %s\n", "tool", "telegram.send_message", "", "", "S"},
		{"import.trigger.provider", "imports:\n  provider_trigger_events:\n    - event: inbound.telegram.text_message\n      %s\n", "provider", "telegram", "", "", "S"},
		{"import.trigger.event", "imports:\n  provider_trigger_events:\n    - provider: telegram\n      %s\n", "event", "inbound.telegram.text_message", "", "", "S"},
		{"ingress", root, "ingress", "x", "{alias: hooks, providers: [{provider: partner}]}", "", "MP"},
		{"ingress.alias", ingress, "alias", "hooks", "", "", "MS"},
		{"ingress.providers", ingress, "providers", "x", "", "[{provider: partner}]", "Q"},
		{"provider.provider", provider, "provider", "partner", "", "", "S"},
		{"provider.signing_secret", provider, "signing_secret", "TOKEN", "", "", "MS"},
		{"provider.admission", provider, "admission", "x", "{kind: pack, pack: {id: partner}}", "", "MP"},
		{"admission.kind", admission, "kind", "pack", "", "", "MS"},
		{"admission.acknowledge", admission, "acknowledge", "empty", "", "", "MS"},
		{"admission.pack", admission, "pack", "x", "{id: partner}", "", "MP"},
		{"admission.pack.id", "ingress:\n  alias: hooks\n  providers:\n    - provider: partner\n      admission:\n        pack:\n          %s\n", "id", "partner", "", "", "S"},
		{"admission.authentication", rawAdmission, "authentication", "x", "{kind: token, header: X-Token}", "", "P"},
		{"admission.event", rawAdmission, "event", "work.received", "", "", "S"},
		{"admission.payload", rawAdmission, "payload", "json", "", "", "S"},
		{"admission.delivery_id", rawAdmission, "delivery_id", "x", "{source: body_sha256}", "", "P"},
		{"authentication.kind", authentication, "kind", "token", "", "", "S"},
		{"authentication.header", authentication, "header", "X-Signature", "", "", "S"},
		{"authentication.prefix", authentication, "prefix", "Token", "", "", "MES"},
		{"authentication.encoding", authentication, "encoding", "hex", "", "", "MS"},
		{"delivery.source", delivery, "source", "header", "", "", "S"},
		{"delivery.header", delivery, "header", "X-Id", "", "", "S"},
		{"delivery.json_path", strings.ReplaceAll(delivery, "source: header\n          header: X-Id", "source: json_path"), "json_path", "$.id", "", "", "S"},
		{"pins", root, "pins", "x", "{inputs: [work.requested]}", "", "MP"},
		{"pins.inputs", "pins:\n  outputs: [work.completed]\n  %s\n", "inputs", "x", "{events: [work.requested]}", "[work.requested]", "MQ"},
		{"pins.outputs", "pins:\n  inputs: [work.requested]\n  %s\n", "outputs", "x", "{events: [work.completed]}", "[work.completed]", "MQ"},
		{"retired.pins.inputs.events", "pins:\n  inputs:\n    %s\n", "events", "x", "", "[work.requested]", ""},
		{"retired.pins.outputs.events", "pins:\n  outputs:\n    %s\n", "events", "x", "", "[work.completed]", ""},
		{"retired.pins.inputs.reads", "pins:\n  inputs:\n    events: [work.requested]\n    %s\n", "reads", "x", "", "[note]", ""},
		{"retired.pins.outputs.writes", "pins:\n  outputs:\n    events: [work.completed]\n    %s\n", "writes", "x", "", "[note]", ""},
		{"retired.outputPin.event", outputPin, "event", "work.completed", "", "", ""},
		{"retired.outputPin.sink", outputPin, "sink", "harness", "", "", ""},
	}
	for _, tc := range rows {
		t.Run(tc.path, func(t *testing.T) {
			// Remove the fixture's original spelling of the varied field, not its siblings.
			template := tc.template
			lines := strings.Split(template, "\n")
			fieldIndent := 0
			for _, line := range lines {
				if strings.Contains(line, "%s") {
					fieldIndent = len(line) - len(strings.TrimLeft(line, " "))
				}
			}
			for i, line := range lines {
				trimmed := strings.TrimSpace(line)
				indent := len(line) - len(strings.TrimLeft(line, " "))
				if strings.Contains(line, "%s") {
					continue
				}
				if indent == fieldIndent && strings.HasPrefix(trimmed, tc.key+":") {
					lines[i] = ""
				}
				if indent+2 == fieldIndent && strings.HasPrefix(trimmed, "- "+tc.key+":") {
					lines[i] = line[:strings.Index(line, "-")] + "-"
				}
			}
			template = strings.Join(lines, "\n")
			values := map[byte]string{'M': "", 'N': "null", 'E': "''", 'S': tc.scalar, 'O': "{}", 'P': tc.object, 'Z': "[]", 'Q': tc.list}
			for _, state := range "MNESOPZQ" {
				t.Run(string(state), func(t *testing.T) {
					value := values[byte(state)]
					if value == "" && state != 'M' {
						if state == 'P' {
							value = "{unknown: value}"
						} else if state == 'Q' {
							value = "[unknown]"
						} else {
							value = "value"
						}
					}
					field := ""
					if state != 'M' {
						field = tc.key + ": " + value
					}
					source := fmt.Sprintf(template, field)
					_, err := admitSchemaFragment(source)
					want := strings.ContainsRune(tc.admit, state)
					if (err == nil) != want {
						t.Fatalf("state %c expected admission=%v: %v\n%s", state, want, err, source)
					}
					if err != nil && !strings.Contains(err.Error(), "schema.yaml:") {
						t.Fatalf("diagnostic lost source coordinates: %v", err)
					}
				})
			}
		})
	}
}

func TestSchemaAdmissionBooleanKindsAndLiteralDefaultPresence(t *testing.T) {
	for _, key := range []string{"subscribes_to", "emits"} {
		for _, raw := range []string{"null", "''", "false", "0", "1.5", "' work.requested '", "{}", "[]"} {
			if _, err := admitSchemaFragment("required_agents: [{role: worker, " + key + ": [" + raw + "]}]\n"); err == nil {
				t.Fatalf("required agent %s admitted noncanonical event %s", key, raw)
			}
		}
	}
	for _, key := range []string{"initial", "terminal"} {
		for _, raw := range []string{"'true'", "yes", "1", "null", "{}", "[]"} {
			if _, err := admitSchemaFragment("stages: {waiting: {" + key + ": " + raw + "}}\n"); err == nil {
				t.Fatalf("%s admitted %s", key, raw)
			}
		}
	}
}

func TestSchemaAdmissionIngressBranchPresenceMatrix(t *testing.T) {
	type branch struct {
		name, template string
		forbidden      []string
	}
	provider := "ingress:\n  alias: hooks\n  providers:\n    - provider: partner\n      admission:\n        %s\n"
	raw := "kind: raw\n        event: work.received\n        payload: json\n        authentication: {kind: token, header: X-Token}\n        delivery_id: {source: body_sha256}\n        %s"
	branches := []branch{
		{"pack", fmt.Sprintf(provider, "kind: pack\n        pack: {id: partner}\n        %s"), []string{"authentication", "event", "delivery_id", "payload"}},
		{"raw", fmt.Sprintf(provider, raw), []string{"pack"}},
		{"none", fmt.Sprintf(provider, "kind: raw\n        event: work.received\n        payload: json\n        delivery_id: {source: body_sha256}\n        authentication:\n          kind: none\n          %s"), []string{"header", "prefix", "encoding"}},
		{"token", fmt.Sprintf(provider, "kind: raw\n        event: work.received\n        payload: json\n        delivery_id: {source: body_sha256}\n        authentication:\n          kind: token\n          header: X-Token\n          %s"), []string{"encoding"}},
	}
	for _, delivery := range []struct{ mode, required, forbidden string }{{"header", "header: X-Id", "json_path"}, {"json_path", "json_path: $.id", "header"}, {"body_sha256", "", "header"}, {"body_sha256", "", "json_path"}} {
		branches = append(branches, branch{"delivery-" + delivery.mode + "-" + delivery.forbidden, fmt.Sprintf(provider, "kind: raw\n        event: work.received\n        payload: json\n        authentication: {kind: token, header: X-Token}\n        delivery_id:\n          source: "+delivery.mode+"\n          "+delivery.required+"\n          %s"), []string{delivery.forbidden}})
	}
	for _, b := range branches {
		for _, key := range b.forbidden {
			t.Run(b.name+"/"+key, func(t *testing.T) {
				for _, raw := range []string{"null", "''", "false", "value", "{}", "{a: b}", "[]", "[a]"} {
					_, err := admitSchemaFragment(fmt.Sprintf(b.template, key+": "+raw))
					if err == nil || !strings.Contains(err.Error(), key) {
						t.Fatalf("forbidden %s=%s: %v", key, raw, err)
					}
				}
				if _, err := admitSchemaFragment(fmt.Sprintf(b.template, "")); err != nil {
					t.Fatalf("valid omitted branch: %v", err)
				}
			})
		}
	}
}

func TestSchemaAdmissionResolutionPresenceDoesNotBypassMode(t *testing.T) {
	for _, mode := range []string{"create", "select", "select-or-create", "fan-in", "fan-out", "reply"} {
		for _, key := range []string{"from", "aggregation", "window", "dedup_by", "singleton", "replies_to", "correlation_key"} {
			t.Run(mode+"/"+key, func(t *testing.T) {
				for _, raw := range []string{"null", "''", "{}", "[]"} {
					source := "pins: {inputs: [{event: work.requested, resolution: {mode: " + mode + ", " + key + ": " + raw + "}}]}\n"
					if _, err := admitSchemaFragment(source); err == nil {
						t.Fatalf("mode %s admitted explicit invalid %s=%s", mode, key, raw)
					}
				}
				value := "value"
				if key == "from" {
					value = "payload.work_id"
				}
				if key == "aggregation" {
					value = "stream"
				}
				if key == "dedup_by" {
					value = "[work_id]"
				}
				if key == "replies_to" {
					value = "work.requested"
				}
				_, err := admitSchemaFragment("pins: {inputs: [{event: work.requested, resolution: {mode: " + mode + ", " + key + ": " + value + "}}]}\n")
				if err == nil || !strings.Contains(err.Error(), `must be a scalar text, got mapping`) || !strings.Contains(err.Error(), `["inputs"][0]`) {
					t.Fatalf("retired pin mode %s %s was not rejected: %v", mode, key, err)
				}
			})
		}
	}
}
