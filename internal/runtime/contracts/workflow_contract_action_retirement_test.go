package contracts

import (
	"fmt"
	"strings"
	"testing"
)

func requireRetiredHandlerAction(t *testing.T, err error, key string) {
	t.Helper()
	diagnostic, ok := AsLoaderDiagnostic(err)
	if !ok || diagnostic.Code != "contract_loader.undefined_field" || !strings.Contains(diagnostic.Problem, fmt.Sprintf("%q", key)) || len(diagnostic.ValidOptions) == 0 || diagnostic.Location.YAMLPath == "" || diagnostic.Location.Line < 1 || diagnostic.Location.Column < 1 {
		t.Fatalf("error = %v, want located current-vocabulary rejection of %q", err, key)
	}
}

func TestRetiredHandlerActionsRejectedOnPresence(t *testing.T) {
	for _, value := range []string{
		"create_flow_instance", "record_evidence", "mailbox_write", "artifact_repo_commit", "unknown_action",
		"null", "''", "{}", "[]", "false", "{id: create_flow_instance, template: child, config_from: {name: payload.name}}",
		"{id: mailbox_write, mailbox: {summary: ready}}", "{id: artifact_repo_commit, artifact_repo: {provider: local_git}}",
		"{type: create_flow_instance, flow_template: child, instance_id: payload.id}",
	} {
		t.Run(value, func(t *testing.T) {
			var handler SystemNodeEventHandler
			err := decodeNodeTestYAML([]byte("action: "+value+"\n"), &handler)
			requireRetiredHandlerAction(t, err, "action")
		})
	}
}

func TestRetiredHandlerActionOptionsRejected(t *testing.T) {
	contexts := []struct{ name, pattern string }{
		{"handler", "%s: %s\n"},
		{"rules_sequence", "rules: [{else: true, %s: %s}]\n"},
		{"rules_singleton", "rules: {else: true, %s: %s}\n"},
		{"rules_lone_field", "rules: {%s: %s}\n"},
		{"rules_keyed", "rules: {chosen: {else: true, %s: %s}}\n"},
		{"completion", "on_complete: [{condition: true, %s: %s}]\n"},
		{"success", "on_success: {%s: %s}\n"},
		{"join", "join: {%s: %s}\n"},
		{"join_completion", "join: {stage: waiting, members: {from: state.ids, by: payload.id}, output: payload.result, on_complete: {%s: %s}}\n"},
		{"join_deadline", "join: {stage: waiting, members: {from: state.ids, by: payload.id}, output: payload.result, on_complete: {advances_to: done}, deadline: {after: 1h, from: stage_entry}, on_deadline: {%s: %s}}\n"},
	}
	for _, site := range contexts {
		for _, key := range []string{"action", "evidence_target", "template", "instance_id_from", "config_from"} {
			for _, value := range []string{"null", "''", "{}", "[]", "false", "{nested: value}", "payload.id"} {
				t.Run(site.name+"/"+key+"/"+value, func(t *testing.T) {
					var handler SystemNodeEventHandler
					err := decodeNodeTestYAML([]byte(fmt.Sprintf(site.pattern, key, value)), &handler)
					if site.name == "rules_lone_field" && value == "{}" {
						if err == nil || !strings.Contains(err.Error(), "EMPTY-AUTHORED-RULE") {
							t.Fatalf("empty keyed rule %q was admitted: %v", key, err)
						}
						return
					}
					if site.name == "rules_lone_field" && value == "{nested: value}" {
						requireRetiredHandlerAction(t, err, "nested")
						return
					}
					requireRetiredHandlerAction(t, err, key)
				})
			}
		}
	}
}

func TestRetiredHandlerActionAliasesRejected(t *testing.T) {
	for _, key := range []string{"action", "evidence_target", "template", "instance_id_from", "config_from"} {
		for _, row := range []string{key + ": *value", "<<: *retired", "<<: [*retired]", "<<: *retired, " + key + ": null"} {
			for _, site := range []string{"handler: {%s}", "handler: {rules: [{%s}]}", "handler: {on_complete: [{%s}]}", "handler: {on_success: {%s}}", "handler: {join: {%s}}", "handler: {join: {stage: waiting, members: {from: state.ids, by: payload.id}, output: payload.result, on_complete: {%s}}}", "handler: {join: {stage: waiting, members: {from: state.ids, by: payload.id}, output: payload.result, on_complete: {advances_to: done}, deadline: {after: 1h, from: stage_entry}, on_deadline: {%s}}}"} {
				t.Run(key+"/"+row+"/"+site, func(t *testing.T) {
					raw := "value: &value null\nretired: &retired {" + key + ": null}\n" + fmt.Sprintf(site, row) + "\n"
					var handler SystemNodeEventHandler
					requireRetiredHandlerAction(t, decodeNodeTestMember([]byte(raw), "handler", &handler), key)
				})
			}
		}
		for _, target := range []string{"handler: *retired", "handler: {rules: [*retired]}", "handler: {rules: {chosen: *retired}}", "handler: {on_complete: [*retired]}"} {
			t.Run(key+"/row_alias/"+target, func(t *testing.T) {
				var handler SystemNodeEventHandler
				raw := "retired: &retired {" + key + ": null}\n" + target + "\n"
				requireRetiredHandlerAction(t, decodeNodeTestMember([]byte(raw), "handler", &handler), key)
			})
		}
	}
}

func TestRetiredHandlerActionDirectRuleAdmission(t *testing.T) {
	for _, key := range []string{"action", "evidence_target", "template", "instance_id_from", "config_from"} {
		for _, raw := range []string{key + ": null\n", "<<: &retired {" + key + ": ''}\n", key + ": &value {}\n"} {
			t.Run(raw, func(t *testing.T) {
				var rule HandlerRuleEntry
				requireRetiredHandlerAction(t, decodeNodeTestYAML([]byte(raw), &rule), key)
			})
		}
	}
}

func TestLoadWorkflowContractBundleRejectsRetiredHandlerActions(t *testing.T) {
	repoRoot := contractRepoRoot(t)
	for _, key := range []string{"action", "evidence_target", "template", "instance_id_from", "config_from"} {
		for _, handler := range []string{
			"{" + key + ": null}",
			"{rules: [{" + key + ": ''}]}",
			"{rules: {selected: {" + key + ": {}}}}",
			"{on_complete: [{" + key + ": null}]}",
			"{join: {stage: waiting, members: {from: state.ids, by: payload.id}, output: payload.result, on_complete: {" + key + ": null}}}",
			"{join: {stage: waiting, members: {from: state.ids, by: payload.id}, output: payload.result, on_complete: {advances_to: done}, deadline: {after: 1h, from: stage_entry}, on_deadline: {" + key + ": null}}}",
			"{<<: &retired {" + key + ": null}}",
		} {
			t.Run(key+"/"+handler, func(t *testing.T) {
				root := t.TempDir()
				writeFieldReconciliationBundle(t, root, "", "worker:\n  event_handlers:\n    work.requested: "+handler+"\n")
				_, err := LoadWorkflowContractBundleWithOverrides(repoRoot, root, DefaultPlatformSpecFile(repoRoot))
				requireRetiredHandlerAction(t, err, key)
			})
		}
	}
}

func TestHandlerActionRetirementPreservesOtherActionConcepts(t *testing.T) {
	t.Run("join_deadline_outcome", func(t *testing.T) {
		var handler SystemNodeEventHandler
		err := decodeNodeTestYAML([]byte(`join:
  stage: waiting
  members: {from: state.ids, by: payload.id}
  output: payload.result
  on_complete: {advances_to: done}
  deadline: {after: 1h, from: stage_entry}
  on_deadline: {advances_to: expired}
`), &handler)
		if err != nil || handler.Join == nil || handler.Join.Deadline == nil || handler.Join.Deadline.After != "1h" ||
			handler.Join.Deadline.From != JoinDeadlineFromStageEntry || !handler.Join.OnCompleteFound || !handler.Join.OnDeadlineFound ||
			handler.Join.OnComplete.AdvancesTo != "done" || handler.Join.OnDeadline.AdvancesTo != "expired" {
			t.Fatalf("supported deadline outcome changed: %#v, %v", handler.Join, err)
		}
	})
	t.Run("emit_template_specialization", func(t *testing.T) {
		var handler SystemNodeEventHandler
		err := decodeNodeTestYAML([]byte(`emit: {event: scored, fields: {id: payload.id}}
rules:
  - when: payload.score > 0
    emit: {fields: {label: "positive"}}
  - else: true
    emit: {fields: {label: "other"}}
`), &handler)
		if err != nil {
			t.Fatal(err)
		}
		sites := HandlerRuleEmitTemplateSites(handler)
		if len(sites) != 2 || sites[0].Spec.EventType() != "scored" || sites[0].Spec.Fields["id"].CEL != "payload.id" || sites[1].Spec.Fields["label"].Literal != "other" {
			t.Fatalf("emit template specialization changed: %#v", sites)
		}
	})
	t.Run("activity_and_business_field_names", func(t *testing.T) {
		var handler SystemNodeEventHandler
		err := decodeNodeTestYAML([]byte("activity: {tool: notify_human, input: {action: payload.action, template: payload.template, config_from: payload.config_from}}\n"), &handler)
		if err != nil || handler.Activity.Tool != "notify_human" || len(handler.Activity.Input) != 3 {
			t.Fatalf("activity input rejected as handler action: %#v, %v", handler.Activity, err)
		}
	})
	t.Run("timer_action", func(t *testing.T) {
		var timer WorkflowTimerContract
		if err := decodeNodeTestYAML([]byte("id: expiry\naction: expire\ndelay: 1h\n"), &timer); err != nil || timer.Action != "expire" {
			t.Fatalf("timer action changed: %#v, %v", timer, err)
		}
	})
	t.Run("data_accumulation_source_event", func(t *testing.T) {
		var handler SystemNodeEventHandler
		err := decodeNodeTestYAML([]byte("data_accumulation: {source_event: work.received}\n"), &handler)
		if err != nil || handler.DataAccumulation.SourceEvent != "work.received" {
			t.Fatalf("source_event confused with retired config_from: %#v, %v", handler.DataAccumulation, err)
		}
	})
	t.Run("retired_names_are_not_reserved_rule_labels", func(t *testing.T) {
		for _, label := range []string{"action", "template", "config_from", "evidence_target", "instance_id_from"} {
			var handler SystemNodeEventHandler
			err := decodeNodeTestYAML([]byte("rules: {"+label+": {else: true, emit: result}}\n"), &handler)
			if err != nil || len(handler.Rules) != 1 || handler.Rules[0].ID != label || handler.Rules[0].Emit.EventType() != "result" {
				t.Fatalf("display label %q became reserved: %#v, %v", label, handler.Rules, err)
			}
		}
	})
}
