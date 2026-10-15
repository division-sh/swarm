package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestNativeConversationActivationSuccessorPreservesExactSourceAndAssertions(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	var selected *recipe
	for i := range rows {
		if rows[i].Function == "runServedConversationForkLifecycleProof" {
			if selected != nil {
				t.Fatal("ambiguous conversation activation recipe")
			}
			selected = &rows[i]
		}
	}
	if selected == nil || selected.Successor == "" || selected.Family != "native-served-delivery-quiescence-caller" {
		t.Fatal("missing exact conversation activation successor")
	}
	const cut = "\t// Empty delivery queues do not join committed source-agent activation.\n\trt.Probe.RequireEventPersisted(initial.EventID)\n"
	if strings.Count(selected.Successor, cut) != 1 {
		t.Fatal("missing or repeated exact source activation fence")
	}
	restored, err := canonicalFunction(strings.Replace(selected.Successor, cut, "", 1))
	before, beforeErr := canonicalFunction(selected.After)
	if err != nil || beforeErr != nil || restored != before {
		t.Fatal("activation successor changed an original assertion or other operation")
	}
	const oldSnapshot = `if len(chat.Snapshot.EntitySnapshot) != 1 || chat.Snapshot.EntitySnapshot[0].CurrentState != "draft" || chat.Snapshot.EntitySnapshot[0].Fields["name"] != "Before" {`
	const currentSnapshot = `if len(chat.Snapshot.EntitySnapshot) != 1 || chat.Snapshot.EntitySnapshot[0].CurrentState == nil || *chat.Snapshot.EntitySnapshot[0].CurrentState != "draft" || chat.Snapshot.EntitySnapshot[0].Fields["name"] != "Before" {`
	if strings.Count(selected.Successor, oldSnapshot) != 1 {
		t.Fatal("missing exact historical conversation lifecycle assertion")
	}
	current := strings.Replace(selected.Successor, oldSnapshot, currentSnapshot, 1)
	active, err := canonicalFunction(selectedCausalObservationBody(t, selected.File, selected.Function))
	want, wantErr := canonicalFunction(current)
	if err != nil || wantErr != nil || active != want {
		t.Fatal("current conversation source differs from its exact activation successor")
	}
	for name, mutant := range map[string]string{
		"missing":                     strings.Replace(current, cut, "", 1),
		"duplicate":                   strings.Replace(current, cut, cut+cut, 1),
		"wrong_event":                 strings.Replace(current, "RequireEventPersisted(initial.EventID)", "RequireEventPersisted(ready.EventID)", 1),
		"after_dependent_publication": strings.Replace(strings.Replace(current, cut, "", 1), "\twaitServedConversationForkSourceAgentReady(t, rt, initial.RunID, ready.EventID)", cut+"\twaitServedConversationForkSourceAgentReady(t, rt, initial.RunID, ready.EventID)", 1),
		"changed_assertion":           strings.Replace(current, "!initial.NewRunCreated", "initial.NewRunCreated", 1),
		"absent_lifecycle":            strings.Replace(current, "chat.Snapshot.EntitySnapshot[0].CurrentState == nil || ", "", 1),
		"wrong_stage":                 strings.Replace(current, `*chat.Snapshot.EntitySnapshot[0].CurrentState != "draft"`, `*chat.Snapshot.EntitySnapshot[0].CurrentState != "unknown"`, 1),
		"lost_business_field":         strings.Replace(current, `chat.Snapshot.EntitySnapshot[0].Fields["name"] != "Before"`, `chat.Snapshot.EntitySnapshot[0].Fields["name"] != "After"`, 1),
		"historical_scalar":           selected.Successor,
	} {
		t.Run(name, func(t *testing.T) {
			guard := *selected
			guard.Before, guard.After, guard.Successor = current, current, ""
			if mutant == current {
				t.Fatal("counterexample did not change the source")
			}
			if _, changed, err := rewriteFunction(guard.File, []byte("package proof\n"+mutant), guard); err == nil || changed {
				t.Fatal("changed activation cut accepted by the finite source guard")
			}
		})
	}
}
