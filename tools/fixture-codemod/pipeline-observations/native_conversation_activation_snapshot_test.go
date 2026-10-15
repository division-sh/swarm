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
	actual := selectedCausalObservationBody(t, selected.File, selected.Function)
	prior, actual, err := catchUp2542PriorMasterPair(*selected, actual)
	if err != nil {
		t.Fatal(err)
	}
	selected = &prior
	const cut = "\t// Empty delivery queues do not join committed source-agent activation.\n\trt.Probe.RequireEventPersisted(initial.EventID)\n"
	if strings.Count(selected.Successor, cut) != 1 {
		t.Fatal("missing or repeated exact source activation fence")
	}
	restored, err := canonicalFunction(strings.Replace(selected.Successor, cut, "", 1))
	before, beforeErr := canonicalFunction(selected.After)
	if err != nil || beforeErr != nil || restored != before {
		t.Fatal("activation successor changed an original assertion or other operation")
	}
	active, err := canonicalFunction(actual)
	want, wantErr := canonicalFunction(selected.Successor)
	if err != nil || wantErr != nil || active != want {
		t.Fatal("current conversation source differs from its exact activation successor")
	}
	for name, mutant := range map[string]string{
		"missing":                     strings.Replace(selected.Successor, cut, "", 1),
		"duplicate":                   strings.Replace(selected.Successor, cut, cut+cut, 1),
		"wrong_event":                 strings.Replace(selected.Successor, "RequireEventPersisted(initial.EventID)", "RequireEventPersisted(ready.EventID)", 1),
		"after_dependent_publication": strings.Replace(strings.Replace(selected.Successor, cut, "", 1), "\twaitServedConversationForkSourceAgentReady(t, rt, initial.RunID, ready.EventID)", cut+"\twaitServedConversationForkSourceAgentReady(t, rt, initial.RunID, ready.EventID)", 1),
		"changed_assertion":           strings.Replace(selected.Successor, "!initial.NewRunCreated", "initial.NewRunCreated", 1),
	} {
		t.Run(name, func(t *testing.T) {
			guard := *selected
			guard.Before, guard.After, guard.Successor = guard.Successor, guard.Successor, ""
			if mutant == selected.Successor {
				t.Fatal("counterexample did not change the source")
			}
			if _, changed, err := rewriteFunction(guard.File, []byte("package proof\n"+mutant), guard); err == nil || changed {
				t.Fatal("changed activation cut accepted by the finite source guard")
			}
		})
	}
}
