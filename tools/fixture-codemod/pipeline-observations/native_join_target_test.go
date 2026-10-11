package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func nativeJoinTargetRecipe(t *testing.T) recipe {
	t.Helper()
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	var selected []recipe
	for _, row := range rows {
		if row.Family == "native-join-target" {
			selected = append(selected, row)
		}
	}
	if len(selected) != 1 || selected[0].Function != "TestDeliveryTargetApplicationConsumesDeclarationBoundJoinTargetWithoutPayloadSelectorOnBothStores" {
		t.Fatal("join target cohort must retain its exact original root")
	}
	return selected[0]
}

func TestNativeJoinTargetRecipePreservesDeclarationPayloadAndRoute(t *testing.T) {
	row := nativeJoinTargetRecipe(t)
	if nativeCompositionTargetWorkload(t, row.Before, true) != nativeCompositionTargetWorkload(t, row.After, false) {
		t.Fatal("declaration-bound join source, handle, payload, route or assertions changed")
	}
}

func TestNativeJoinTargetOracleRejectsDeclarationPayloadAndOwnerDrift(t *testing.T) {
	row := nativeJoinTargetRecipe(t)
	for _, change := range []struct{ original, replacement string }{
		{`exactCompiledJoinPlanForTest(bundle, ".")`, "bundle.Semantics.Joins[0]"},
		{"json.Marshal(handle.PayloadMetadata())", "json.Marshal(nil)"},
		{"handle.TaskID()", `"unrelated-task"`},
		{"!application.Owner().ExistingEntity()", "false"},
		{"application.Route().InstancePath != testPipelineRunID", "false"},
		{"application.EntityID() != entityID", "false"},
	} {
		if !strings.Contains(row.After, change.original) {
			t.Fatalf("join target obligation missing: %s", change.original)
		}
		changed := strings.Replace(row.After, change.original, change.replacement, 1)
		if nativeCompositionTargetWorkload(t, row.Before, true) == nativeCompositionTargetWorkload(t, changed, false) {
			t.Fatalf("altered join target obligation admitted: %s", change.original)
		}
	}
}
