package main

import (
	"encoding/json"
	"strconv"
	"strings"
	"testing"
)

func TestNativeVerificationListenerRecipeChangesOnlyFixtureAddresses(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, row := range rows {
		if row.Family != "native-verification-listeners" {
			continue
		}
		count++
		if row.File != "internal/cliapp/provider_trigger_test_registry_test.go" || row.Function != "writeTestVerifyRuntimeConfig" {
			t.Fatal("unreviewed verification fixture recipe")
		}
		before := strconv.Quote("llm:\n  backend: anthropic\n")
		after := before + "+testutil.EphemeralServeListenerConfig()"
		if strings.Count(row.Before, before) != 1 {
			t.Fatal("original verification profile changed")
		}
		expected, err := canonicalFunction(strings.Replace(row.Before, before, after, 1))
		actual, parseErr := canonicalFunction(row.After)
		if err != nil || parseErr != nil || expected != actual {
			t.Fatalf("verification profile, writer or fixture lifetime changed: %v / %v", err, parseErr)
		}
		for _, pair := range [][2]string{
			{"anthropic", "openai_responses"},
			{"testutil.EphemeralServeListenerConfig()", "foreignFixturePolicy()"},
			{"+testutil.EphemeralServeListenerConfig()", ""},
			{"t.TempDir()", "foreignRoot"},
			{"writeRuntimeConfigText(t, path,", "foreignWriter(t, path,"},
		} {
			changed := strings.Replace(row.After, pair[0], pair[1], 1)
			if changed == row.After {
				t.Fatalf("negative control did not change source: %v", pair)
			}
			actual, err := canonicalFunction(changed)
			if err == nil && actual == expected {
				t.Fatalf("changed profile/port/writer/lifetime accepted: %v", pair)
			}
		}
	}
	if count != 1 {
		t.Fatalf("verification fixture recipe count=%d, want one shared owner", count)
	}
}

func TestNativeGoldenListenerRecipePreservesWorkloadAndStore(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, row := range rows {
		if row.Family != "native-golden-verification-listeners" {
			continue
		}
		count++
		if row.File != "internal/releasee2e/golden_agent_workload_test.go" || row.Function != "goldenRuntimeConfig" {
			t.Fatal("unreviewed golden fixture recipe")
		}
		if strings.Count(row.Before, "store.configYAML") != 1 {
			t.Fatal("original golden store selection changed")
		}
		expected, err := canonicalFunction(strings.Replace(row.Before, "store.configYAML", "store.configYAML+testutil.EphemeralServeListenerConfig()", 1))
		actual, parseErr := canonicalFunction(row.After)
		if err != nil || parseErr != nil || expected != actual {
			t.Fatalf("golden workload or store selection changed: %v / %v", err, parseErr)
		}
		for _, pair := range [][2]string{
			{"claude_cli", "anthropic"},
			{"recovery_on_startup: true", "recovery_on_startup: false"},
			{"backend: host", "backend: foreign"},
			{"store.configYAML", "foreignStoreConfig"},
			{"testutil.EphemeralServeListenerConfig()", "foreignFixturePolicy()"},
		} {
			changed := strings.Replace(row.After, pair[0], pair[1], 1)
			if changed == row.After {
				t.Fatalf("negative control did not change source: %v", pair)
			}
			actual, err := canonicalFunction(changed)
			if err == nil && actual == expected {
				t.Fatalf("changed golden workload/store/listeners accepted: %v", pair)
			}
		}
	}
	if count != 1 {
		t.Fatalf("golden fixture recipe count=%d, want one shared producer", count)
	}
}
