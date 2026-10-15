package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func family145MockMemoryConstructionProofError(function, before, after string) error {
	if function != "runMockAgentSupportedSurface" && function != "runStandingTelegramMemorySupportedSurface" {
		return fmt.Errorf("unreviewed pre-open constructor: %s", function)
	}
	for _, old := range []string{"runtimePG", "openStore", "prepareRestart", "store.NewPostgresStore(", "BootstrapPostgresRuntimeStore(", "DatabaseForTest(", "openSelectedPostgresOwner("} {
		if strings.Contains(after, old) {
			return fmt.Errorf("detached mock/memory construction survived: %s", old)
		}
	}
	normalized, err := family145ServedRestartConstructionInverse(before, after)
	if err != nil {
		return err
	}
	start := strings.Index(before, "\n\t\tvar runtimePG ")
	end := strings.Index(before, "\n\t\toldBuildStores := ")
	if start < 0 || end <= start {
		return fmt.Errorf("missing original detached adapter lifetime")
	}
	normalized = strings.Replace(normalized, "\n\t\toldBuildStores := ", before[start:end]+"\n\t\toldBuildStores := ", 1)
	normalized = strings.Replace(normalized, "\n\topts := cliapp.ServeOptions{", "\n\tvar prepareRestart func()\n\topts := cliapp.ServeOptions{", 1)
	marker := "\n\t\topts.ConfigPath = writeMockAgentRuntimeConfig(t, \"postgres\", \"\")"
	if function == "runStandingTelegramMemorySupportedSurface" {
		marker = "\n\t\topts.ConfigPath = writeStandingMockRuntimeConfig(t, \"postgres\", \"\")"
	}
	if strings.Count(normalized, marker) != 1 {
		return fmt.Errorf("missing original PostgreSQL configuration boundary")
	}
	normalized = strings.Replace(normalized, marker, "\n\t\tprepareRestart = openStore"+marker, 1)
	marker = "\n\tsecond := startOwnedMockLifecycleTestProcess("
	if function == "runStandingTelegramMemorySupportedSurface" {
		marker = "\n\tif !recoveryOnRestart {"
	}
	if strings.Count(normalized, marker) != 1 {
		return fmt.Errorf("missing real served restart boundary")
	}
	normalized = strings.Replace(normalized, marker, "\n\tif prepareRestart != nil {\n\t\tprepareRestart()\n\t}"+marker, 1)
	if normalized != before {
		return fmt.Errorf("mock/memory construction lost source/credentials/claims/recovery/identity/teardown or an assertion: %s", function)
	}
	return nil
}

func TestFamily145MockMemoryConstructionRetiresWholeDetachedRestartCapability(t *testing.T) {
	var rows []struct{ File, Function, Before, After string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, row := range rows {
		if row.Function != "runMockAgentSupportedSurface" && row.Function != "runStandingTelegramMemorySupportedSurface" {
			continue
		}
		if seen[row.Function] {
			t.Fatalf("duplicate construction owner: %s", row.Function)
		}
		seen[row.Function] = true
		prove := func(candidate string) error {
			return family145MockMemoryConstructionProofError(row.Function, row.Before, candidate)
		}
		if err := prove(row.After); err != nil {
			t.Fatal(err)
		}
		for _, cut := range []string{"storeselected.OpenRuntime(ctx,", "Selection: selection", "PostgresDSN: dsn", "SessionLockTTL: runtimeSessionLockTTL(cfg)", "buildStoresForServe = oldBuildStores", "t.Cleanup(cleanup)", "first.stop()", "second.stop()", "second.waitForReadyLine()", "retainedRoot, opts", "t.Fatalf"} {
			if !strings.Contains(row.After, cut) || prove(strings.Replace(row.After, cut, "foreignMockMemoryScope()", 1)) == nil {
				t.Fatalf("lost context/selection/source/claim/identity/assertion/restart/teardown cut: %s/%s", row.Function, cut)
			}
		}
		actual := family145ConversationForkOwnerSource(t, "../serveapp/"+strings.TrimPrefix(row.File, "internal/serveapp/"), row.Function)
		if actual != row.After {
			t.Fatalf("mock/memory native constructor escaped its finite source: %s", row.Function)
		}
	}
	if len(seen) != 2 {
		t.Fatalf("whole pre-open/restart cohort=%d/2", len(seen))
	}
}
