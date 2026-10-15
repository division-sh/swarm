package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestFamily145RetainedMailboxNativeConstructorPreservesSourceTokensRestartAndJoinedRelease(t *testing.T) {
	var rows []struct{ File, Function, Before, After string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Function != "newRetainedMailboxCompletionRuntimeConfigured" {
			continue
		}
		matched++
		prove := func(candidate string) error {
			return family145ServedProjectionProofError(row.Function, row.Before, candidate)
		}
		if err := prove(row.After); err != nil {
			t.Fatal(err)
		}
		for _, old := range []string{"store.NewPostgresStore(", "BootstrapPostgresRuntimeStore(", "DatabaseForTest(", "openSelectedPostgresOwner("} {
			if strings.Contains(row.After, old) {
				t.Fatalf("retained mailbox still owns duplicate construction: %s", old)
			}
		}
		for _, cut := range []string{"storeselected.OpenRuntime(ctx,", "Selection: selection", "PostgresDSN: dsn", "SessionLockTTL: runtimeSessionLockTTL(cfg)", "append(raw, []byte(configSuffix)...)", "Tokens: tokens", "Explicit: true", "executionposture.MockOnly", "process.stop()", "process.waitForReadyLine()", "selectedRuntimeStoreForTest(t, p)", "ReadApplicationSnapshot: snapshotReader", "buildStoresForServe = original", "t.Cleanup(cleanup)", "removePrivateTestRoot(retained)", "return rt, owner, start"} {
			if !strings.Contains(row.After, cut) || prove(strings.Replace(row.After, cut, "foreignRetainedOwner()", 1)) == nil {
				t.Fatalf("retained mailbox lost source/token/config/owner/restart/cleanup cut: %s", cut)
			}
		}
		current := family145ConversationForkOwnerSource(t, "../serveapp/mailbox_completion_fixture_test.go", row.Function)
		if current != row.After {
			t.Fatal("retained mailbox construction escaped its exact finite source")
		}
	}
	if matched != 1 {
		t.Fatalf("retained mailbox construction owner=%d/1", matched)
	}
}
