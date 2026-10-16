package main

import "testing"

func TestHelperRenamesRequireExactReviewedPathAndName(t *testing.T) {
	t.Chdir("../../../..")
	for _, entry := range []struct{ path, before, after string }{
		{"internal/store/storetest/event.go", "InsertCanonicalEventRecord", "InsertNativeCanonicalEventRecord"},
		{"internal/runtime/template_instance_delivery_test.go", "waitRuntimeDBCount", "waitRuntimeStorageCount"},
		{"internal/runtime/template_instance_delivery_test.go", "waitRuntimeEventID", "waitRuntimeStoredEventID"},
		{"internal/runtime/template_instance_delivery_test.go", "assertRuntimeDBCount", "assertRuntimeStorageCount"},
		{"internal/runtime/artifact_action_result_delivery_test.go", "waitArtifactActionResultDBCount", "waitArtifactActionResultSettledCount"},
	} {
		candidate := "func " + entry.after + "() {}"
		got, removed := currentReplacement(entry.path, entry.before, map[string]string{entry.after: candidate})
		if got != candidate || removed {
			t.Fatalf("exact helper mapping changed: %s/%s => %s/%t", entry.path, entry.before, got, removed)
		}
		for _, foreign := range []struct{ path, name string }{
			{"unrelated.go", entry.before},
			{entry.path, "foreignHelper"},
		} {
			assertUnreviewedHelperRenameRefuses(t, foreign.path, foreign.name, map[string]string{entry.after: candidate})
		}
		assertUnreviewedHelperRenameRefuses(t, entry.path, entry.before, nil)
	}
	if len(renamedHelpers) != 3 || len(renamedHelpers["internal/store/storetest/event.go"]) != 1 || len(renamedHelpers["internal/runtime/template_instance_delivery_test.go"]) != 3 || len(renamedHelpers["internal/runtime/artifact_action_result_delivery_test.go"]) != 1 {
		t.Fatal("exact helper rename inventory changed")
	}
}

func assertUnreviewedHelperRenameRefuses(t *testing.T, path, name string, candidate map[string]string) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Errorf("unreviewed helper rename accepted: %s/%s", path, name)
		}
	}()
	currentReplacement(path, name, candidate)
}
