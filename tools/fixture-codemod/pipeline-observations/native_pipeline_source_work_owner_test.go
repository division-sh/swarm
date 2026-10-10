package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestNativePipelineSourceOwnerRecipesPreserveBusWorkload(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, row := range rows {
		if row.Family != "native-pipeline-source-work-owner" {
			continue
		}
		if row.File != "internal/runtime/pipeline/author_activity_external_test_context_test.go" || seen[row.Function] {
			t.Fatal("unreviewed or repeated pipeline source-owner recipe")
		}
		seen[row.Function] = true
		switch row.Function {
		case "newScopedTestEventBus":
			before := "opts.WorkOwner = pipelineExternalTestWorkOwner(t)"
			after := "opts.WorkOwner = pipelineExternalTestWorkOwnerForSource(t, opts.SourceArtifactFact)"
			if strings.Count(row.Before, before) != 1 {
				t.Fatal("unreviewed bus work-owner setup")
			}
			expected, err := canonicalFunction(strings.Replace(row.Before, before, after, 1))
			actual, parseErr := canonicalFunction(row.After)
			if err != nil || parseErr != nil || expected != actual {
				t.Fatalf("bus registration/admission/publication workload changed: %v / %v", err, parseErr)
			}
		case "pipelineExternalTestWorkOwnerForSource":
			if !nativePipelineSourceOwnerShape(t, row.After) {
				t.Fatal("fixture does not use the original process and exact runtime identity")
			}
			for _, pair := range [][2]string{
				{"fact.Validate()", "foreignFact.Validate()"},
				{"BundleHash:        fact.BundleHash()", "BundleHash:        authorActivityTestSourceArtifactFact.BundleHash()"},
				{"fixture.runtimes[identity]", "fixture.runtimes[foreignIdentity]"},
				{"fixture.process.NewRuntime(context.Background(), identity)", "foreignProcess.NewRuntime(context.Background(), identity)"},
				{"fixture.retireAndWait(ctx)", "fixture.process.Join(ctx)"},
				{"fixture.mu.Lock()", "ignoredLock()"},
				{"defer fixture.mu.Unlock()", "defer ignoredUnlock()"},
			} {
				changed := strings.Replace(row.After, pair[0], pair[1], 1)
				if changed == row.After || nativePipelineSourceOwnerShape(t, changed) {
					t.Fatalf("lost source/process/cleanup authority accepted: %v", pair)
				}
			}
		default:
			t.Fatal("unknown source-owner consumer")
		}
	}
	if len(seen) != 2 {
		t.Fatalf("source-owner recipe count=%d, want the complete two-helper cohort", len(seen))
	}
}

func nativePipelineSourceOwnerShape(t *testing.T, source string) bool {
	t.Helper()
	body := formattedNativeReadNode(projectionShapeFunction(t, source).Body)
	identity := formattedNativeReadNode(projectionShapeStatement(t, `identity := worklifetime.RuntimeIdentity{
RuntimeInstanceID: authorActivityTestRuntimeInstanceID, BundleHash: fact.BundleHash(),
}`))
	return strings.Contains(body, identity) &&
		strings.Count(body, "fixture.runtimes[identity]") == 2 &&
		strings.Count(body, "fixture.process.NewRuntime(context.Background(), identity)") == 1 &&
		strings.Count(body, "fixture.retireAndWait(ctx)") == 1 &&
		strings.Count(body, "fixture.mu.Lock()") == 1 &&
		strings.Count(body, "defer fixture.mu.Unlock()") == 1 &&
		strings.Index(body, "fact.Validate()") >= 0 &&
		strings.Index(body, "fact.Validate()") < strings.Index(body, "pipelineExternalTestWorkFixtures.LoadOrStore") &&
		strings.Index(body, "fixture.mu.Lock()") < strings.Index(body, "fixture.runtimes[identity]") &&
		strings.Index(body, "pipelineExternalTestWorkFixtures.Delete(t)") > strings.Index(body, "fixture.retireAndWait(ctx)")
}
