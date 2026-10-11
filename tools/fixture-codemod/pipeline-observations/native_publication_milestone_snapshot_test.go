package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func TestNativePublicationMilestoneSuccessorsPreserveReviewedAssertions(t *testing.T) {
	type cut struct{ before, after string }
	frontier := func(run string) cut {
		return cut{"", "\t\t\twaitServedEventPublishedLog(t, rt.Endpoint, " + run + ", frontier.EventID)\n"}
	}
	for _, target := range []struct {
		name, predecessor string
		cuts              []cut
	}{
		{"runLifecycleRootForkPolicy", "943897c3fbf0be765f133c22f26032b4b51d4912914c04c53771ef32918d7d93", []cut{frontier("seed.RunID")}},
		{"TestServedCompiledTransitionStaticForkEvidenceOnBothStores", "7b1057096b2f50fe7102d95ad575b65ab9c6f6b74630ed22b2e03db8b3f6f8ca", []cut{frontier("seed.RunID")}},
		{"TestServedCompiledTransitionTemplateSiblingForkCapabilityRefusalOnBothStores", "91f3ce70ac71dce662d9d3f32ddaf597332ac7dd10633df3ff58e567d85b9b7f", []cut{frontier("runID")}},
		{"TestSelectedForkPendingInputBothStores", "22dc3b9992b8ba39a92bd13ca7d26ada2ffa800a5ef0e808a0d15f6187f20433", []cut{
			{"", "\t\t\t\twaitServedEventPublishedLog(t, rt.Endpoint, seed.RunID, seed.EventID)\n"},
			{"", "\t\t\t\twaitServedEventPublishedLog(t, rt.Endpoint, seed.RunID, point.EventID)\n"},
		}},
		{"TestSelectedForkPendingInputEvidenceBothStores", "9f5c1e797d05ab230fa7c55756c000b955c19ec80ad3af9d241883f71e43aa02", []cut{
			{"", "\t\t\t\twaitServedEventPublishedLog(t, rt.Endpoint, seed.RunID, seed.EventID)\n"},
			{"", "\t\t\t\twaitServedEventPublishedLog(t, rt.Endpoint, seed.RunID, point.EventID)\n"},
		}},
		{"TestServedCompiledTransitionNestedCarrierCollisionOnBothStores", "3c668b9cdb84a0bd50b72d799fddedae23a4cde0958d57e6e6c89aaaa901ce90", []cut{
			{"\t\t\tconfigureOwnedMockLifecycleProbe(t, probe)\n", "\t\t\treadiness := &nestedSeedReadinessBarrier{observer: probe, release: make(chan struct{})}\n\t\t\treleaseReadiness := sync.OnceFunc(func() { close(readiness.release) })\n\t\t\tt.Cleanup(releaseReadiness)\n\t\t\tdefer releaseReadiness()\n\t\t\tconfigureOwnedMockLifecycleProbe(t, readiness)\n"},
			{"", "\t\t\t// Activation precedes this exact milestone; hold delivery so the\n\t\t\t// complete initial-tree assertion cannot observe a later handler state.\n\t\t\tprobe.Expect(seed.EventID).PostCommitDispatchStarted().Within(servedEventPublishLifecycleProbeWaitTimeout)\n"},
			{"", "\t\t\treleaseReadiness()\n"},
		}},
	} {
		t.Run(target.name, func(t *testing.T) {
			var rows []recipe
			if err := json.Unmarshal(recipeBytes, &rows); err != nil {
				t.Fatal(err)
			}
			var selected *recipe
			for i := range rows {
				if rows[i].Function == target.name {
					if selected != nil {
						t.Fatal("ambiguous milestone successor")
					}
					selected = &rows[i]
				}
			}
			if selected == nil || selected.Family != "native-served-delivery-quiescence-caller" || selected.Successor == "" {
				t.Fatal("missing exact milestone successor")
			}
			restored := selected.Successor
			for _, change := range target.cuts {
				if strings.Count(restored, change.after) != 1 {
					t.Fatal("missing or repeated reviewed temporal cut")
				}
				restored = strings.Replace(restored, change.after, change.before, 1)
				mutant := strings.Replace(selected.Successor, change.after, "\t\t\tunreviewedMilestone();\n", 1)
				guard := *selected
				guard.Before, guard.After, guard.Successor = guard.After, guard.Successor, ""
				if _, changed, err := rewriteFunction(guard.File, []byte("package probe\n"+mutant), guard); err == nil || changed {
					t.Fatal("changed temporal cut accepted by the source-bound guard")
				}
			}
			canonical, err := canonicalFunction(restored)
			if err != nil || fmt.Sprintf("%x", sha256.Sum256([]byte(canonical))) != target.predecessor {
				t.Fatal("milestone successor changed reviewed setup, state, refusal or snapshot assertions")
			}
			if files, changes, err := prepareFiles("../../..", []recipe{*selected}); err != nil || len(files) != 0 || len(changes) != 0 {
				t.Fatalf("current milestone successor is not inert: %v", err)
			}
		})
	}
}

func TestNativeRootForkMilestoneSuccessorRejectsHostileCuts(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	var selected *recipe
	for i := range rows {
		if rows[i].Function == "runLifecycleRootForkPolicy" {
			if selected != nil {
				t.Fatal("ambiguous root-fork successor")
			}
			selected = &rows[i]
		}
	}
	if selected == nil || selected.Successor == "" || selected.Removed || selected.Mechanical != nil {
		t.Fatal("root-fork successor changed its migration identity")
	}
	actual := selectedCausalObservationBody(t, selected.File, selected.Function)
	const gate = "\t\t\twaitServedEventPublishedLog(t, rt.Endpoint, seed.RunID, frontier.EventID)\n"
	const snapshot = "\t\t\tbefore := lifecycleStoredSnapshot(t, rt, seed.RunID)\n"
	const fork = "\t\t\trequireServedJSONRPCResult(t, rt.Endpoint, \"run.fork\", params, &fork)\n"
	withoutGate := strings.Replace(actual, gate, "", 1)
	mutants := map[string]string{
		"missing":         withoutGate,
		"duplicate":       strings.Replace(actual, gate, gate+gate, 1),
		"wrong_run":       strings.Replace(actual, gate, strings.Replace(gate, "seed.RunID", "frontier.RunID", 1), 1),
		"wrong_event":     strings.Replace(actual, gate, strings.Replace(gate, "frontier.EventID", "seed.EventID", 1), 1),
		"after_snapshot":  strings.Replace(withoutGate, snapshot, snapshot+gate, 1),
		"after_fork":      strings.Replace(withoutGate, fork, fork+gate, 1),
		"assertion":       strings.Replace(actual, "fork.ExecutedEventCount != 1", "fork.ExecutedEventCount != 0", 1),
		"source_identity": strings.Replace(actual, "\"source_run_id\": seed.RunID", "\"source_run_id\": frontier.RunID", 1),
	}
	if err := validateRootForkMilestoneSuccessor(*selected, actual); err != nil {
		t.Fatal(err)
	}
	for name, mutant := range mutants {
		t.Run(name, func(t *testing.T) {
			if mutant == actual || validateRootForkMilestoneSuccessor(*selected, mutant) == nil {
				t.Fatal("changed temporal cut or business assertion accepted")
			}
		})
	}
}

func validateRootForkMilestoneSuccessor(row recipe, actual string) error {
	want, err := canonicalFunction(row.Successor)
	if err != nil {
		return err
	}
	got, err := canonicalFunction(actual)
	if err != nil || got != want {
		return fmt.Errorf("actual root-fork function differs from exact successor: %v", err)
	}
	const gate = "\t\t\twaitServedEventPublishedLog(t, rt.Endpoint, seed.RunID, frontier.EventID)\n"
	publication := strings.Index(actual, "frontier := requireServedEventPublishRPCResult")
	observation := strings.Index(actual, gate)
	snapshot := strings.Index(actual, "before := lifecycleStoredSnapshot(t, rt, seed.RunID)")
	if strings.Count(actual, gate) != 1 || publication < 0 || observation <= publication || snapshot <= observation {
		return fmt.Errorf("exact frontier gate is not between publication and snapshot")
	}
	before, err := canonicalFunction(row.After)
	if err != nil {
		return err
	}
	restored, err := canonicalFunction(strings.Replace(actual, gate, "", 1))
	if err != nil || restored != before {
		return fmt.Errorf("root-fork setup or assertions changed beyond the one gate: %v", err)
	}
	return nil
}
