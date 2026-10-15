package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func TestNativePublicationMilestoneSuccessorsPreserveReviewedAssertions(t *testing.T) {
	type cut struct{ before, after, run, event, dependent string }
	frontier := func(run string) cut {
		return cut{after: "\t\t\twaitServedEventPublishedLog(t, rt.Endpoint, " + run + ", frontier.EventID)\n", run: run, event: "frontier.EventID", dependent: "\t\t\tbefore := "}
	}
	// Predecessors are the reviewed source at e685734f3, including prior Successors.
	for _, target := range []struct {
		name, predecessor string
		cuts              []cut
	}{
		{"runLifecycleRootForkPolicy", "943897c3fbf0be765f133c22f26032b4b51d4912914c04c53771ef32918d7d93", []cut{frontier("seed.RunID")}},
		{"TestServedCompiledTransitionStaticForkEvidenceOnBothStores", "7b1057096b2f50fe7102d95ad575b65ab9c6f6b74630ed22b2e03db8b3f6f8ca", []cut{frontier("seed.RunID")}},
		{"TestServedCompiledTransitionTemplateSiblingForkCapabilityRefusalOnBothStores", "91f3ce70ac71dce662d9d3f32ddaf597332ac7dd10633df3ff58e567d85b9b7f", []cut{frontier("runID")}},
		{"TestSelectedForkPendingInputBothStores", "22dc3b9992b8ba39a92bd13ca7d26ada2ffa800a5ef0e808a0d15f6187f20433", []cut{
			{after: "\t\t\t\twaitServedEventPublishedLog(t, rt.Endpoint, seed.RunID, seed.EventID)\n", run: "seed.RunID", event: "seed.EventID", dependent: "\t\t\t\twaitServedRunDeliveryQuiescence("},
			{after: "\t\t\t\twaitServedEventPublishedLog(t, rt.Endpoint, seed.RunID, point.EventID)\n", run: "seed.RunID", event: "point.EventID", dependent: "\t\t\t\trequirePendingInputStateCount("},
		}},
		{"TestSelectedForkPendingInputEvidenceBothStores", "9f5c1e797d05ab230fa7c55756c000b955c19ec80ad3af9d241883f71e43aa02", []cut{
			{after: "\t\t\t\twaitServedEventPublishedLog(t, rt.Endpoint, seed.RunID, seed.EventID)\n", run: "seed.RunID", event: "seed.EventID", dependent: "\t\t\t\twaitServedRunDeliveryQuiescence("},
			{after: "\t\t\t\twaitServedEventPublishedLog(t, rt.Endpoint, seed.RunID, point.EventID)\n", run: "seed.RunID", event: "point.EventID", dependent: "\t\t\t\tplan, err := family.Plan("},
		}},
		{"TestServedCompiledTransitionNestedCarrierCollisionOnBothStores", "3c668b9cdb84a0bd50b72d799fddedae23a4cde0958d57e6e6c89aaaa901ce90", []cut{
			{before: "\t\t\tconfigureOwnedMockLifecycleProbe(t, probe)\n", after: "\t\t\treadiness := &nestedSeedReadinessBarrier{observer: probe, release: make(chan struct{})}\n\t\t\treleaseReadiness := sync.OnceFunc(func() { close(readiness.release) })\n\t\t\tt.Cleanup(releaseReadiness)\n\t\t\tdefer releaseReadiness()\n\t\t\tconfigureOwnedMockLifecycleProbe(t, readiness)\n"},
			{after: "\t\t\t// Activation precedes this exact milestone; hold delivery so the\n\t\t\t// complete initial-tree assertion cannot observe a later handler state.\n\t\t\tprobe.Expect(seed.EventID).PostCommitDispatchStarted().Within(servedEventPublishLifecycleProbeWaitTimeout)\n", event: "seed.EventID", dependent: "\t\t\tgotStages := map[string]string{}\n"},
			{after: "\t\t\treleaseReadiness()\n"},
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
			current := *selected
			reviewed := reviewedMilestoneRow(t, current)
			selected = &reviewed
			restored := selected.Successor
			for index, change := range target.cuts {
				if strings.Count(restored, change.after) != 1 {
					t.Fatal("missing or repeated reviewed temporal cut")
				}
				restored = strings.Replace(restored, change.after, change.before, 1)
				without := strings.Replace(selected.Successor, change.after, change.before, 1)
				mutants := map[string]string{
					"missing":   without,
					"duplicate": strings.Replace(selected.Successor, change.after, change.after+change.after, 1),
					"altered":   strings.Replace(selected.Successor, change.after, "\t\t\tunreviewedMilestone();\n", 1),
				}
				if change.run != "" {
					mutants["wrong_run"] = strings.Replace(selected.Successor, change.after, strings.Replace(change.after, change.run, "foreign.RunID", 1), 1)
				}
				if change.event != "" {
					mutants["wrong_event"] = strings.Replace(selected.Successor, change.after, strings.Replace(change.after, change.event, "foreign.EventID", 1), 1)
				}
				if change.dependent != "" {
					start := strings.Index(selected.Successor, change.after)
					observation := strings.Index(without[start:], change.dependent)
					if observation < 0 {
						t.Fatal("missing reviewed dependent observation")
					}
					end := start + observation
					end += strings.IndexByte(without[end:], '\n') + 1
					mutants["after_observation"] = without[:end] + change.after + without[end:]
				}
				for name, mutant := range mutants {
					t.Run(fmt.Sprintf("cut_%d/%s", index, name), func(t *testing.T) {
						guard := *selected
						guard.Before, guard.After, guard.Successor = guard.Successor, guard.Successor, ""
						if mutant == selected.Successor {
							t.Fatal("hostile control did not change source")
						}
						if _, changed, err := rewriteFunction(guard.File, []byte("package probe\n"+mutant), guard); err == nil || changed {
							t.Fatal("changed temporal cut accepted by the source-bound guard")
						}
					})
				}
			}
			mutants := map[string]string{"unrelated_source": strings.Replace(selected.Successor, "{\n", "{\n\tunreviewedSetup()\n", 1)}
			if strings.Contains(selected.Successor, "nestedSeedReadinessBarrier") {
				const release = "\t\t\treleaseReadiness()\n"
				const assertion = "\t\t\tif !reflect.DeepEqual(gotStages, wantStages) {\n"
				without := strings.Replace(selected.Successor, release, "", 1)
				mutants["release_before_tree_assertion"] = strings.Replace(without, assertion, release+assertion, 1)
				mutants["changed_tree_assertion"] = strings.Replace(selected.Successor, assertion, strings.Replace(assertion, "!reflect", "reflect", 1), 1)
			}
			for name, mutant := range mutants {
				t.Run(name, func(t *testing.T) {
					guard := *selected
					guard.Before, guard.After, guard.Successor = guard.Successor, guard.Successor, ""
					if mutant == selected.Successor {
						t.Fatal("hostile control did not change source")
					}
					if _, changed, err := rewriteFunction(guard.File, []byte("package probe\n"+mutant), guard); err == nil || changed {
						t.Fatal("changed source or tree assertion accepted by the source-bound guard")
					}
				})
			}
			canonical, err := canonicalFunction(restored)
			if err != nil || fmt.Sprintf("%x", sha256.Sum256([]byte(canonical))) != target.predecessor {
				t.Fatal("milestone successor changed reviewed setup, state, refusal or snapshot assertions")
			}
			if files, changes, err := prepareFiles("../../..", []recipe{current}); err != nil || len(files) != 0 || len(changes) != 0 {
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
	current, err := canonicalFunction(actual)
	want, wantErr := canonicalFunction(selected.Successor)
	if err != nil || wantErr != nil || current != want {
		t.Fatal("root-fork source differs from its current successor")
	}
	reviewed := reviewedMilestoneRow(t, *selected)
	selected = &reviewed
	actual = reviewed.Successor
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

func TestNativeReceiverForkMilestoneSuccessorPreservesReviewedCuts(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	var selected *recipe
	for i := range rows {
		if rows[i].Function == "TestReceiverCompositionForkBothStores" {
			if selected != nil {
				t.Fatal("ambiguous receiver-fork successor")
			}
			selected = &rows[i]
		}
	}
	if selected == nil || selected.Family != "native-served-delivery-quiescence-caller" || selected.File != "internal/serveapp/receiver_fork_test.go" || selected.Successor == "" || selected.Removed || selected.Mechanical != nil {
		t.Fatal("receiver-fork successor changed its migration identity")
	}
	currentRow := *selected
	actual := selectedCausalObservationBody(t, selected.File, selected.Function)
	prior, actual, err := catchUp2542PriorMasterPair(*selected, actual)
	if err != nil {
		t.Fatal(err)
	}
	selected = &prior
	if err := validateReceiverForkMilestoneSuccessor(*selected, actual); err != nil {
		t.Fatal(err)
	}
	const seedGate = "\t\t\t\tprobe.Expect(seedEventID).PostCommitDispatchStarted().PostCommitDispatchCompleted().Within(servedEventPublishLifecycleProbeWaitTimeout)\n"
	const requestGate = "\t\t\t\tprobe.Expect(request.EventID).PostCommitDispatchStarted().PostCommitDispatchCompleted().Within(remaining)\n"
	const publication = "\t\t\t\t\tpublication.unblock()\n"
	const nodeWait = "\t\t\t\t\tselect {\n\t\t\t\t\tcase <-reached:\n"
	const snapshot = "\t\t\t\tbefore := repeatedStaticRunSnapshot(t, rt.DB, request.RunID)\n"
	const fieldlessCapture = "\"idempotency_key\": \"empty-fork-seed\"})\n\t\t\t\t\tseedEventID = seed.EventID\n"
	without := strings.Replace(actual, requestGate, "", 1)
	mutants := map[string]string{
		"missing_fieldless_identity":      strings.Replace(actual, fieldlessCapture, strings.Replace(fieldlessCapture, "\t\t\t\t\tseedEventID = seed.EventID\n", "", 1), 1),
		"wrong_fieldless_identity":        strings.Replace(actual, fieldlessCapture, strings.Replace(fieldlessCapture, "seedEventID = seed.EventID", "seedEventID = \"foreign\"", 1), 1),
		"missing_seed_gate":               strings.Replace(actual, seedGate, "", 1),
		"duplicate_seed_gate":             strings.Replace(actual, seedGate, seedGate+seedGate, 1),
		"wrong_seed":                      strings.Replace(actual, seedGate, strings.Replace(seedGate, "seedEventID", "request.EventID", 1), 1),
		"missing_request_gate":            without,
		"duplicate_request_gate":          strings.Replace(actual, requestGate, requestGate+requestGate, 1),
		"wrong_request":                   strings.Replace(actual, requestGate, strings.Replace(requestGate, "request.EventID", "seedEventID", 1), 1),
		"after_snapshot":                  strings.Replace(without, snapshot, snapshot+requestGate, 1),
		"node_before_publication_release": strings.Replace(strings.Replace(actual, publication, "", 1), nodeWait, nodeWait+publication, 1),
		"premature_node_release":          strings.Replace(actual, snapshot, "\t\t\t\tclose(release)\n"+snapshot, 1),
		"fingerprint":                     strings.Replace(actual, snapshot, strings.Replace(snapshot, "request.RunID", "seedEventID", 1), 1),
		"business_assertion":              strings.Replace(actual, "result.ExecutedEventCount != 1", "result.ExecutedEventCount != 0", 1),
		"unrelated":                       strings.Replace(actual, "{\n", "{\n\tunreviewedSetup()\n", 1),
	}
	for name, mutant := range mutants {
		t.Run(name, func(t *testing.T) {
			if mutant == actual || validateReceiverForkMilestoneSuccessor(*selected, mutant) == nil {
				t.Fatal("changed receiver cut or business assertion accepted")
			}
			guard := *selected
			guard.Before, guard.After, guard.Successor = actual, actual, ""
			if _, changed, err := rewriteFunction(guard.File, []byte("package probe\n"+mutant), guard); err == nil || changed {
				t.Fatal("hostile receiver source accepted by inertness guard")
			}
		})
	}
	if files, changes, err := prepareFiles("../../..", []recipe{currentRow}); err != nil || len(files) != 0 || len(changes) != 0 {
		t.Fatalf("current receiver successor is not inert: %v", err)
	}
}

func validateReceiverForkMilestoneSuccessor(row recipe, actual string) error {
	if fmt.Sprintf("%x", sha256.Sum256([]byte(row.After))) != "fecc19b2cd332c799fb77740e3e4e9c0c857810022536660b0f188d1b87791e6" {
		return fmt.Errorf("receiver predecessor changed")
	}
	want, err := canonicalFunction(row.Successor)
	if err != nil {
		return err
	}
	got, err := canonicalFunction(actual)
	if err != nil || got != want {
		return fmt.Errorf("actual receiver function differs from exact successor: %v", err)
	}
	// Reverse the one fieldless identity capture and two concrete block replacements.
	const fieldless = "\t\t\t\t\tseed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{\"event_name\": \"fork.seeded\", \"bundle_hash\": rt.BundleHash, \"payload\": map[string]any{}, \"idempotency_key\": \"empty-fork-seed\"})\n"
	const capture = "\t\t\t\t\tseedEventID = seed.EventID\n"
	if strings.Count(actual, fieldless+capture) != 1 {
		return fmt.Errorf("fieldless seed identity not captured exactly once")
	}
	restored := strings.Replace(actual, fieldless+capture, fieldless, 1)
	for _, boundary := range []struct{ start, end string }{
		{"\t\t\t\tif surface == \"pending_refusal\" {\n\t\t\t\t\tselect {", "\t\t\t\t} else if !admitted {\n"},
		{"\t\t\t\t// Join the exact publications, not the independently held node.\n", "\t\t\t\tbefore := repeatedStaticRunSnapshot(t, rt.DB, request.RunID)\n"},
	} {
		start := strings.Index(restored, boundary.start)
		end := strings.Index(restored, boundary.end)
		if start < 0 || end <= start || strings.Count(restored, boundary.start) != 1 || strings.Count(restored, boundary.end) != 1 {
			return fmt.Errorf("ambiguous reviewed receiver cut")
		}
		oldStart := strings.Index(row.After, boundary.start)
		if oldStart < 0 {
			// The historical completion block was pending-only; preserve it verbatim.
			oldStart = strings.Index(row.After, "\t\t\t\tif surface == \"pending_refusal\" {\n\t\t\t\t\t// Join only the source publications")
		}
		oldEnd := strings.Index(row.After, boundary.end)
		if oldStart < 0 || oldEnd <= oldStart {
			return fmt.Errorf("immutable predecessor lacks receiver cut")
		}
		restored = restored[:start] + row.After[oldStart:oldEnd] + restored[end:]
	}
	canonical, err := canonicalFunction(restored)
	if err != nil || fmt.Sprintf("%x", sha256.Sum256([]byte(canonical))) != "8eb64583ba2be0ff717d2d2768956a062ebe7d0164f3587493ac7b7836853c73" {
		return fmt.Errorf("receiver edit exceeds identity capture, completion gates and barrier move: %v", err)
	}
	return nil
}
