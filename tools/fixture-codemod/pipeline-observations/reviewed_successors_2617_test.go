package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
)

type reviewedSuccessorPin struct {
	File, Function, Family                                  string
	BeforeSHA256, AfterSHA256, PriorSHA256, CandidateSHA256 string
	HasMechanicalReceipt                                    bool
}

type reviewedSuccessorEdit struct {
	Start          int
	Current, Prior []string
}

type reviewedSuccessor2617 struct {
	Pin       reviewedSuccessorPin
	Original  recipe
	Candidate string
	Edits     []reviewedSuccessorEdit
}

type reviewedSharedCoordinatorPin struct {
	Family, File, Function, HistorySHA256, CurrentSHA256 string
	HasSuccessor                                         bool
}

type reviewedSuccessorPacket2617 struct {
	Source, Authorization, ProposalSHA256 string
	Rows                                  []reviewedSuccessor2617
	Shared                                []reviewedSharedCoordinatorPin
}

func loadReviewedSuccessors2617(t *testing.T) reviewedSuccessorPacket2617 {
	t.Helper()
	raw, err := os.ReadFile("testdata/reviewed_successors_2617.json")
	if err != nil {
		t.Fatal(err)
	}
	// This packet preserves all prior endpoints, including nonempty Successors.
	if fmt.Sprintf("%x", sha256.Sum256(raw)) != "ee6bdfc2cdf99e3257d92cfe2899219c2628b46eeaecff5e50d551c6387088a7" {
		t.Fatal("finite reviewed composition evidence changed")
	}
	var packet reviewedSuccessorPacket2617
	if err := json.Unmarshal(raw, &packet); err != nil {
		t.Fatal(err)
	}
	if packet.Source != "afd689753fdd9addeca9d19c3a5c809176a2c1a7" ||
		packet.Authorization != "https://github.com/division-sh/swarm/pull/2617#issuecomment-6149649761" ||
		packet.ProposalSHA256 != "f05762ac1b4e2d46b492bab3d76033fb511898ba1ad01c1c7b7a524be7f2e42f" ||
		len(packet.Rows) != 13 || len(packet.Shared) != 18 {
		t.Fatal("wrong reviewed source, authorization or finite row set")
	}
	return packet
}

func sameReviewedRecipeKey(left, right recipe) bool {
	return left.Family == right.Family && left.File == right.File && left.Function == right.Function
}

func matchesReviewedSuccessor2617(row recipe, source string, pin reviewedSuccessor2617) bool {
	if original, err := fixtureSetupCurrentEndpoint2617(row, source); err == nil {
		row, source = original, original.Successor
	}
	history := row
	history.Successor = pin.Original.Successor
	return reflect.DeepEqual(history, pin.Original) &&
		historicalCurrentSourcePinned(row.Successor, pin.Pin.CandidateSHA256) &&
		historicalCurrentSourcePinned(source, pin.Pin.CandidateSHA256)
}

func reverseReviewedSuccessor2617(source string, edits []reviewedSuccessorEdit) (string, error) {
	lines := strings.SplitAfter(source, "\n")
	end := len(lines)
	for index := len(edits) - 1; index >= 0; index-- {
		edit := edits[index]
		stop := edit.Start + len(edit.Current)
		if edit.Start < 0 || stop > end || !reflect.DeepEqual(lines[edit.Start:stop], edit.Current) {
			return "", fmt.Errorf("changed or overlapping finite composition edit %d", index)
		}
		lines = append(append(append([]string{}, lines[:edit.Start]...), edit.Prior...), lines[stop:]...)
		end = edit.Start
	}
	return strings.Join(lines, ""), nil
}

func reviewedSharedCoordinatorBody2617(row recipe, packet reviewedSuccessorPacket2617) (string, error) {
	for _, pin := range packet.Shared {
		if row.Family != pin.Family || row.File != pin.File || row.Function != pin.Function {
			continue
		}
		history := row
		history.Successor = ""
		raw, err := json.Marshal(history)
		if err != nil {
			return "", err
		}
		current := row.After
		if row.Successor != "" {
			current = row.Successor
		}
		if fmt.Sprintf("%x", sha256.Sum256(raw)) != pin.HistorySHA256 ||
			(row.Successor != "") != pin.HasSuccessor || !historicalCurrentSourcePinned(current, pin.CurrentSHA256) {
			return "", fmt.Errorf("unreviewed shared coordinator history/current endpoint: %s", row.Function)
		}
		return current, nil
	}
	return "", fmt.Errorf("unlisted shared coordinator endpoint: %s/%s", row.File, row.Function)
}

func TestReviewedSuccessors2617PreserveExactHistoryAndRejectHostileCompositions(t *testing.T) {
	packet := loadReviewedSuccessors2617(t)
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	for _, pin := range packet.Rows {
		t.Run(pin.Pin.Function, func(t *testing.T) {
			var matched []recipe
			for _, row := range rows {
				if sameReviewedRecipeKey(row, pin.Original) {
					matched = append(matched, row)
				}
			}
			if len(matched) != 1 {
				t.Fatalf("finite recipe multiplicity=%d", len(matched))
			}
			row := matched[0]
			prior := pin.Original.After
			if pin.Original.Successor != "" {
				prior = pin.Original.Successor
			}
			if row.Family != pin.Pin.Family || row.File != pin.Pin.File || row.Function != pin.Pin.Function ||
				!historicalCurrentSourcePinned(row.Before, pin.Pin.BeforeSHA256) ||
				!historicalCurrentSourcePinned(row.After, pin.Pin.AfterSHA256) ||
				!historicalCurrentSourcePinned(prior, pin.Pin.PriorSHA256) ||
				(row.Mechanical != nil) != pin.Pin.HasMechanicalReceipt {
				t.Fatal("reviewed historical fields or prior endpoint changed")
			}
			actual := selectedCausalObservationBody(t, row.File, row.Function)
			if !matchesReviewedSuccessor2617(row, actual, pin) {
				t.Fatal("current source/Successor pair is not the exact authorized endpoint")
			}
			candidate, err := canonicalFunction(actual)
			if err != nil {
				t.Fatal(err)
			}
			historicalCandidate := candidate
			if previous, err := fixtureSetupCurrentEndpoint2617(row, actual); err == nil {
				historicalCandidate, err = canonicalFunction(previous.Successor)
				if err != nil {
					t.Fatal(err)
				}
			}
			original, err := reverseReviewedSuccessor2617(historicalCandidate, pin.Edits)
			if err != nil || !historicalCurrentSourcePinned(original, pin.Pin.PriorSHA256) {
				t.Fatalf("only the listed edits must recover the prior endpoint: %v", err)
			}
			if files, changes, err := prepareFiles("../../..", []recipe{row}); err != nil || len(files) != 0 || len(changes) != 0 {
				t.Fatalf("authorized current endpoint is not inert: %v", err)
			}
			for _, field := range []string{"family", "file", "function", "before", "after", "removed", "mechanical"} {
				mutant := row
				switch field {
				case "family":
					mutant.Family += "-unreviewed"
				case "file":
					mutant.File = "unlisted.go"
				case "function":
					mutant.Function += "Unlisted"
				case "before":
					mutant.Before = row.After
				case "after":
					mutant.After = row.Before
				case "removed":
					mutant.Removed = true
				case "mechanical":
					mutant.Mechanical = &mechanicalSnapshot{SourceCommit: strings.Repeat("0", 40), After: row.After}
				}
				if matchesReviewedSuccessor2617(mutant, actual, pin) {
					t.Fatalf("changed historical %s accepted", field)
				}
			}
			// Matching an unlisted source and Successor cannot self-authorize.
			cuts := []string{"SourceArtifactFact", "source", "authority", "DeliveryAuthority", "ParentInstance", "parent", "EffectiveMode", "FlowModeTemplate", "InstanceKey", "pg", "selected", "t.Fatal", "t.Fatalf", "cancel", "Shutdown", "t.Cleanup", "time.After", "timer", "ready", "reply-first", "reply-second", "route_marker", "return"}
			mutants := 0
			for _, cut := range cuts {
				changed := strings.Replace(candidate, cut, "unreviewedCut", 1)
				if changed == candidate {
					continue
				}
				mutants++
				mutant := row
				mutant.Successor = changed
				if matchesReviewedSuccessor2617(mutant, changed, pin) || matchesReviewedSuccessor2617(row, changed, pin) {
					t.Fatalf("hostile current source/Successor admitted: %s", cut)
				}
			}
			if mutants == 0 {
				t.Fatal("no hostility coverage for finite endpoint")
			}
			mutant := row
			mutant.Successor = ""
			if matchesReviewedSuccessor2617(mutant, actual, pin) {
				t.Fatal("missing exact successor admitted")
			}
		})
	}
}

func TestReviewedSuccessors2617RetainDiagnosticBoundsAndExactReplaySiblings(t *testing.T) {
	packet := loadReviewedSuccessors2617(t)
	diagnostic := packet.Rows[1]
	old, err := canonicalFunction(diagnostic.Original.After)
	if err != nil {
		t.Fatal(err)
	}
	call := "\tlogScatterGatherContinuationOwners(t, ctx, h)\n"
	if strings.Count(diagnostic.Candidate, call) != 1 || strings.Replace(diagnostic.Candidate, call, "", 1) != old ||
		!strings.Contains(diagnostic.Candidate, "context.WithTimeout(context.WithoutCancel(h.ctx), time.Second)") {
		t.Fatal("scatter diagnostic changed more than its single bounded failure-only call")
	}
	helper := selectedCausalObservationBody(t, diagnostic.Pin.File, "logScatterGatherContinuationOwners")
	const helperPin = "de9d49b8a0a2035d917c42fc7291d6ce4920d65986a6b814aa8186641c01a826"
	if !historicalCurrentSourcePinned(helper, helperPin) {
		t.Fatal("reviewed read-only diagnostic helper changed")
	}
	if strings.Count(helper, ".ScanDeliveryContinuations(") != 1 ||
		!strings.Contains(helper, "deliverylifecycle.ContinuationCursor{}, 64)") ||
		strings.Count(helper, "h.workflow.Load(ctx, owner)") != 1 {
		t.Fatal("scatter diagnostic lost its single bounded page/native header reads")
	}
	for _, write := range []string{".Claim(", ".ClaimDelivery(", ".SettleSuccess(", ".SettleFailure(", ".Commit(", ".Publish(", ".ExecContext(", "go func", "for {"} {
		if strings.Contains(helper, write) {
			t.Fatalf("scatter diagnostic gained mutation/retry: %s", write)
		}
	}
	for _, cut := range []string{"ScanDeliveryContinuations", "{}, 64)", "h.workflow.Load", "catalogRuntimeRunID"} {
		mutant := strings.Replace(helper, cut, "unreviewedCut", 1)
		if mutant == helper || historicalCurrentSourcePinned(mutant, helperPin) {
			t.Fatalf("changed diagnostic owner/bound admitted: %s", cut)
		}
	}
	// Static actors are exactly entityless; distinction is reply/projection, not arbitrary entity IDs.
	replay := packet.Rows[9].Candidate
	for _, required := range []string{
		`map[string]string{"route_marker": "first"}`, `map[string]string{"route_marker": "second"}`,
		`ID: "reply-first"`, `ID: "reply-second"`, "firstClaim.Claim", "secondClaim.Claim",
		"runtimedelivery.FailureRetry", "snapshot.Terminal()", "len(originalRows) != 2 || len(newRows) != 2",
		"len(persistedReplay.Deliveries) != 2", "events.SameDeliveryRouteIdentity(route, delivery.Route)",
		"sourceByNew[newDeliveryID]", "agentSourceByNew[newDeliveryID]", "originalDeliveryID",
		"delivery.DeliveryContext().ReplyContextID() != routes[index].Context.ReplyContextID()",
	} {
		if !strings.Contains(replay, required) {
			t.Fatalf("exact replay sibling/eligibility evidence disappeared: %s", required)
		}
	}
	if strings.Count(replay, "events.MustEntitylessReceiverTarget(") != 2 {
		t.Fatal("entityless static actor proof silently became an arbitrary entity-target proof")
	}
}

func TestReviewedSharedCoordinator2617RefusesUnlistedCurrentPairs(t *testing.T) {
	packet := loadReviewedSuccessors2617(t)
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, row := range rows {
		if row.Family != "native-shared-coordinator-authority" {
			continue
		}
		count++
		actual := selectedCausalObservationBody(t, row.File, row.Function)
		current, err := reviewedSharedCoordinatorCurrentBody2617(row, actual, packet)
		if err != nil {
			t.Fatal(err)
		}
		for _, field := range []string{"history", "key", "source"} {
			mutant := row
			switch field {
			case "history":
				mutant.Before = row.After
			case "key":
				mutant.File = "unlisted.go"
			case "source":
				mutant.Successor = strings.Replace(current, "func ", "func Unreviewed", 1)
			}
			if _, err := reviewedSharedCoordinatorCurrentBody2617(mutant, current, packet); err == nil {
				t.Fatalf("shared coordinator accepted %s mutation for %s", field, row.Function)
			}
		}
	}
	if count != 18 {
		t.Fatalf("shared finite coverage=%d,want18", count)
	}
}
