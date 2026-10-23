package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
)

type reviewedSuccessorPin struct {
	File, Function, Family                                  string
	BeforeSHA256, AfterSHA256, PriorSHA256, CandidateSHA256 string
	HasMechanicalReceipt                                    bool
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
	if fmt.Sprintf("%x", sha256.Sum256(raw)) != "6ed21982df76f46f7048b20781b83a7ba663de7dce3bd383adc7f3dce43d876f" {
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
