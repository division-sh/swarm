// The original 34 calls have a typed rule; the explicitly approved sibling
// repairs retain complete snapshots for stratified semantic review.
package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"sort"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/channelanchor"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/channelintent"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/selectedreadrole"
)

const predecessor = "3dc4108680367fcf0633aae75c8f7cdca2959961"
const ledger = "tools/fixture-codemod/pipeline-observations/family145-channel-intent-preimages.json"

type pin struct {
	File, Function, Before, After, SHA256, Class string
	Calls                                        int
}

var paths = []string{
	"internal/serveapp/channel_object_supported_surface_test.go",
	"internal/serveapp/channel_object_input_supported_surface_test.go",
	"internal/serveapp/channel_object_anchor_supported_surface_test.go",
	"internal/serveapp/channel_object_response_supported_surface_test.go",
	"internal/serveapp/channel_object_temporal_supported_surface_test.go",
	"internal/serveapp/channel_input_process_supported_surface_test.go",
	"internal/serveapp/channel_selected_control_supported_surface_test.go",
	"internal/serveapp/channel_receptionist_supported_surface_test.go",
	"internal/serveapp/channel_delivery_uncertain_copy_supported_surface_test.go",
	"internal/serveapp/channel_delivery_draft_temporal_supported_surface_test.go",
	"internal/serveapp/channel_delivery_anchor_supported_surface_test.go",
	"internal/serveapp/channel_delivery_temporal_supported_surface_test.go",
	"internal/serveapp/channel_delivery_process_supported_surface_test.go",
	"internal/serveapp/channel_ack_process_supported_surface_test.go",
	"internal/serveapp/channel_source_lifecycle_supported_surface_test.go",
	"internal/serveapp/channel_intent_identity_test.go",
}

var originalRules = map[string]bool{
	"TestChannelLearnedObjectPublicJourney": true, "proveObjectChannelRebindRetirement": true,
	"waitObjectChannelDisposition": true, "type/objectChannelProvider": true,
	"TestChannelLearnedObjectInvalidChosenAnswerPublicJourney": true,
	"startObjectChannelJourney":                                true, "proveObjectChannelChooserRestart": true,
	"proveObjectChannelLostPrompt": true, "TestChannelLearnedObjectRealAnchorProducersPublicJourney": true,
	"proveObjectRejectedControl": true, "TestChannelLearnedObjectRecoveryPagingPublicJourney": true,
	"requestObjectRecoveryInbox": true, "proveObjectAllRecoveryChoices": true,
	"proveObjectResponseEditLoss": true, "TestChannelLearnedObjectHumanTemporalReceiptPublicJourney": true,
	"proveChannelInputAuthorityRestart": true, "proveChannelInputCommitDeath": true,
	"assertChannelInputCommit": true, "TestChannelSelectedForkInputBoundaryMatrix": true,
}

var retirements = map[string]bool{
	"waitReceptionistActionRejection": true, "waitChannelTextTeaching": true,
	"waitChannelDraftCallbackRejection": true, "waitChannelRejectedCallback": true,
}

var repairs = map[string]bool{
	"TestChannelOneBotManyConversationsPublicJourney":        true,
	"TestChannelDeliveryUncertainCopyAuthorityPublicJourney": true,
	"assertUncertainCopyRebindFence":                         true, "postUncertainCopyStaleControls": true,
	"TestChannelDraftTerminalRestartPublicJourney":                   true,
	"TestChannelDeliveryHumanTemporalReceiptPublicJourney":           true,
	"TestChannelDeliveryAbruptProcessDeathPublicJourney":             true,
	"TestChannelActionAcknowledgmentAbruptProcessDeathPublicJourney": true,
	"TestChannelSourceLifecyclePublicJourney":                        true, "postChannelSourceControl": true,
}

var bridges = map[string]bool{
	"objectIntentQuery": true, "closeObjectIntentObservation": true,
	"waitChannelInputCommitDispositions": true, "observeChannelDisposition": true,
	"channelDispositionAccepted": true, "captureChannelIntentIdentity": true,
	"channelSourceControlPayload": true, "postChannelSourceControlObserved": true,
}

func main() {
	write := len(os.Args) == 2 && os.Args[1] == "-record"
	if !write && len(os.Args) != 1 {
		panic("usage: migrate-channel-intents [-record]")
	}
	var pins []pin
	for _, path := range paths {
		pins = append(pins, filePins(path)...)
	}
	calls := 0
	for _, row := range pins {
		calls += row.Calls
	}
	if calls != 34 {
		panic(fmt.Sprintf("original channel-intent inventory=%d want34", calls))
	}
	raw, err := json.MarshalIndent(pins, "", "  ")
	must(err)
	raw = append(raw, '\n')
	fmt.Printf("original_calls=%d declarations=%d digest=%x\n", calls, len(pins), sha256.Sum256(raw))
	if write {
		must(os.WriteFile(ledger, raw, 0644))
	}
}

func filePins(path string) []pin {
	beforeBytes, err := exec.Command("git", "show", predecessor+":"+path).Output()
	if path == "internal/serveapp/channel_intent_identity_test.go" {
		if err == nil {
			panic("new identity bridge already existed at predecessor")
		}
		beforeBytes = []byte("package serveapp\n")
	} else {
		must(err)
	}
	afterBytes, err := os.ReadFile(path)
	must(err)
	before, after := declarations(path, beforeBytes), declarations(path, afterBytes)
	for key, current := range after {
		current, err = selectedreadrole.EarlierChannelProcessSource(".", path, key, current)
		must(err)
		after[key], err = channelanchor.ReadPrevious(path, key, current)
		must(err)
	}
	keys := map[string]bool{}
	for key := range before {
		keys[key] = true
	}
	for key := range after {
		if key != "TestChannelNativeDispositionDecisionPreservesAbsencePendingAndFailClosedSettlement" {
			keys[key] = true
		}
	}
	var ordered []string
	for key := range keys {
		ordered = append(ordered, key)
	}
	sort.Strings(ordered)
	var pins []pin
	for _, key := range ordered {
		if before[key] == after[key] {
			continue
		}
		pins = append(pins, classifyPin(path, key, before[key], after[key]))
	}
	return pins
}

func classifyPin(path, key, before, after string) pin {
	row := pin{File: path, Function: key, Before: before, After: after, SHA256: fmt.Sprintf("%x", sha256.Sum256([]byte(before)))}
	switch {
	case originalRules[key]:
		expected, calls, err := channelintent.Transform(before)
		must(err)
		if expected != after {
			panic("original intent caller escaped typed rule: " + path + "/" + key + " " + mismatch(expected, after))
		}
		row.Class, row.Calls = "original-rule", calls
	case retirements[key] && after == "":
		row.Class = "retired-interpreter"
	case repairs[key] && before != "" && after != "":
		row.Class = "approved-sibling-repair"
	case bridges[key] && before == "" && after != "":
		row.Class = "bounded-bridge"
	default:
		panic("unclassified intent declaration change: " + path + "/" + key)
	}
	return row
}

func declarations(path string, source []byte) map[string]string {
	file, err := parser.ParseFile(token.NewFileSet(), path, source, parser.AllErrors)
	must(err)
	out := map[string]string{}
	for _, decl := range file.Decls {
		key := ""
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Recv == nil {
			key = fn.Name.Name
		}
		if group, ok := decl.(*ast.GenDecl); ok && group.Tok == token.TYPE && len(group.Specs) == 1 && group.Specs[0].(*ast.TypeSpec).Name.Name == "objectChannelProvider" {
			key = "type/objectChannelProvider"
		}
		if key != "" {
			if _, duplicate := out[key]; duplicate {
				panic("ambiguous intent declaration: " + key)
			}
			out[key] = channelintent.Node(decl)
		}
	}
	return out
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func mismatch(expected, actual string) string {
	index := 0
	for index < len(expected) && index < len(actual) && expected[index] == actual[index] {
		index++
	}
	return fmt.Sprintf("byte=%d expected=%q actual=%q", index, expected[max(0, index-60):min(len(expected), index+120)], actual[max(0, index-60):min(len(actual), index+120)])
}
