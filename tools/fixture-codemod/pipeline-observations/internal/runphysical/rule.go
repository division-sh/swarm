package runphysical

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
)

const Predecessor = "50315ab6755c15d6137fa5cf4186c935bc2c5c24"
const Path = "tools/fixture-codemod/pipeline-observations/family145-run-physical-preimages.json"
const Digest = "d7e775886f9b93aade33f1a0ed139fef3af4a53f8337894e3c3b11c1c419c114"

type Pin struct {
	File, Function, Before, After, SHA256 string
	Calls                                 int
}

func Decode(raw []byte) ([]Pin, error) {
	if fmt.Sprintf("%x", sha256.Sum256(raw)) != Digest {
		return nil, fmt.Errorf("physical run count complete source manifest changed")
	}
	var pins []Pin
	if err := json.Unmarshal(raw, &pins); err != nil {
		return nil, err
	}
	if len(pins) != 8 {
		return nil, fmt.Errorf("physical run count declaration inventory changed")
	}
	return pins, nil
}

func Previous(pins []Pin, file, function, current string) string {
	for _, pin := range pins {
		if pin.File == file && pin.Function == function && current == pin.After {
			return pin.Before
		}
	}
	return current
}

type Edit struct{ Before, After string }
type Rule struct {
	File, Function string
	Edits          []Edit
	Calls          int
}

var Rules = []Rule{
	{"internal/serveapp/lifecycle_transition_served_test.go", "TestServedCompiledTransitionSelectedCarrierEvidenceOnBothStores", fieldCount("entities", "seed.RunID", "COUNT"), 1},
	{"internal/serveapp/lifecycle_transition_nested_test.go", "TestServedCompiledTransitionNestedCarrierCollisionOnBothStores", fieldCount("entities", "runID", "COUNT"), 1},
	{"internal/serveapp/lifecycle_transition_template_test.go", "TestServedCompiledTransitionNestedTemplatesFirstJourneyOnBothStores", fieldCount("count", "runID", "COUNT"), 1},
	{"internal/serveapp/receiver_external_onward_test.go", "TestReceiverCompositionEntitylessRootExportBothStores", fieldCount("entities", "published.RunID", "count"), 1},
	{"internal/serveapp/constructed_decision_deferral_test.go", "TestServedFieldlessGateDeferralBothStores", fieldCount("fieldRows", "seed.RunID", "COUNT"), 1},
	{"internal/serveapp/materializing_sender_test.go", "TestProspectiveTerminalReceiverRefusesWithoutMutationOrPublicationBothStores", []Edit{
		{"var states, publications, failed int", "var publications, failed int"},
		{rawFieldCount("states", "seed.RunID", "count"), "states, err := readServedRunFieldRows(rt.ReceiverStateReader, seed.RunID)\nif err"},
	}, 1},
	{"internal/serveapp/receiver_fork_test.go", "TestReceiverCompositionForkBothStores", []Edit{
		{"var count int\n\t\t\t\t\t" + rawFieldCount("count", "request.RunID", "COUNT"), "count, err := readServedRunFieldRows(rt.ReceiverStateReader, request.RunID)\nif err"},
		{"var headers, companions int", "var companions int"},
		{"if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM flow_instances WHERE run_id=$1`, forkRunID).Scan(&headers); err", "headers, err := readServedRunConstructionHeaders(rt.ReceiverStateReader, forkRunID)\nif err"},
		{rawFieldCount("companions", "forkRunID", "COUNT"), "companions, err = readServedRunFieldRows(rt.ReceiverStateReader, forkRunID)\nif err"},
	}, 3},
	{"internal/runtime/cataloge2e/receiver_materialization_geometry_test.go", "TestReceiverMaterializationRootPreflightBothStores", []Edit{
		{"if err := h.db.QueryRowContext(h.ctx, `SELECT COUNT(*) FROM flow_instances WHERE run_id=$1`, foreign.RunID()).Scan(&count); err", "count, err = storetest.CountRunFlowInstanceStorage(h.ctx, h.rt.Bus.Store(), foreign.RunID())\nif err"},
	}, 1},
}

func fieldCount(destination, run, spelling string) []Edit {
	return []Edit{
		{"var " + destination + " int", ""},
		{rawFieldCount(destination, run, spelling), destination + ", err := readServedRunFieldRows(rt.ReceiverStateReader, " + run + ")\nif err"},
	}
}

func rawFieldCount(destination, run, spelling string) string {
	return "if err := rt.DB.QueryRow(`SELECT " + spelling + "(*) FROM entity_state WHERE run_id=$1`, " + run + ").Scan(&" + destination + "); err"
}

func Transform(rule Rule, before string) (string, error) {
	after, err := ApplyRaw(rule, before)
	if err != nil {
		return "", err
	}
	return requestkey.Normalize(after)
}

func ApplyRaw(rule Rule, before string) (string, error) {
	after := before
	for _, edit := range rule.Edits {
		if strings.Count(after, edit.Before) != 1 {
			return "", fmt.Errorf("physical run count rule has no unique predecessor: %s/%s: %q", rule.File, rule.Function, edit.Before)
		}
		after = strings.Replace(after, edit.Before, edit.After, 1)
	}
	return after, nil
}
