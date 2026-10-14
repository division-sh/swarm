package selectedreadrole

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sync"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runphysical"
)

const ReadbackPredecessor = "19de2c2e1fc57bcf4b9264c738db22de51742195"
const ReadbackPath = "tools/fixture-codemod/pipeline-observations/family145-receiver-publication-readback-preimages.json"
const ReadbackDigest = "6e8a242bbac3092e8885c8792b899a693c4495d24cc5ccac53f93b2debadffee"

var readbackPinCache sync.Map

var readbackFunctions = map[string][]string{
	"internal/serveapp/publication_activity_test.go":                  {"TestServedGeneratedActivityPublicationBothStores", "requireActivityPublicationReadback"},
	"internal/serveapp/fork_loop_generation_notice_test.go":           {"TestServedForkLoopGenerationStateEffectBothStores", "requireForkLoopStateEffect"},
	"internal/serveapp/fork_loop_accumulator_test.go":                 {"TestServedForkAccumulatorRetainedGenerationBothStores"},
	"internal/serveapp/receiver_agent_collision_test.go":              {"TestServedReceiverAgentBusinessNamespaceBothStores", "requireServedReceiverAgentCollision"},
	"internal/serveapp/connection_edge_policy_test.go":                {"TestEdgeOwnedConnectionPoliciesSharingInputBothStores", "TestEdgeOwnedMixedConnectionProjectionsBothStores", "requireMixedConnectionReadback", "requireConnectionPolicyRoutes"},
	"internal/serveapp/fork_historical_context_test.go":               {"TestRunForkHistoricalIdentityPublicExecutionBothStores"},
	"internal/serveapp/fork_receiver_acquisition_effect_only_test.go": {"requireSupplementalForkAcquisitionSettlement", "TestSelectedForkSupplementalReceiverAcquisitionWithoutPostRevisionEmissionBothStores"},
	"internal/serveapp/fork_receiver_execution_effect_test.go":        {"requireForkReceiverExecution"},
	"internal/serveapp/fork_receiver_ownership_test.go":               {"requireForkReceiverDelivery", "requireForkReceiverEffect", "runForkReceiverOwnershipJourney", "requireForkReceiverBusinessMutation", "TestSelectedForkIndependentReceiverBusinessMutationBothStores"},
	"internal/serveapp/publication_sites_test.go":                     {"TestServedPublicationDirectRestartBothStores", "proveServedPublicationSites", "waitPublicationSiteFanOut", "requirePublicationSiteReadback"},
	"internal/serveapp/semantic_numeric_activity_test.go":             {"proveActivitySemanticResultExecutionParity", "requireNumericMockForkOutput"},
	"internal/serveapp/semantic_numeric_gate_test.go":                 {"TestWorkflowGateSemanticNumericOutcomeExecutionParity"},
	"internal/serveapp/semantic_numeric_ingress_test.go":              {"TestServedSemanticNumericIdempotencyReplay", "TestServedSemanticNumericEventSubscription", "semanticNumericOutput"},
	"internal/serveapp/semantic_numeric_scenario_test.go":             {"startSemanticNumericRuntime"},
	"internal/serveapp/semantic_numeric_schedule_test.go":             {"TestGenericScheduleSemanticPayloadExecutionParity"},
}

func ReadbackFunction(file, function string) bool {
	for _, name := range readbackFunctions[file] {
		if name == function {
			return true
		}
	}
	return false
}

func ReadReadbackPins(root string) ([]runphysical.Pin, error) {
	raw, err := os.ReadFile(filepath.Join(root, ReadbackPath))
	if err != nil || fmt.Sprintf("%x", sha256.Sum256(raw)) != ReadbackDigest {
		return nil, fmt.Errorf("receiver/publication immutable declaration inventory changed: %v", err)
	}
	if cached, ok := readbackPinCache.Load(ReadbackDigest); ok {
		return append([]runphysical.Pin(nil), cached.([]runphysical.Pin)...), nil
	}
	var pins []runphysical.Pin
	if err := json.Unmarshal(raw, &pins); err != nil || len(pins) != 32 {
		return nil, fmt.Errorf("receiver/publication declaration count changed: %v", err)
	}
	seen := map[string]bool{}
	for _, pin := range pins {
		key := pin.File + "/" + pin.Function
		if !ReadbackFunction(pin.File, pin.Function) || seen[key] || pin.Before == "" || pin.After == "" || pin.Before == pin.After || fmt.Sprintf("%x", sha256.Sum256([]byte(pin.Before))) != pin.SHA256 {
			return nil, fmt.Errorf("receiver/publication signed declaration changed: %s", key)
		}
		seen[key] = true
	}
	readbackPinCache.Store(ReadbackDigest, append([]runphysical.Pin(nil), pins...))
	return pins, nil
}

func ValidateReadbackCurrent(root string, pins []runphysical.Pin) error {
	authenticated, err := ReadReadbackPins(root)
	if err != nil || !reflect.DeepEqual(authenticated, pins) {
		return fmt.Errorf("receiver/publication supplied inventory is not authenticated: %v", err)
	}
	files := map[string][]byte{}
	hash := sha256.New()
	for _, pin := range pins {
		if files[pin.File] != nil {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(root, pin.File))
		if err != nil {
			return err
		}
		files[pin.File] = raw
		fmt.Fprintf(hash, "%s:%x\n", pin.File, sha256.Sum256(raw))
	}
	key := fmt.Sprintf("readback:%s:%s:%x", root, ReadbackDigest, hash.Sum(nil))
	if _, ok := sourceCache.Load(key); ok {
		return nil
	}
	for _, pin := range pins {
		body, err := Body(files[pin.File], pin.Function)
		if err != nil || body != pin.After {
			return fmt.Errorf("receiver/publication actual declaration changed: %s: %v", pin.Function, err)
		}
	}
	sourceCache.Store(key, true)
	return nil
}

func EarlierReadbackSource(root, file, function, current string) (string, error) {
	if !ReadbackFunction(file, function) {
		return current, nil
	}
	pins, err := ReadReadbackPins(root)
	if err != nil {
		return "", err
	}
	if err := ValidateReadbackCurrent(root, pins); err != nil {
		return "", err
	}
	for _, pin := range pins {
		if pin.File == file && pin.Function == function && current == pin.After {
			return pin.Before, nil
		}
	}
	return "", fmt.Errorf("receiver/publication supplied source is not its live endpoint: %s", function)
}

func PriorReadbackHistory(root string, recipes bool) ([]byte, error) {
	path := "tools/fixture-codemod/pipeline-observations/family145-transitions.json"
	if recipes {
		path = "tools/fixture-codemod/pipeline-observations/recipes.json"
	}
	return exec.Command("git", "-C", root, "show", ReadbackPredecessor+":"+path).Output()
}

func ComposeReadbackHistory(root string, prior []byte, recipes bool) ([]byte, error) {
	signed, err := PriorReadbackHistory(root, recipes)
	if err != nil || !bytes.Equal(signed, prior) {
		return nil, fmt.Errorf("receiver/publication history is not its complete signed predecessor: %v", err)
	}
	pins, err := ReadReadbackPins(root)
	if err != nil {
		return nil, err
	}
	var rows []map[string]any
	if err := json.Unmarshal(prior, &rows); err != nil {
		return nil, err
	}
	appends := 0
	for _, pin := range pins {
		var added bool
		rows, added, err = advanceReadback(rows, pin, recipes)
		if err != nil {
			return nil, err
		}
		if added {
			appends++
		}
	}
	want := 8
	if recipes {
		want = 6
	}
	if appends != want {
		return nil, fmt.Errorf("receiver/publication exact append inventory changed: %d/%d", appends, want)
	}
	raw, err := json.MarshalIndent(rows, "", "  ")
	return append(raw, '\n'), err
}

func advanceReadback(rows []map[string]any, pin runphysical.Pin, recipes bool) ([]map[string]any, bool, error) {
	if recipes && pin.File == "internal/serveapp/semantic_numeric_scenario_test.go" && pin.Function == "startSemanticNumericRuntime" {
		for _, row := range rows {
			if row["File"] != pin.File || row["Function"] != pin.Function {
				continue
			}
			endpoint, _ := row["After"].(string)
			if row["Successor"] != nil {
				return nil, false, fmt.Errorf("numeric constructor already has an unrelated successor")
			}
			normal, err := requestkey.Normalize(endpoint)
			if err != nil || normal != pin.Before {
				return nil, false, fmt.Errorf("numeric constructor's old layout is not AST-identical to the signed source")
			}
			// Match the immutable old spelling; advance changes only Successor.
			pin.Before = endpoint
		}
	}
	return advance(rows, pin, recipes)
}

func EarlierReadbackHistory(root string, current []byte, recipes bool) ([]byte, error) {
	prior, err := PriorReadbackHistory(root, recipes)
	if err != nil {
		return nil, err
	}
	if equalProcessHistory(current, prior) {
		return prior, nil
	}
	pins, err := ReadReadbackPins(root)
	if err != nil {
		return nil, err
	}
	if err := ValidateReadbackCurrent(root, pins); err != nil {
		return nil, err
	}
	expected, err := ComposeReadbackHistory(root, prior, recipes)
	if err != nil || !equalProcessHistory(current, expected) {
		return nil, fmt.Errorf("receiver/publication history changed fields, membership or order: %v", err)
	}
	return prior, nil
}
