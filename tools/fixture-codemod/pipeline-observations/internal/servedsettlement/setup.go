package servedsettlement

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sync"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/budgetdiagnostic"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/channelsettlement"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runphysical"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/selectedreadrole"
)

const SetupPredecessor = "6c51b51d27d0932b6f90029325af3ba6f84f698f"
const SetupOwnerPath = "tools/fixture-codemod/pipeline-observations/family145-served-setup-owner-preimages.json"
const SetupRolePath = "tools/fixture-codemod/pipeline-observations/family145-served-setup-role-preimages.json"
const SetupMergedDigest = "c6a4fbc3de081d4cdac57652548613c805e89b3dfd995c6cf7fe79c7f5563c26"

var setupBodyCache sync.Map
var setupHistoryCache sync.Map
var setupPinCache sync.Map

var setupStages = []struct {
	path, digest string
	count        int
}{
	{SetupOwnerPath, "b6c64415787cef265ba2a50a08e6c8cdbe9df1829cb5ff2f71a1d50e5f8bbcb8", 4},
	{SetupRolePath, "e8921f93982c571ac22d582f5bdedee63982789bb8f6b92f87ebb72327d46967", 101},
}

// Always read actual bytes; only their deterministic parse result is reusable.
func setupCurrentBody(raw []byte, name string) (string, error) {
	key := fmt.Sprintf("%x/%s", sha256.Sum256(raw), name)
	if body, ok := setupBodyCache.Load(key); ok {
		return body.(string), nil
	}
	body, err := UniqueBody(raw, name)
	if err == nil {
		setupBodyCache.Store(key, body)
	}
	return body, err
}

func ReadSetupPins(root string) ([]runphysical.Pin, error) {
	inputs := make([][]byte, len(setupStages))
	for index, stage := range setupStages {
		raw, err := os.ReadFile(filepath.Join(root, stage.path))
		if err != nil || fmt.Sprintf("%x", sha256.Sum256(raw)) != stage.digest {
			return nil, fmt.Errorf("served setup immutable stage changed: %s: %v", stage.path, err)
		}
		inputs[index] = raw
	}
	// Every input was just read/authenticated. Only immutable decoding and
	// composition are reused, never a current-source validation result.
	if cached, ok := setupPinCache.Load(SetupMergedDigest); ok {
		return slices.Clone(cached.([]runphysical.Pin)), nil
	}
	merged := map[string]runphysical.Pin{}
	for index, stage := range setupStages {
		var pins []runphysical.Pin
		if err := json.Unmarshal(inputs[index], &pins); err != nil || len(pins) != stage.count {
			return nil, fmt.Errorf("served setup stage inventory changed: %s", stage.path)
		}
		seen := map[string]bool{}
		for _, pin := range pins {
			key := pin.File + "/" + pin.Function
			if seen[key] || pin.Before == pin.After || pin.After == "" || fmt.Sprintf("%x", sha256.Sum256([]byte(pin.Before))) != pin.SHA256 {
				return nil, fmt.Errorf("served setup repeated/changed complete predecessor: %s", key)
			}
			seen[key] = true
			if previous, ok := merged[key]; ok {
				if previous.After != pin.Before {
					return nil, fmt.Errorf("served setup intermediate link changed: %s", key)
				}
				pin.Before, pin.SHA256 = previous.Before, previous.SHA256
			}
			merged[key] = pin
		}
	}
	var keys []string
	for key := range merged {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	var pins []runphysical.Pin
	for _, key := range keys {
		pins = append(pins, merged[key])
	}
	raw, err := json.MarshalIndent(pins, "", "  ")
	if err != nil || len(pins) != 104 || fmt.Sprintf("%x", sha256.Sum256(append(raw, '\n'))) != SetupMergedDigest {
		return nil, fmt.Errorf("served setup complete stage composition changed")
	}
	setupPinCache.Store(SetupMergedDigest, slices.Clone(pins))
	return pins, nil
}

func ValidateSetupCurrentSource(root string, pins []runphysical.Pin) error {
	files := map[string][]byte{}
	for _, pin := range pins {
		if files[pin.File] == nil {
			raw, err := os.ReadFile(filepath.Join(root, pin.File))
			if err != nil {
				return err
			}
			raw, err = selectedreadrole.CatchUp2542HistoricalSource(root, pin.File, raw)
			if err != nil {
				return err
			}
			files[pin.File] = raw
		}
		current, err := setupCurrentBody(files[pin.File], pin.Function)
		if err == nil {
			current, err = selectedreadrole.EarlierSource(root, pin.File, pin.Function, current)
		}
		if err == nil {
			current, err = channelsettlement.EarlierSource(root, pin.File, pin.Function, current)
		}
		if err != nil || current != pin.After {
			return fmt.Errorf("served setup actual current declaration changed: %s: %v", pin.Function, err)
		}
	}
	return nil
}

func SetupEarlierSource(root, file, function, current string) (string, error) {
	current, err := setupPredecessorSource(root, file, function, current)
	if err != nil {
		return "", err
	}
	pins, err := ReadSetupPins(root)
	if err != nil {
		return "", err
	}
	for _, pin := range pins {
		if pin.File == file && pin.Function == function {
			if err := ValidateSetupCurrentSource(root, pins); err != nil {
				return "", err
			}
			return EarlierSource(pins, file, function, current)
		}
	}
	return current, nil
}

func setupPredecessorSource(root, file, function, current string) (string, error) {
	current, err := selectedreadrole.EarlierSource(root, file, function, current)
	if err != nil {
		return "", err
	}
	current, err = budgetdiagnostic.EarlierSource(root, file, function, current)
	if err != nil {
		return "", err
	}
	return channelsettlement.EarlierSource(root, file, function, current)
}

func ComposeSetupHistory(prior []byte, pins []runphysical.Pin, recipes bool) ([]byte, error) {
	want, count := "a3383821bfea13afac8e9f2a0b5ebd5f031bdd7c2202ab3be98e18229299fa34", 837
	if recipes {
		want, count = "7571bb7e424297f1a3ea0ef650ea314d20d2eae9521a956c93748efc6e54d23d", 1532
	}
	if fmt.Sprintf("%x", sha256.Sum256(prior)) != want {
		return nil, fmt.Errorf("served setup historical input differs from signed predecessor")
	}
	raw, err := json.MarshalIndent(pins, "", "  ")
	if err != nil || len(pins) != 104 || fmt.Sprintf("%x", sha256.Sum256(append(raw, '\n'))) != SetupMergedDigest {
		return nil, fmt.Errorf("served setup history lacks the complete finite inventory")
	}
	cacheKey := fmt.Sprintf("%t/%s/%s", recipes, want, SetupMergedDigest)
	if encoded, ok := setupHistoryCache.Load(cacheKey); ok {
		return append([]byte{}, encoded.([]byte)...), nil
	}
	var rows []batchHistoryRow
	if err := json.Unmarshal(prior, &rows); err != nil || len(rows) != count {
		return nil, fmt.Errorf("served setup historical row inventory changed")
	}
	appends := map[string]bool{}
	for _, pin := range pins {
		matches := 0
		for _, row := range rows {
			if row["File"] == pin.File && row["Function"] == pin.Function {
				matches++
			}
		}
		if matches == 0 {
			appends[pin.Function] = true
		}
	}
	expected := 34
	if recipes {
		expected = 32
	}
	if len(appends) != expected {
		return nil, fmt.Errorf("served setup exact append set changed")
	}
	for _, pin := range pins {
		pin, err = setupHistorySourceSpelling(rows, pin, recipes)
		if err != nil {
			return nil, err
		}
		rows, err = composeBatchPinWithAppends(rows, pin, recipes, appends)
		if err != nil {
			return nil, err
		}
	}
	encoded, err := json.MarshalIndent(rows, "", "  ")
	if err == nil {
		setupHistoryCache.Store(cacheKey, append([]byte{}, encoded...))
	}
	return encoded, err
}

// This one older endpoint preserves comments/formatting. Its original bytes and
// metadata remain in the signed prior ledger; only AST-identical spelling bridges.
func setupHistorySourceSpelling(rows []batchHistoryRow, pin runphysical.Pin, recipes bool) (runphysical.Pin, error) {
	if !recipes || pin.File != "internal/serveapp/fork_delivery_route_evidence_test.go" || pin.Function != "TestServedForkConnectedDeliveryRouteEvidenceOnBothStores" {
		return pin, nil
	}
	for _, row := range rows {
		if row["File"] != pin.File || row["Function"] != pin.Function {
			continue
		}
		current, _ := row["After"].(string)
		if successor, ok := row["Successor"].(string); ok {
			current = successor
		}
		normal, err := requestkey.Normalize(current)
		if err != nil || normal != pin.Before {
			return pin, fmt.Errorf("served setup historical route source changed beyond spelling")
		}
		pin.Before = current
	}
	return pin, nil
}

func RestoreSetupHistory(current, prior []byte, pins []runphysical.Pin, recipes bool) ([]byte, error) {
	expected, err := ComposeSetupHistory(prior, pins, recipes)
	if err != nil {
		return nil, err
	}
	var actual, want []batchHistoryRow
	if err := json.Unmarshal(current, &actual); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(expected, &want); err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(actual, want) {
		return nil, fmt.Errorf("served setup history changed endpoints, metadata, inventory or order")
	}
	return append([]byte{}, prior...), nil
}
