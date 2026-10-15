package selectedreadrole

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
)

const SelectedForkExecutionPredecessor = "bfb2ceb887f97d66f30d1507707cabbef3c6be49"
const SelectedForkExecutionPath = "tools/fixture-codemod/pipeline-observations/family145-selected-fork-execution-preimages.json"
const SelectedForkExecutionFile = "internal/runtime/runforkexecution/execution_test.go"
const selectedForkExecutionFileDigest = "747cdedd21dcdaaeaba3e60ef917c8a9352003726e46b8eee126d8d831df8b29"
const selectedForkExecutionDigest = "1ff09e6a15713b52ee79c0683a0078d71761372be67ca99be5e0c6ca724eaed6"
const selectedForkExecutionDeclarationCount = 14

func ReadSelectedForkExecutionPins(root string) ([]EntityToolPin, error) {
	raw, err := os.ReadFile(filepath.Join(root, SelectedForkExecutionPath))
	if err != nil || fmt.Sprintf("%x", sha256.Sum256(raw)) != selectedForkExecutionFileDigest {
		return nil, fmt.Errorf("selected fork execution complete manifest bytes changed: %v", err)
	}
	key := "channel-process-pins/" + selectedForkExecutionFileDigest
	if cached, ok := sourceCache.Load(key); ok {
		return append([]EntityToolPin(nil), cached.([]EntityToolPin)...), nil
	}
	var pins []EntityToolPin
	if err := json.Unmarshal(raw, &pins); err != nil {
		return nil, err
	}
	canonical, err := json.Marshal(pins)
	if err != nil || len(pins) != selectedForkExecutionDeclarationCount || fmt.Sprintf("%x", sha256.Sum256(canonical)) != selectedForkExecutionDigest {
		return nil, fmt.Errorf("selected fork execution declaration inventory changed: %v", err)
	}
	sourceCache.Store(key, append([]EntityToolPin(nil), pins...))
	return pins, nil
}

func ValidateSelectedForkExecutionCurrent(root string, pins []EntityToolPin) error {
	authenticated, err := ReadSelectedForkExecutionPins(root)
	if err != nil || !reflect.DeepEqual(authenticated, pins) {
		return fmt.Errorf("selected fork execution supplied inventory is not authenticated: %v", err)
	}
	files, key, err := channelProcessCurrentFiles(root, pins)
	if err != nil {
		return err
	}
	key = "channel-process/" + selectedForkExecutionDigest + "/" + key
	if _, ok := sourceCache.Load(key); ok {
		return nil
	}
	current := map[string]map[string]string{}
	for path, raw := range files {
		current[path], err = standingIngressDeclarations(raw)
		if err != nil {
			return err
		}
	}
	previous := map[string]map[string]string{}
	for _, pin := range pins {
		if previous[pin.File] == nil {
			raw, err := exec.Command("git", "-C", root, "show", SelectedForkExecutionPredecessor+":"+pin.File).Output()
			if err != nil {
				return err
			}
			previous[pin.File], err = standingIngressDeclarations(raw)
			if err != nil {
				return err
			}
		}
		if previous[pin.File][pin.Function] != pin.Before || pin.Before == pin.After || pin.Removed != (pin.After == "") {
			return fmt.Errorf("selected fork execution signed declaration changed: %s", pin.Function)
		}
		if current[pin.File][pin.Function] != pin.After {
			return fmt.Errorf("selected fork execution actual source changed: %s", pin.Function)
		}
	}
	sourceCache.Store(key, true)
	return nil
}

func EarlierSelectedForkExecutionSource(root, file, function, current string) (string, error) {
	if file != SelectedForkExecutionFile {
		return current, nil
	}
	pins, err := ReadSelectedForkExecutionPins(root)
	if err != nil {
		return "", err
	}
	for _, pin := range pins {
		if pin.File != file || pin.Function != function {
			continue
		}
		if err := ValidateSelectedForkExecutionCurrent(root, pins); err != nil {
			return "", err
		}
		return SelectedForkExecutionPrevious(pins, file, function, current)
	}
	return current, nil
}

// The caller authenticates the complete pins and actual source once before a
// finite batch comparison. This is the same exact per-declaration inverse.
func SelectedForkExecutionPrevious(pins []EntityToolPin, file, function, current string) (string, error) {
	for _, pin := range pins {
		if pin.File != file || pin.Function != function {
			continue
		}
		if current != pin.After {
			return "", fmt.Errorf("selected fork execution supplied source is not its live endpoint: %s", function)
		}
		return pin.Before, nil
	}
	return current, nil
}

func PriorSelectedForkExecutionHistory(root string, recipes bool) ([]byte, error) {
	path := "tools/fixture-codemod/pipeline-observations/family145-transitions.json"
	if recipes {
		path = "tools/fixture-codemod/pipeline-observations/recipes.json"
	}
	return exec.Command("git", "-C", root, "show", SelectedForkExecutionPredecessor+":"+path).Output()
}

func ComposeSelectedForkExecutionHistory(root string, prior []byte, recipes bool) ([]byte, error) {
	pins, err := ReadSelectedForkExecutionPins(root)
	if err != nil {
		return nil, err
	}
	if err := ValidateSelectedForkExecutionCurrent(root, pins); err != nil {
		return nil, err
	}
	trusted, err := PriorSelectedForkExecutionHistory(root, recipes)
	var rows, signed []map[string]any
	if err != nil || json.Unmarshal(prior, &rows) != nil || json.Unmarshal(trusted, &signed) != nil || !reflect.DeepEqual(rows, signed) {
		return nil, fmt.Errorf("selected fork execution history is not its complete signed predecessor: %v", err)
	}
	for _, pin := range pins {
		before := len(rows)
		if recipes {
			rows, err = advanceEntityToolHistory(rows, pin, true)
		} else {
			rows, err = appendSelectedForkExecutionTransition(rows, pin)
		}
		if err != nil {
			return nil, err
		}
		if recipes && len(rows) > before {
			rows[before]["Family"] = "native-selected-fork-execution-carrier-family145"
		}
	}
	return json.Marshal(rows)
}

func appendSelectedForkExecutionTransition(rows []map[string]any, pin EntityToolPin) ([]map[string]any, error) {
	// Validate the old endpoint without replacing its historical After bytes.
	validation := make([]map[string]any, len(rows))
	for i, row := range rows {
		validation[i] = make(map[string]any, len(row))
		for key, value := range row {
			validation[i][key] = value
		}
	}
	if _, err := advanceEntityToolHistory(validation, pin, false); err != nil {
		return nil, err
	}
	row := map[string]any{"File": pin.File, "Function": pin.Function, "Before": pin.Before, "After": pin.After}
	if pin.Removed {
		row["Removed"] = true
	}
	return append(rows, row), nil
}

func EarlierSelectedForkExecutionHistory(root string, current []byte, recipes bool) ([]byte, error) {
	prior, err := PriorSelectedForkExecutionHistory(root, recipes)
	if err != nil {
		return nil, err
	}
	expected, err := ComposeSelectedForkExecutionHistory(root, prior, recipes)
	if err != nil {
		return nil, err
	}
	var have, want any
	if json.Unmarshal(current, &have) != nil || json.Unmarshal(expected, &want) != nil || !reflect.DeepEqual(have, want) {
		return nil, fmt.Errorf("selected fork execution current history contains foreign changes")
	}
	return prior, nil
}

// Older owners still validate their complete predecessor. Peel this stage only
// when one of its exact new endpoints is actually present in the supplied ledger.
func BeforeSelectedForkExecutionHistory(root string, current []byte, recipes bool) ([]byte, error) {
	var rows []map[string]any
	if err := json.Unmarshal(current, &rows); err != nil {
		return nil, err
	}
	pins, err := ReadSelectedForkExecutionPins(root)
	if err != nil {
		return nil, err
	}
	for _, pin := range pins {
		for _, row := range rows {
			if row["File"] != pin.File || row["Function"] != pin.Function {
				continue
			}
			endpoint := row["After"]
			if recipes && row["Successor"] != nil {
				endpoint = row["Successor"]
			}
			if endpoint == pin.After || pin.Removed && row["Removed"] == true {
				return EarlierSelectedForkExecutionHistory(root, current, recipes)
			}
		}
	}
	return current, nil
}
