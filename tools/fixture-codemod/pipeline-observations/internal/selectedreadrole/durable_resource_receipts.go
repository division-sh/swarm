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

const DurableResourcePredecessor = "50c5c207de5b6986237d3f40a41c9adcfd03aca1"
const DurableResourcePath = "tools/fixture-codemod/pipeline-observations/family145-durable-resource-preimages.json"
const durableResourceFileDigest = "31775fc97f2e77ae238519a0cc523a12aa0fc4a88349aca0b3610f6b9a3b3f4b"
const durableResourceDigest = "f11019c6d95fe6fc2b15766cdfcd63735d1b1c906b57baf87db8d6bb4b94b9c9"
const durableResourceDeclarationCount = 51

func ReadDurableResourcePins(root string) ([]EntityToolPin, error) {
	raw, err := os.ReadFile(filepath.Join(root, DurableResourcePath))
	if err != nil || fmt.Sprintf("%x", sha256.Sum256(raw)) != durableResourceFileDigest {
		return nil, fmt.Errorf("durable resource complete manifest bytes changed: %v", err)
	}
	key := "channel-process-pins/" + durableResourceFileDigest
	if cached, ok := sourceCache.Load(key); ok {
		return append([]EntityToolPin(nil), cached.([]EntityToolPin)...), nil
	}
	var pins []EntityToolPin
	if err := json.Unmarshal(raw, &pins); err != nil {
		return nil, err
	}
	canonical, err := json.Marshal(pins)
	if err != nil || len(pins) != durableResourceDeclarationCount || fmt.Sprintf("%x", sha256.Sum256(canonical)) != durableResourceDigest {
		return nil, fmt.Errorf("durable resource declaration inventory changed: %v", err)
	}
	sourceCache.Store(key, append([]EntityToolPin(nil), pins...))
	return pins, nil
}

func ValidateDurableResourceCurrent(root string, pins []EntityToolPin) error {
	authenticated, err := ReadDurableResourcePins(root)
	if err != nil || !reflect.DeepEqual(authenticated, pins) {
		return fmt.Errorf("durable resource supplied inventory is not authenticated: %v", err)
	}
	files, key, err := channelProcessCurrentFiles(root, pins)
	if err != nil {
		return err
	}
	key = "channel-process/" + durableResourceDigest + "/" + key
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
			raw, err := exec.Command("git", "-C", root, "show", DurableResourcePredecessor+":"+pin.File).Output()
			if err != nil {
				return err
			}
			previous[pin.File], err = standingIngressDeclarations(raw)
			if err != nil {
				return err
			}
		}
		if previous[pin.File][pin.Function] != pin.Before || pin.Before == pin.After || pin.Removed != (pin.After == "") {
			return fmt.Errorf("durable resource signed declaration changed: %s", pin.Function)
		}
		if current[pin.File][pin.Function] != pin.After {
			return fmt.Errorf("durable resource actual source changed: %s", pin.Function)
		}
	}
	sourceCache.Store(key, true)
	return nil
}

func EarlierDurableResourceSource(root, file, function, current string) (string, error) {
	var err error
	current, err = EarlierSelectedForkExecutionSource(root, file, function, current)
	if err != nil {
		return "", err
	}
	pins, err := ReadDurableResourcePins(root)
	if err != nil {
		return "", err
	}
	for _, pin := range pins {
		if pin.File != file || pin.Function != function {
			continue
		}
		if err := ValidateDurableResourceCurrent(root, pins); err != nil {
			return "", err
		}
		return DurableResourcePrevious(pins, file, function, current)
	}
	return current, nil
}

// The caller authenticates the complete pins and actual source once before a
// finite batch comparison. This is the same exact per-declaration inverse.
func DurableResourcePrevious(pins []EntityToolPin, file, function, current string) (string, error) {
	for _, pin := range pins {
		if pin.File != file || pin.Function != function {
			continue
		}
		if current != pin.After {
			return "", fmt.Errorf("durable resource supplied source is not its live endpoint: %s", function)
		}
		return pin.Before, nil
	}
	return current, nil
}

func PriorDurableResourceHistory(root string, recipes bool) ([]byte, error) {
	path := "tools/fixture-codemod/pipeline-observations/family145-transitions.json"
	if recipes {
		path = "tools/fixture-codemod/pipeline-observations/recipes.json"
	}
	return exec.Command("git", "-C", root, "show", DurableResourcePredecessor+":"+path).Output()
}

func ComposeDurableResourceHistory(root string, prior []byte, recipes bool) ([]byte, error) {
	pins, err := ReadDurableResourcePins(root)
	if err != nil {
		return nil, err
	}
	if err := ValidateDurableResourceCurrent(root, pins); err != nil {
		return nil, err
	}
	trusted, err := PriorDurableResourceHistory(root, recipes)
	var rows, signed []map[string]any
	if err != nil || json.Unmarshal(prior, &rows) != nil || json.Unmarshal(trusted, &signed) != nil || !reflect.DeepEqual(rows, signed) {
		return nil, fmt.Errorf("durable resource history is not its complete signed predecessor: %v", err)
	}
	for _, pin := range pins {
		before := len(rows)
		if recipes {
			rows, err = advanceEntityToolHistory(rows, pin, true)
		} else {
			rows, err = appendDurableResourceTransition(rows, pin)
		}
		if err != nil {
			return nil, err
		}
		if recipes && len(rows) > before {
			rows[before]["Family"] = "native-durable-resource-carrier-family145"
		}
	}
	return json.Marshal(rows)
}

func appendDurableResourceTransition(rows []map[string]any, pin EntityToolPin) ([]map[string]any, error) {
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

func EarlierDurableResourceHistory(root string, current []byte, recipes bool) ([]byte, error) {
	var err error
	current, err = BeforeSelectedForkExecutionHistory(root, current, recipes)
	if err != nil {
		return nil, err
	}
	prior, err := PriorDurableResourceHistory(root, recipes)
	if err != nil {
		return nil, err
	}
	expected, err := ComposeDurableResourceHistory(root, prior, recipes)
	if err != nil {
		return nil, err
	}
	var have, want any
	if json.Unmarshal(current, &have) != nil || json.Unmarshal(expected, &want) != nil || !reflect.DeepEqual(have, want) {
		return nil, fmt.Errorf("durable resource current history contains foreign changes")
	}
	return prior, nil
}

// Older owners still validate their complete predecessor. Peel this stage only
// when one of its exact new endpoints is actually present in the supplied ledger.
func BeforeDurableResourceHistory(root string, current []byte, recipes bool) ([]byte, error) {
	var err error
	current, err = BeforeSelectedForkExecutionHistory(root, current, recipes)
	if err != nil {
		return nil, err
	}
	var rows []map[string]any
	if err := json.Unmarshal(current, &rows); err != nil {
		return nil, err
	}
	pins, err := ReadDurableResourcePins(root)
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
				return EarlierDurableResourceHistory(root, current, recipes)
			}
		}
	}
	return current, nil
}
