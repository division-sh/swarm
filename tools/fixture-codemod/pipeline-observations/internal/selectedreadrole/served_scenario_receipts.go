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

const ServedScenarioPredecessor = "07e3cce858308112e45070ec35d5c73983c81460"
const ServedScenarioPath = "tools/fixture-codemod/pipeline-observations/family145-served-scenario-preimages.json"
const ServedScenarioFile = "internal/serveapp/test_command_runtime_test.go"
const ServedScenarioStartupFile = "internal/runtime/template_instance_delivery_test.go"
const servedScenarioFileDigest = "4040570dee0ab72b903aeebaeb4d10fee10b40f4428a5172765aaf3beba459c8"
const servedScenarioDigest = "a2d0bad5b4bd453e396b44d626a46c22d41992460fff3daa8bdaa8d9a197803c"
const servedScenarioDeclarationCount = 22

func ReadServedScenarioPins(root string) ([]EntityToolPin, error) {
	raw, err := os.ReadFile(filepath.Join(root, ServedScenarioPath))
	if err != nil || fmt.Sprintf("%x", sha256.Sum256(raw)) != servedScenarioFileDigest {
		return nil, fmt.Errorf("served scenario complete manifest bytes changed: %v", err)
	}
	key := "channel-process-pins/" + servedScenarioFileDigest
	if cached, ok := sourceCache.Load(key); ok {
		return append([]EntityToolPin(nil), cached.([]EntityToolPin)...), nil
	}
	var pins []EntityToolPin
	if err := json.Unmarshal(raw, &pins); err != nil {
		return nil, err
	}
	canonical, err := json.Marshal(pins)
	if err != nil || len(pins) != servedScenarioDeclarationCount || fmt.Sprintf("%x", sha256.Sum256(canonical)) != servedScenarioDigest {
		return nil, fmt.Errorf("served scenario declaration inventory changed: %v", err)
	}
	sourceCache.Store(key, append([]EntityToolPin(nil), pins...))
	return pins, nil
}

func ValidateServedScenarioCurrent(root string, pins []EntityToolPin) error {
	authenticated, err := ReadServedScenarioPins(root)
	if err != nil || !reflect.DeepEqual(authenticated, pins) {
		return fmt.Errorf("served scenario supplied inventory is not authenticated: %v", err)
	}
	files, key, err := channelProcessCurrentFiles(root, pins)
	if err != nil {
		return err
	}
	key = "channel-process/" + servedScenarioDigest + "/" + key
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
			raw, err := exec.Command("git", "-C", root, "show", ServedScenarioPredecessor+":"+pin.File).Output()
			if err != nil {
				return err
			}
			previous[pin.File], err = standingIngressDeclarations(raw)
			if err != nil {
				return err
			}
		}
		if previous[pin.File][pin.Function] != pin.Before || pin.Before == pin.After || pin.Removed != (pin.After == "") {
			return fmt.Errorf("served scenario signed declaration changed: %s", pin.Function)
		}
		if current[pin.File][pin.Function] != pin.After {
			return fmt.Errorf("served scenario actual source changed: %s", pin.Function)
		}
	}
	sourceCache.Store(key, true)
	return nil
}

func EarlierServedScenarioSource(root, file, function, current string) (string, error) {
	if file != ServedScenarioFile && file != ServedScenarioStartupFile && file != "internal/serveapp/root_publication_ack_loss_test.go" && file != "internal/store/testsql/event.go" {
		return current, nil
	}
	pins, err := ReadServedScenarioPins(root)
	if err != nil {
		return "", err
	}
	for _, pin := range pins {
		if pin.File != file || pin.Function != function {
			continue
		}
		if err := ValidateServedScenarioCurrent(root, pins); err != nil {
			return "", err
		}
		return ServedScenarioPrevious(pins, file, function, current)
	}
	return current, nil
}

// The caller authenticates the complete pins and actual source once before a
// finite batch comparison. This is the same exact per-declaration inverse.
func ServedScenarioPrevious(pins []EntityToolPin, file, function, current string) (string, error) {
	for _, pin := range pins {
		if pin.File != file || pin.Function != function {
			continue
		}
		if current != pin.After {
			return "", fmt.Errorf("served scenario supplied source is not its live endpoint: %s", function)
		}
		return pin.Before, nil
	}
	return current, nil
}

func PriorServedScenarioHistory(root string, recipes bool) ([]byte, error) {
	path := "tools/fixture-codemod/pipeline-observations/family145-transitions.json"
	if recipes {
		path = "tools/fixture-codemod/pipeline-observations/recipes.json"
	}
	return exec.Command("git", "-C", root, "show", ServedScenarioPredecessor+":"+path).Output()
}

func ComposeServedScenarioHistory(root string, prior []byte, recipes bool) ([]byte, error) {
	pins, err := ReadServedScenarioPins(root)
	if err != nil {
		return nil, err
	}
	if err := ValidateServedScenarioCurrent(root, pins); err != nil {
		return nil, err
	}
	trusted, err := PriorServedScenarioHistory(root, recipes)
	var rows, signed []map[string]any
	if err != nil || json.Unmarshal(prior, &rows) != nil || json.Unmarshal(trusted, &signed) != nil || !reflect.DeepEqual(rows, signed) {
		return nil, fmt.Errorf("served scenario history is not its complete signed predecessor: %v", err)
	}
	for _, pin := range pins {
		before := len(rows)
		if recipes {
			rows, err = advanceEntityToolHistory(rows, pin, true)
		} else {
			rows, err = appendServedScenarioTransition(rows, pin)
		}
		if err != nil {
			return nil, err
		}
		if recipes && len(rows) > before {
			rows[before]["Family"] = "native-served-scenario-carrier-family145"
		}
	}
	return json.Marshal(rows)
}

func appendServedScenarioTransition(rows []map[string]any, pin EntityToolPin) ([]map[string]any, error) {
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

func EarlierServedScenarioHistory(root string, current []byte, recipes bool) ([]byte, error) {
	prior, err := PriorServedScenarioHistory(root, recipes)
	if err != nil {
		return nil, err
	}
	expected, err := ComposeServedScenarioHistory(root, prior, recipes)
	if err != nil {
		return nil, err
	}
	var have, want any
	if json.Unmarshal(current, &have) != nil || json.Unmarshal(expected, &want) != nil || !reflect.DeepEqual(have, want) {
		return nil, fmt.Errorf("served scenario current history contains foreign changes")
	}
	return prior, nil
}

// Older owners still validate their complete predecessor. Peel this stage only
// when one of its exact new endpoints is actually present in the supplied ledger.
func BeforeServedScenarioHistory(root string, current []byte, recipes bool) ([]byte, error) {
	var rows []map[string]any
	if err := json.Unmarshal(current, &rows); err != nil {
		return nil, err
	}
	pins, err := ReadServedScenarioPins(root)
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
				return EarlierServedScenarioHistory(root, current, recipes)
			}
		}
	}
	return current, nil
}
