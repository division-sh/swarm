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

const AgentSnapshotPredecessor = "11d5e9ef4b0cd967d25448bba87ccb86d39ffac3"
const AgentSnapshotFile = "internal/apiv1/agent_snapshot_atomicity_test.go"
const AgentSnapshotPath = "tools/fixture-codemod/pipeline-observations/family145-agent-snapshot-preimages.json"
const AgentSnapshotDigest = "e7bae37c583d9e6d9f5ff869e0c0f6039941584f80b225ed6ea8f1d54a8589ea"
const agentSnapshotFileDigest = "ae3d3b2fb754b3d4dfcae1da46fcc95ec0ff4ebc1f446fd34bdbb37adc894193"
const AgentSnapshotSourceDigest = "02b0f2dba8e136b66272c1d9a76f14faa96437056bb3c7c3a4f9ae125e07089c"

func ReadAgentSnapshotPins(root string) ([]EntityToolPin, error) {
	raw, err := os.ReadFile(filepath.Join(root, AgentSnapshotPath))
	if err != nil {
		return nil, err
	}
	if fmt.Sprintf("%x", sha256.Sum256(raw)) != agentSnapshotFileDigest {
		return nil, fmt.Errorf("agent snapshot complete manifest bytes changed")
	}
	var pins []EntityToolPin
	if err := json.Unmarshal(raw, &pins); err != nil {
		return nil, err
	}
	canonical, err := json.Marshal(pins)
	if err != nil || len(pins) != 42 || fmt.Sprintf("%x", sha256.Sum256(canonical)) != AgentSnapshotDigest {
		return nil, fmt.Errorf("agent snapshot complete declaration inventory changed: %v", err)
	}
	return pins, nil
}

func ValidateAgentSnapshotCurrent(root string, pins []EntityToolPin) error {
	authenticated, err := ReadAgentSnapshotPins(root)
	if err != nil || !reflect.DeepEqual(authenticated, pins) {
		return fmt.Errorf("agent snapshot supplied pins are not authenticated: %v", err)
	}
	current, key, err := entityToolCurrentFiles(root, pins)
	if err != nil {
		return err
	}
	raw, err := os.ReadFile(filepath.Join(root, AgentSnapshotFile))
	if err != nil || fmt.Sprintf("%x", sha256.Sum256(raw)) != AgentSnapshotSourceDigest {
		return fmt.Errorf("agent snapshot complete current file/types/outputs changed: %v", err)
	}
	key = "agent-snapshot/" + AgentSnapshotDigest + "/" + key
	if _, ok := sourceCache.Load(key); ok {
		return nil
	}
	raw, err = exec.Command("git", "-C", root, "show", AgentSnapshotPredecessor+":"+AgentSnapshotFile).Output()
	if err != nil {
		return err
	}
	previous, _, err := entityToolDeclarations(raw)
	if err != nil {
		return err
	}
	for _, pin := range pins {
		if pin.File != AgentSnapshotFile || previous[pin.Function] != pin.Before {
			return fmt.Errorf("agent snapshot signed predecessor declaration changed: %s", pin.Function)
		}
		body, found := current[pin.File][pin.Function]
		if pin.Removed {
			if found || pin.After != "" {
				return fmt.Errorf("retired agent snapshot authority survived: %s", pin.Function)
			}
		} else if !found || body != pin.After {
			return fmt.Errorf("agent snapshot current declaration changed: %s", pin.Function)
		}
	}
	sourceCache.Store(key, true)
	return nil
}

func EarlierAgentSnapshotSource(root, file, function, current string) (string, error) {
	if file != AgentSnapshotFile {
		return current, nil
	}
	pins, err := ReadAgentSnapshotPins(root)
	if err != nil {
		return "", err
	}
	for _, pin := range pins {
		if pin.Function != function {
			continue
		}
		if err := ValidateAgentSnapshotCurrent(root, pins); err != nil {
			return "", err
		}
		if current != pin.After {
			return "", fmt.Errorf("agent snapshot source is not its current endpoint: %s", function)
		}
		return pin.Before, nil
	}
	return current, nil
}

func PriorAgentSnapshotHistory(root string, recipes bool) ([]byte, error) {
	path := "tools/fixture-codemod/pipeline-observations/family145-transitions.json"
	if recipes {
		path = "tools/fixture-codemod/pipeline-observations/recipes.json"
	}
	return exec.Command("git", "-C", root, "show", AgentSnapshotPredecessor+":"+path).Output()
}

func ComposeAgentSnapshotHistory(root string, prior []byte, recipes bool) ([]byte, error) {
	pins, err := ReadAgentSnapshotPins(root)
	if err != nil {
		return nil, err
	}
	if err := ValidateAgentSnapshotCurrent(root, pins); err != nil {
		return nil, err
	}
	trusted, err := PriorAgentSnapshotHistory(root, recipes)
	var rows, signed []map[string]any
	if err != nil || json.Unmarshal(prior, &rows) != nil || json.Unmarshal(trusted, &signed) != nil || !reflect.DeepEqual(rows, signed) {
		return nil, fmt.Errorf("agent snapshot history is not its complete signed predecessor: %v", err)
	}
	for _, pin := range pins {
		before := len(rows)
		rows, err = advanceEntityToolHistory(rows, pin, recipes)
		if err != nil {
			return nil, err
		}
		if len(rows) != before+1 {
			return nil, fmt.Errorf("agent snapshot declaration unexpectedly intersects older history: %s", pin.Function)
		}
		if recipes {
			rows[before]["Family"] = "native-operator-agent-snapshot-family145"
		}
	}
	return json.Marshal(rows)
}

func EarlierAgentSnapshotHistory(root string, current []byte, recipes bool) ([]byte, error) {
	prior, err := PriorAgentSnapshotHistory(root, recipes)
	if err != nil {
		return nil, err
	}
	expected, err := ComposeAgentSnapshotHistory(root, prior, recipes)
	if err != nil {
		return nil, err
	}
	var have, want any
	if json.Unmarshal(current, &have) != nil || json.Unmarshal(expected, &want) != nil || !reflect.DeepEqual(have, want) {
		return nil, fmt.Errorf("agent snapshot history contains foreign changes")
	}
	return prior, nil
}

// Older receipts may be passed an already peeled predecessor. Only a history
// that actually contains this exact family's appended rows is inverted here;
// the older owner still authenticates its own complete expected inventory.
func BeforeAgentSnapshotHistory(root string, current []byte, recipes bool) ([]byte, error) {
	var rows []struct{ File, Function string }
	if err := json.Unmarshal(current, &rows); err != nil {
		return nil, err
	}
	for _, row := range rows {
		if row.File == AgentSnapshotFile && row.Function != "seedTurnWithOutputContract" {
			return EarlierAgentSnapshotHistory(root, current, recipes)
		}
	}
	return current, nil
}
