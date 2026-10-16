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

const DeploymentResourcePredecessor = "1c4cb4917703cda30faa17de2f093332e7a38c36"
const DeploymentResourcePath = "tools/fixture-codemod/pipeline-observations/family145-deployment-resource-preimages.json"
const DeploymentResourceDigest = "d5893977e460eb6bbbb6600465f3fd6451ba32d9d0dc6ab559ce8b4e2ec0029a"
const deploymentResourceFileDigest = "04bfe5a3bd361ba195b7be1f9791d51fd723cf587cc7261c0edfd9ef328a078e"

func ReadDeploymentResourcePins(root string) ([]EntityToolPin, error) {
	raw, err := os.ReadFile(filepath.Join(root, DeploymentResourcePath))
	if err != nil {
		return nil, err
	}
	if fmt.Sprintf("%x", sha256.Sum256(raw)) != deploymentResourceFileDigest {
		return nil, fmt.Errorf("deployment-resource complete manifest bytes changed")
	}
	key := "immutable-pins/" + deploymentResourceFileDigest
	if cached, ok := sourceCache.Load(key); ok {
		return append([]EntityToolPin(nil), cached.([]EntityToolPin)...), nil
	}
	var pins []EntityToolPin
	if err := json.Unmarshal(raw, &pins); err != nil {
		return nil, err
	}
	canonical, err := json.Marshal(pins)
	if err != nil || len(pins) != 32 || fmt.Sprintf("%x", sha256.Sum256(canonical)) != DeploymentResourceDigest {
		return nil, fmt.Errorf("deployment-resource complete source inventory changed: %v", err)
	}
	sourceCache.Store(key, append([]EntityToolPin(nil), pins...))
	return pins, nil
}

func ValidateDeploymentResourceCurrent(root string, pins []EntityToolPin) error {
	authenticated, err := ReadDeploymentResourcePins(root)
	if err != nil || !reflect.DeepEqual(authenticated, pins) {
		return fmt.Errorf("deployment-resource supplied inventory is not authenticated: %v", err)
	}
	current, key, err := entityToolCurrentFiles(root, pins)
	if err != nil {
		return err
	}
	key = "deployment-resource/" + DeploymentResourceDigest + "/" + key
	if _, ok := sourceCache.Load(key); ok {
		return nil
	}
	previous := map[string]map[string]string{}
	for _, pin := range pins {
		if previous[pin.File] != nil {
			continue
		}
		raw, err := exec.Command("git", "-C", root, "show", DeploymentResourcePredecessor+":"+pin.File).Output()
		if err != nil {
			return err
		}
		previous[pin.File], _, err = entityToolDeclarations(raw)
		if err != nil {
			return err
		}
	}
	for _, pin := range pins {
		if pin.Removed || previous[pin.File][pin.Function] != pin.Before {
			return fmt.Errorf("deployment-resource signed source or retained-declaration status changed: %s", pin.Function)
		}
		body, found := current[pin.File][pin.Function]
		if !found || body != pin.After {
			return fmt.Errorf("deployment-resource live sibling changed: %s", pin.Function)
		}
	}
	sourceCache.Store(key, true)
	return nil
}

// Apply only at the actual live-source entry, before older family inverses.
func EarlierDeploymentResourceSource(root, file, function, current string) (string, error) {
	pins, err := ReadDeploymentResourcePins(root)
	if err != nil {
		return "", err
	}
	for _, pin := range pins {
		if pin.File != file || pin.Function != function {
			continue
		}
		if err := ValidateDeploymentResourceCurrent(root, pins); err != nil {
			return "", err
		}
		if current != pin.After {
			return "", fmt.Errorf("deployment-resource supplied source is not its live endpoint: %s", function)
		}
		return pin.Before, nil
	}
	return current, nil
}

func PriorDeploymentResourceHistory(root string, recipes bool) ([]byte, error) {
	path := "tools/fixture-codemod/pipeline-observations/family145-transitions.json"
	if recipes {
		path = "tools/fixture-codemod/pipeline-observations/recipes.json"
	}
	return exec.Command("git", "-C", root, "show", DeploymentResourcePredecessor+":"+path).Output()
}

func ComposeDeploymentResourceHistory(root string, prior []byte, recipes bool) ([]byte, error) {
	pins, err := ReadDeploymentResourcePins(root)
	if err != nil {
		return nil, err
	}
	if err := ValidateDeploymentResourceCurrent(root, pins); err != nil {
		return nil, err
	}
	trusted, err := PriorDeploymentResourceHistory(root, recipes)
	var rows, signed []map[string]any
	if err != nil || json.Unmarshal(prior, &rows) != nil || json.Unmarshal(trusted, &signed) != nil || !reflect.DeepEqual(rows, signed) {
		return nil, fmt.Errorf("deployment-resource history is not its complete signed predecessor: %v", err)
	}
	for _, pin := range pins {
		before := len(rows)
		rows, err = advanceEntityToolHistory(rows, pin, recipes)
		if err != nil {
			return nil, err
		}
		if recipes && len(rows) > before {
			rows[before]["Family"] = "native-deployment-resource-family145"
		}
	}
	return json.Marshal(rows)
}

func EarlierDeploymentResourceHistory(root string, current []byte, recipes bool) ([]byte, error) {
	prior, err := PriorDeploymentResourceHistory(root, recipes)
	if err != nil {
		return nil, err
	}
	expected, err := ComposeDeploymentResourceHistory(root, prior, recipes)
	if err != nil {
		return nil, err
	}
	var have, want any
	if json.Unmarshal(current, &have) != nil || json.Unmarshal(expected, &want) != nil || !reflect.DeepEqual(have, want) {
		return nil, fmt.Errorf("deployment-resource current history contains foreign changes")
	}
	return prior, nil
}
