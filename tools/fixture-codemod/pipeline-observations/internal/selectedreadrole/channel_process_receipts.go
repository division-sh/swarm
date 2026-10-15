package selectedreadrole

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"

	"github.com/division-sh/swarm/internal/checkoutsource"
)

const ChannelProcessPredecessor = "7333184becb6ce4dc1f81ba963e28d5b9472bf8f"
const ChannelProcessPath = "tools/fixture-codemod/pipeline-observations/family145-channel-process-preimages.json"
const channelProcessFileDigest = "1e94cca2eaa12fedf807b37ed910371a79d0ba5df00df9f79afb4db5de879334"
const channelProcessDigest = "d6da606d63f42152bd30b9ec056d37b3b2a359ce03342540fdefd0dd5bc7db31"
const channelProcessDeclarationCount = 45

func ReadChannelProcessPins(root string) ([]EntityToolPin, error) {
	raw, err := os.ReadFile(filepath.Join(root, ChannelProcessPath))
	if err != nil || fmt.Sprintf("%x", sha256.Sum256(raw)) != channelProcessFileDigest {
		return nil, fmt.Errorf("channel process complete manifest bytes changed: %v", err)
	}
	key := "channel-process-pins/" + channelProcessFileDigest
	if cached, ok := sourceCache.Load(key); ok {
		return append([]EntityToolPin(nil), cached.([]EntityToolPin)...), nil
	}
	var pins []EntityToolPin
	if err := json.Unmarshal(raw, &pins); err != nil {
		return nil, err
	}
	canonical, err := json.Marshal(pins)
	if err != nil || len(pins) != channelProcessDeclarationCount || fmt.Sprintf("%x", sha256.Sum256(canonical)) != channelProcessDigest {
		return nil, fmt.Errorf("channel process declaration inventory changed: %v", err)
	}
	sourceCache.Store(key, append([]EntityToolPin(nil), pins...))
	return pins, nil
}

func ValidateChannelProcessCurrent(root string, pins []EntityToolPin) error {
	authenticated, err := ReadChannelProcessPins(root)
	if err != nil || !reflect.DeepEqual(authenticated, pins) {
		return fmt.Errorf("channel process supplied inventory is not authenticated: %v", err)
	}
	files, key, err := channelProcessCurrentFiles(root, pins)
	if err != nil {
		return err
	}
	key = "channel-process/" + channelProcessDigest + "/" + key
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
			raw, err := exec.Command("git", "-C", root, "show", ChannelProcessPredecessor+":"+pin.File).Output()
			if err != nil {
				return err
			}
			previous[pin.File], err = standingIngressDeclarations(raw)
			if err != nil {
				return err
			}
		}
		if previous[pin.File][pin.Function] != pin.Before || pin.Before == pin.After || pin.Removed != (pin.After == "") {
			return fmt.Errorf("channel process signed declaration changed: %s", pin.Function)
		}
		if current[pin.File][pin.Function] != pin.After {
			return fmt.Errorf("channel process actual source changed: %s", pin.Function)
		}
	}
	sourceCache.Store(key, true)
	return nil
}

func channelProcessCurrentFiles(root string, pins []EntityToolPin) (map[string][]byte, string, error) {
	files := map[string][]byte{}
	hash := sha256.New()
	for _, pin := range pins {
		if files[pin.File] != nil {
			continue
		}
		path := filepath.Join(root, pin.File)
		owned, err := checkoutsource.Member(root, path)
		if err != nil || !owned {
			return nil, "", fmt.Errorf("channel process source is not owned: %s: %v", pin.File, err)
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, "", err
		}
		files[pin.File] = raw
		fmt.Fprintf(hash, "%s:%x\n", pin.File, sha256.Sum256(raw))
	}
	return files, fmt.Sprintf("%s/%x", root, hash.Sum(nil)), nil
}

func EarlierChannelProcessSource(root, file, function, current string) (string, error) {
	var err error
	current, err = EarlierDurableResourceSource(root, file, function, current)
	if err != nil {
		return "", err
	}
	pins, err := ReadChannelProcessPins(root)
	if err != nil {
		return "", err
	}
	for _, pin := range pins {
		if pin.File != file || pin.Function != function {
			continue
		}
		if err := ValidateChannelProcessCurrent(root, pins); err != nil {
			return "", err
		}
		return ChannelProcessPrevious(pins, file, function, current)
	}
	return current, nil
}

// The caller authenticates the complete pins and actual source once before a
// finite batch comparison. This is the same exact per-declaration inverse.
func ChannelProcessPrevious(pins []EntityToolPin, file, function, current string) (string, error) {
	for _, pin := range pins {
		if pin.File != file || pin.Function != function {
			continue
		}
		if current != pin.After {
			return "", fmt.Errorf("channel process supplied source is not its live endpoint: %s", function)
		}
		return pin.Before, nil
	}
	return current, nil
}

func PriorChannelProcessHistory(root string, recipes bool) ([]byte, error) {
	path := "tools/fixture-codemod/pipeline-observations/family145-transitions.json"
	if recipes {
		path = "tools/fixture-codemod/pipeline-observations/recipes.json"
	}
	return exec.Command("git", "-C", root, "show", ChannelProcessPredecessor+":"+path).Output()
}

func ComposeChannelProcessHistory(root string, prior []byte, recipes bool) ([]byte, error) {
	pins, err := ReadChannelProcessPins(root)
	if err != nil {
		return nil, err
	}
	if err := ValidateChannelProcessCurrent(root, pins); err != nil {
		return nil, err
	}
	trusted, err := PriorChannelProcessHistory(root, recipes)
	var rows, signed []map[string]any
	if err != nil || json.Unmarshal(prior, &rows) != nil || json.Unmarshal(trusted, &signed) != nil || !reflect.DeepEqual(rows, signed) {
		return nil, fmt.Errorf("channel process history is not its complete signed predecessor: %v", err)
	}
	for _, pin := range pins {
		before := len(rows)
		rows, err = advanceEntityToolHistory(rows, pin, recipes)
		if err != nil {
			return nil, err
		}
		if recipes && len(rows) > before {
			rows[before]["Family"] = "native-channel-process-carrier-family145"
		}
	}
	return json.Marshal(rows)
}

func EarlierChannelProcessHistory(root string, current []byte, recipes bool) ([]byte, error) {
	var err error
	current, err = BeforeDurableResourceHistory(root, current, recipes)
	if err != nil {
		return nil, err
	}
	prior, err := PriorChannelProcessHistory(root, recipes)
	if err != nil {
		return nil, err
	}
	expected, err := ComposeChannelProcessHistory(root, prior, recipes)
	if err != nil {
		return nil, err
	}
	var have, want any
	if json.Unmarshal(current, &have) != nil || json.Unmarshal(expected, &want) != nil || !reflect.DeepEqual(have, want) {
		return nil, fmt.Errorf("channel process current history contains foreign changes")
	}
	return prior, nil
}

// Older owners still validate their complete predecessor. Peel this stage only
// when one of its exact new endpoints is actually present in the supplied ledger.
func BeforeChannelProcessHistory(root string, current []byte, recipes bool) ([]byte, error) {
	var err error
	current, err = BeforeDurableResourceHistory(root, current, recipes)
	if err != nil {
		return nil, err
	}
	var rows []map[string]any
	if err := json.Unmarshal(current, &rows); err != nil {
		return nil, err
	}
	pins, err := ReadChannelProcessPins(root)
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
				return EarlierChannelProcessHistory(root, current, recipes)
			}
		}
	}
	return current, nil
}
