package selectedreadrole

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"

	"github.com/division-sh/swarm/internal/checkoutsource"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
)

const StandingIngressPredecessor = "366ef25c98c0ec629354038246e7a30b214d7971"
const StandingIngressPath = "tools/fixture-codemod/pipeline-observations/family145-standing-ingress-control-preimages.json"
const StandingIngressDigest = "0f9130759c896e3dae5d0eef393454ee3b5621741a0681f0d0d9c4b685ff8219"
const standingIngressFileDigest = "ea16d7efc8ff4471bebc5b5e2af94a833c10a4650586e60264b9d057e5edb625"

func ReadStandingIngressPins(root string) ([]EntityToolPin, error) {
	raw, err := os.ReadFile(filepath.Join(root, StandingIngressPath))
	if err != nil {
		return nil, err
	}
	if fmt.Sprintf("%x", sha256.Sum256(raw)) != standingIngressFileDigest {
		return nil, fmt.Errorf("standing-ingress complete manifest bytes changed")
	}
	key := "immutable-pins/" + standingIngressFileDigest
	if cached, ok := sourceCache.Load(key); ok {
		return append([]EntityToolPin(nil), cached.([]EntityToolPin)...), nil
	}
	var pins []EntityToolPin
	if err := json.Unmarshal(raw, &pins); err != nil {
		return nil, err
	}
	canonical, err := json.Marshal(pins)
	if err != nil || len(pins) != 29 || fmt.Sprintf("%x", sha256.Sum256(canonical)) != StandingIngressDigest {
		return nil, fmt.Errorf("standing-ingress complete source inventory changed: %v", err)
	}
	sourceCache.Store(key, append([]EntityToolPin(nil), pins...))
	return pins, nil
}

func ValidateStandingIngressCurrent(root string, pins []EntityToolPin) error {
	authenticated, err := ReadStandingIngressPins(root)
	if err != nil || !reflect.DeepEqual(authenticated, pins) {
		return fmt.Errorf("standing-ingress supplied inventory is not authenticated: %v", err)
	}
	current, key, err := standingIngressCurrentFiles(root, pins)
	if err != nil {
		return err
	}
	key = "standing-ingress/" + StandingIngressDigest + "/" + key
	if _, ok := sourceCache.Load(key); ok {
		return nil
	}
	previous := map[string]map[string]string{}
	for _, pin := range pins {
		if previous[pin.File] != nil {
			continue
		}
		raw, err := exec.Command("git", "-C", root, "show", StandingIngressPredecessor+":"+pin.File).Output()
		if err != nil {
			return err
		}
		previous[pin.File], err = standingIngressDeclarations(raw)
		if err != nil {
			return err
		}
	}
	for _, pin := range pins {
		if pin.Removed || previous[pin.File][pin.Function] != pin.Before {
			return fmt.Errorf("standing-ingress signed retained source changed: %s", pin.Function)
		}
		body, found := current[pin.File][pin.Function]
		if !found || body != pin.After {
			return fmt.Errorf("standing-ingress live sibling changed: %s", pin.Function)
		}
	}
	sourceCache.Store(key, true)
	return nil
}

func standingIngressDeclarations(raw []byte) (map[string]string, error) {
	key := fmt.Sprintf("standing-ingress/declarations/%x", sha256.Sum256(raw))
	if cached, ok := sourceCache.Load(key); ok {
		return maps.Clone(cached.(map[string]string)), nil
	}
	file, err := parser.ParseFile(token.NewFileSet(), "source.go", raw, parser.AllErrors)
	if err != nil {
		return nil, err
	}
	bodies := map[string]string{}
	for _, declaration := range file.Decls {
		fn, ok := declaration.(*ast.FuncDecl)
		if !ok || fn.Recv != nil {
			continue
		}
		if _, exists := bodies[fn.Name.Name]; exists {
			return nil, fmt.Errorf("duplicate standing-ingress function: %s", fn.Name.Name)
		}
		bodies[fn.Name.Name] = requestkey.Node(fn)
	}
	// Cache only the deterministic parse, never source ownership or acceptance.
	sourceCache.Store(key, maps.Clone(bodies))
	return bodies, nil
}

func standingIngressCurrentFiles(root string, pins []EntityToolPin) (map[string]map[string]string, string, error) {
	current := map[string]map[string]string{}
	hash := sha256.New()
	for _, pin := range pins {
		if current[pin.File] != nil {
			continue
		}
		path := filepath.Join(root, pin.File)
		owned, err := checkoutsource.Member(root, path)
		if err != nil || !owned {
			return nil, "", fmt.Errorf("standing-ingress source is not owned: %s: %v", pin.File, err)
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, "", err
		}
		fmt.Fprintf(hash, "%s:%x\n", pin.File, sha256.Sum256(raw))
		raw, err = CatchUp2542HistoricalSource(root, pin.File, raw)
		if err != nil {
			return nil, "", err
		}
		current[pin.File], err = standingIngressDeclarations(raw)
		if err != nil {
			return nil, "", err
		}
	}
	return current, fmt.Sprintf("%x", hash.Sum(nil)), nil
}

func EarlierStandingIngressSource(root, file, function, current string) (string, error) {
	var integrationErr error
	current, integrationErr = CatchUp2542HistoricalFunction(root, file, function, current)
	if integrationErr != nil {
		return "", integrationErr
	}
	pins, err := ReadStandingIngressPins(root)
	if err != nil {
		return "", err
	}
	for _, pin := range pins {
		if pin.File != file || pin.Function != function {
			continue
		}
		if err := ValidateStandingIngressCurrent(root, pins); err != nil {
			return "", err
		}
		if current != pin.After {
			return "", fmt.Errorf("standing-ingress source is not its actual live endpoint: %s", function)
		}
		return pin.Before, nil
	}
	return current, nil
}

func PriorStandingIngressHistory(root string, recipes bool) ([]byte, error) {
	path := "tools/fixture-codemod/pipeline-observations/family145-transitions.json"
	if recipes {
		path = "tools/fixture-codemod/pipeline-observations/recipes.json"
	}
	return exec.Command("git", "-C", root, "show", StandingIngressPredecessor+":"+path).Output()
}

func ComposeStandingIngressHistory(root string, prior []byte, recipes bool) ([]byte, error) {
	pins, err := ReadStandingIngressPins(root)
	if err != nil {
		return nil, err
	}
	if err := ValidateStandingIngressCurrent(root, pins); err != nil {
		return nil, err
	}
	trusted, err := PriorStandingIngressHistory(root, recipes)
	var rows, signed []map[string]any
	if err != nil || json.Unmarshal(prior, &rows) != nil || json.Unmarshal(trusted, &signed) != nil || !reflect.DeepEqual(rows, signed) {
		return nil, fmt.Errorf("standing-ingress history is not its complete signed predecessor: %v", err)
	}
	for _, pin := range pins {
		before := len(rows)
		rows, err = advanceEntityToolHistory(rows, pin, recipes)
		if err != nil {
			return nil, err
		}
		if recipes && len(rows) > before {
			rows[before]["Family"] = "native-standing-ingress-control-family145"
		}
	}
	return json.Marshal(rows)
}

func EarlierStandingIngressHistory(root string, current []byte, recipes bool) ([]byte, error) {
	current, err := BeforeAgentSnapshotHistory(root, current, recipes)
	if err != nil {
		return nil, err
	}
	prior, err := PriorStandingIngressHistory(root, recipes)
	if err != nil {
		return nil, err
	}
	expected, err := ComposeStandingIngressHistory(root, prior, recipes)
	if err != nil {
		return nil, err
	}
	var have, want any
	if json.Unmarshal(current, &have) != nil || json.Unmarshal(expected, &want) != nil || !reflect.DeepEqual(have, want) {
		return nil, fmt.Errorf("standing-ingress current history contains foreign changes")
	}
	return prior, nil
}
