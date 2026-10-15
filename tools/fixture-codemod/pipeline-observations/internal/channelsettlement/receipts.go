package channelsettlement

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sync"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/budgetdiagnostic"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runphysical"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/selectedreadrole"
)

// This artifact comparison reverses only the approved embedded read role.
func EarlierObservationSource(root string, current []byte) ([]byte, error) {
	const path = "internal/store/storetest/channel_observation.go"
	actual, err := os.ReadFile(filepath.Join(root, path))
	if err != nil || !bytes.Equal(current, actual) || fmt.Sprintf("%x", sha256.Sum256(current)) != "5f080b0d3987f9c1481267c4fb5b95839e1f6715b8dd541ad0e5409d09578ed0" {
		return nil, fmt.Errorf("channel observation supplied source is not the exact live role addition")
	}
	prior, err := exec.Command("git", "-C", root, "show", Predecessor+":"+path).Output()
	if err != nil || fmt.Sprintf("%x", sha256.Sum256(prior)) != "df61c4bd6f86772194be4bc7f6a615ce0652b5c5c9c2dbdc69466846192d201f" {
		return nil, fmt.Errorf("channel observation signed role predecessor changed: %v", err)
	}
	return prior, nil
}

const Predecessor = "5ffa94a6fd9646b3ee7f38d138ebf4db965415f2"
const Path = "tools/fixture-codemod/pipeline-observations/family145-channel-settlement-preimages.json"
const Digest = "b4e5394d408d8fdfffc4f997a366d45896abfdd71e19c893d8a06baac3c8168f"

var bodyCache sync.Map
var priorCache sync.Map
var historyCache sync.Map

func ReadPins(root string) ([]runphysical.Pin, error) {
	raw, err := os.ReadFile(filepath.Join(root, Path))
	if err != nil || fmt.Sprintf("%x", sha256.Sum256(raw)) != Digest {
		return nil, fmt.Errorf("channel settlement finite source inventory changed: %v", err)
	}
	var pins []runphysical.Pin
	if err := json.Unmarshal(raw, &pins); err != nil || len(pins) != 38 {
		return nil, fmt.Errorf("channel settlement declaration inventory changed")
	}
	seen := map[string]bool{}
	for _, pin := range pins {
		key := pin.File + "/" + pin.Function
		if seen[key] || pin.Before == pin.After || pin.After == "" || fmt.Sprintf("%x", sha256.Sum256([]byte(pin.Before))) != pin.SHA256 {
			return nil, fmt.Errorf("channel settlement complete predecessor changed: %s", key)
		}
		seen[key] = true
	}
	return pins, nil
}

func Body(raw []byte, name string) (string, error) {
	key := fmt.Sprintf("%x/%s", sha256.Sum256(raw), name)
	if value, ok := bodyCache.Load(key); ok {
		return value.(string), nil
	}
	file, err := parser.ParseFile(token.NewFileSet(), "source.go", raw, parser.AllErrors)
	if err != nil {
		return "", err
	}
	body, count := "", 0
	for _, decl := range file.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Recv == nil && fn.Name.Name == name {
			body = requestkey.Node(fn)
			count++
		}
	}
	if count != 1 {
		return "", fmt.Errorf("channel settlement declaration is missing/duplicate: %s", name)
	}
	bodyCache.Store(key, body)
	return body, nil
}

func ValidateCurrent(root string, pins []runphysical.Pin) error {
	files := map[string][]byte{}
	var process []selectedreadrole.EntityToolPin
	for _, pin := range pins {
		if files[pin.File] == nil {
			raw, err := os.ReadFile(filepath.Join(root, pin.File))
			if err != nil {
				return err
			}
			files[pin.File] = raw
		}
		current, err := Body(files[pin.File], pin.Function)
		if err == nil && current != pin.After {
			if process == nil {
				process, err = selectedreadrole.ReadChannelProcessPins(root)
				if err == nil {
					err = selectedreadrole.ValidateChannelProcessCurrent(root, process)
				}
				if err != nil {
					return err
				}
			}
			current, err = selectedreadrole.ChannelProcessPrevious(process, pin.File, pin.Function, current)
		}
		if err != nil || current != pin.After {
			return fmt.Errorf("channel settlement actual current source changed: %s: %v", pin.Function, err)
		}
	}
	return nil
}

func EarlierSource(root, file, function, current string) (string, error) {
	pins, err := ReadPins(root)
	if err != nil {
		return "", err
	}
	for _, pin := range pins {
		if pin.File != file || pin.Function != function {
			continue
		}
		if err := ValidateCurrent(root, pins); err != nil {
			return "", err
		}
		if current != pin.After {
			current, err = selectedreadrole.EarlierChannelProcessSource(root, file, function, current)
			if err != nil {
				return "", err
			}
		}
		if current != pin.After {
			return "", fmt.Errorf("channel settlement supplied source is not its live endpoint: %s", function)
		}
		return pin.Before, nil
	}
	return current, nil
}

func PriorHistory(root string, recipes bool) ([]byte, error) {
	path := "tools/fixture-codemod/pipeline-observations/family145-transitions.json"
	if recipes {
		path = "tools/fixture-codemod/pipeline-observations/recipes.json"
	}
	key := root + "/" + path
	if raw, ok := priorCache.Load(key); ok {
		return append([]byte{}, raw.([]byte)...), nil
	}
	raw, err := exec.Command("git", "-C", root, "show", Predecessor+":"+path).Output()
	if err != nil {
		return nil, err
	}
	priorCache.Store(key, append([]byte{}, raw...))
	return raw, nil
}

func ComposeHistory(prior []byte, pins []runphysical.Pin, recipes bool) ([]byte, error) {
	want := "18f6bd7d7d1a889729e035efa1949972d5a75552af0259f50bb9a3803707edfe"
	count := 871
	if recipes {
		want = "3836f856f10532f96068718a634746a4e4ed3aed5cfef15110c03958e3ac8e8c"
		count = 1564
	}
	if fmt.Sprintf("%x", sha256.Sum256(prior)) != want {
		return nil, fmt.Errorf("channel history prior is not its signed predecessor")
	}
	canonical, err := json.MarshalIndent(pins, "", "  ")
	if err != nil || len(pins) != 38 || fmt.Sprintf("%x", sha256.Sum256(append(canonical, '\n'))) != Digest {
		return nil, fmt.Errorf("channel history lacks its complete finite source proof")
	}
	key := fmt.Sprintf("%t/%x", recipes, sha256.Sum256(prior))
	if cached, ok := historyCache.Load(key); ok {
		return append([]byte{}, cached.([]byte)...), nil
	}
	var rows []map[string]any
	if err := json.Unmarshal(prior, &rows); err != nil {
		return nil, err
	}
	if len(rows) != count {
		return nil, fmt.Errorf("channel history prior row inventory changed")
	}
	for _, pin := range pins {
		var err error
		rows, err = composePin(rows, pin, recipes)
		if err != nil {
			return nil, err
		}
	}
	raw, err := json.MarshalIndent(rows, "", "  ")
	if err != nil {
		return nil, err
	}
	raw = append(raw, '\n')
	historyCache.Store(key, append([]byte{}, raw...))
	return raw, nil
}

func composePin(rows []map[string]any, pin runphysical.Pin, recipes bool) ([]map[string]any, error) {
	matches := 0
	for _, row := range rows {
		if row["File"] != pin.File || row["Function"] != pin.Function {
			continue
		}
		matches++
		endpoint := row["After"]
		if successor, ok := row["Successor"]; ok {
			endpoint = successor
		}
		if endpoint != pin.Before || row["Removed"] == true {
			return nil, fmt.Errorf("channel history predecessor does not match: %s", pin.Function)
		}
		field := "After"
		if recipes {
			field = "Successor"
		}
		row[field] = pin.After
	}
	if matches > 1 {
		return nil, fmt.Errorf("duplicate channel history endpoint: %s", pin.Function)
	}
	if matches == 0 {
		row := map[string]any{"File": pin.File, "Function": pin.Function, "Before": pin.Before, "After": pin.After}
		if recipes {
			row["Family"] = "native-postgres-channel-settlement"
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func RestoreHistory(current, prior []byte, pins []runphysical.Pin, recipes bool) ([]byte, error) {
	expected, err := ComposeHistory(prior, pins, recipes)
	if err != nil {
		return nil, err
	}
	var actual, want []map[string]any
	if json.Unmarshal(current, &actual) != nil || json.Unmarshal(expected, &want) != nil || !reflect.DeepEqual(actual, want) {
		return nil, fmt.Errorf("channel settlement history changed metadata, source, multiplicity or order")
	}
	return append([]byte{}, prior...), nil
}

func EarlierHistory(root string, current []byte, recipes bool) ([]byte, error) {
	current, err := budgetdiagnostic.EarlierHistory(root, current, recipes)
	if err != nil {
		return nil, err
	}
	pins, err := ReadPins(root)
	if err != nil {
		return nil, err
	}
	if err := ValidateCurrent(root, pins); err != nil {
		return nil, err
	}
	prior, err := PriorHistory(root, recipes)
	if err != nil {
		return nil, err
	}
	return RestoreHistory(current, prior, pins, recipes)
}
