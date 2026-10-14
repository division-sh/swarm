package budgetdiagnostic

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

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runphysical"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/selectedreadrole"
)

const Predecessor = "a552cdb3a2953add81cea117bafcadddd9803c11"
const Path = "tools/fixture-codemod/pipeline-observations/family145-budget-diagnostics-preimages.json"
const Digest = "9c9b0ceedd9880ab028181bdbe957ed58ea3042c414f12ea0ea9dc0fac617211"
const Source = "internal/serveapp/channel_delivery_budget_supported_surface_test.go"

func ReadPins(root string) ([]runphysical.Pin, error) {
	raw, err := os.ReadFile(filepath.Join(root, Path))
	if err != nil || fmt.Sprintf("%x", sha256.Sum256(raw)) != Digest {
		return nil, fmt.Errorf("budget diagnostic finite source inventory changed: %v", err)
	}
	var pins []runphysical.Pin
	if err := json.Unmarshal(raw, &pins); err != nil || len(pins) != 2 {
		return nil, fmt.Errorf("budget diagnostic declaration inventory changed")
	}
	return pins, nil
}

func Current(root, name string) (string, error) {
	raw, err := os.ReadFile(filepath.Join(root, Source))
	if err != nil {
		return "", err
	}
	file, err := parser.ParseFile(token.NewFileSet(), Source, raw, parser.AllErrors)
	if err != nil {
		return "", err
	}
	body, count := "", 0
	for _, declaration := range file.Decls {
		if fn, ok := declaration.(*ast.FuncDecl); ok && fn.Recv == nil && fn.Name.Name == name {
			body, count = requestkey.Node(fn), count+1
		}
	}
	if count != 1 {
		return "", fmt.Errorf("budget current declaration missing or duplicated: %s", name)
	}
	return body, nil
}

func ValidateCurrent(root string, pins []runphysical.Pin) error {
	for _, pin := range pins {
		body, err := Current(root, pin.Function)
		if err != nil || pin.File != Source || body != pin.After {
			return fmt.Errorf("budget diagnostic actual source changed: %s: %v", pin.Function, err)
		}
	}
	return nil
}

func EarlierSource(root, file, function, current string) (string, error) {
	if file != Source || (function != "TestChannelDeliveryBudgetNoticePublicJourney" && function != "logBudgetNoticeFailure") {
		return current, nil
	}
	pins, err := ReadPins(root)
	if err != nil {
		return "", err
	}
	if err := ValidateCurrent(root, pins); err != nil {
		return "", err
	}
	for _, pin := range pins {
		if pin.Function == function && pin.After == current {
			return pin.Before, nil
		}
	}
	return "", fmt.Errorf("budget diagnostic supplied source is not actual current source")
}

func PriorHistory(root string, recipes bool) ([]byte, error) {
	name := "family145-transitions.json"
	if recipes {
		name = "recipes.json"
	}
	return exec.Command("git", "-C", root, "show", Predecessor+":tools/fixture-codemod/pipeline-observations/"+name).Output()
}

func ComposeHistory(root string, prior []byte, recipes bool) ([]byte, error) {
	signed, err := PriorHistory(root, recipes)
	if err != nil || !bytes.Equal(signed, prior) {
		return nil, fmt.Errorf("budget history is not its exact signed predecessor: %v", err)
	}
	pins, err := ReadPins(root)
	if err != nil {
		return nil, err
	}
	var rows []map[string]any
	if err := json.Unmarshal(prior, &rows); err != nil {
		return nil, err
	}
	for _, pin := range pins {
		rows, err = advance(rows, pin, recipes)
		if err != nil {
			return nil, err
		}
	}
	raw, err := json.MarshalIndent(rows, "", "  ")
	return append(raw, '\n'), err
}

func advance(rows []map[string]any, pin runphysical.Pin, recipes bool) ([]map[string]any, error) {
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
			return nil, fmt.Errorf("budget history current endpoint changed: %s", pin.Function)
		}
		field := "After"
		if recipes {
			field = "Successor"
		}
		row[field] = pin.After
	}
	if matches > 1 {
		return nil, fmt.Errorf("budget history duplicate endpoint: %s", pin.Function)
	}
	if matches == 0 {
		row := map[string]any{"File": pin.File, "Function": pin.Function, "Before": pin.Before, "After": pin.After}
		if recipes {
			row["Family"] = "channel-budget-diagnostic-storage"
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func EarlierHistory(root string, current []byte, recipes bool) ([]byte, error) {
	current, err := selectedreadrole.EarlierProcessHistory(root, current, recipes)
	if err != nil {
		return nil, err
	}
	current, err = selectedreadrole.EarlierHistory(root, current, recipes)
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
	expected, err := ComposeHistory(root, prior, recipes)
	if err != nil {
		return nil, err
	}
	var want, actual []map[string]any
	if json.Unmarshal(expected, &want) != nil || json.Unmarshal(current, &actual) != nil || !reflect.DeepEqual(want, actual) {
		return nil, fmt.Errorf("budget history changed original fields, metadata or order")
	}
	return prior, nil
}
