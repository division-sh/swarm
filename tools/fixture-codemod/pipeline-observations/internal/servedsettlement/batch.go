package servedsettlement

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/channelsettlement"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/globalname"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runphysical"
)

const BatchPredecessor = "2ad21b4f19fb1c5bc04d34fe73f94d15ad0244f8"

// These immutable stages compose only this reviewed batch, in execution order.
func BatchPins(global, settlement, lookup []byte) ([]runphysical.Pin, error) {
	stages := make([][]runphysical.Pin, 3)
	var err error
	for index, input := range []struct {
		raw    []byte
		decode func([]byte) ([]runphysical.Pin, error)
	}{{global, globalname.Decode}, {settlement, Decode}, {lookup, DecodeLookup}} {
		stages[index], err = input.decode(input.raw)
		if err != nil {
			return nil, err
		}
		if err := validateBatchStage(stages[index], index); err != nil {
			return nil, err
		}
	}
	var pins []runphysical.Pin
	positions := map[string]int{}
	for _, stage := range stages {
		for _, pin := range stage {
			key := pin.File + "/" + pin.Function
			if index, exists := positions[key]; exists {
				if pins[index].After != pin.Before {
					return nil, fmt.Errorf("served batch intermediate source link changed: %s", key)
				}
				pins[index].After = pin.After
				continue
			}
			positions[key] = len(pins)
			pins = append(pins, pin)
		}
	}
	if len(pins) != 48 {
		return nil, fmt.Errorf("served batch complete unique declaration inventory changed")
	}
	return pins, nil
}

func validateBatchStage(pins []runphysical.Pin, stage int) error {
	seen := map[string]bool{}
	for index, pin := range pins {
		key := pin.File + "/" + pin.Function
		if seen[key] || fmt.Sprintf("%x", sha256.Sum256([]byte(pin.Before))) != pin.SHA256 {
			return fmt.Errorf("served batch duplicate/changed complete predecessor: %s", key)
		}
		seen[key] = true
		after, calls, err := batchTransform(pin.Before, stage, index)
		if err != nil || after != pin.After || calls != pin.Calls {
			return fmt.Errorf("served batch finite transformation or multiplicity changed: %s: %v", key, err)
		}
	}
	return nil
}

func batchTransform(before string, stage, index int) (string, int, error) {
	switch stage {
	case 0:
		after, err := runphysical.Transform(globalname.Rules[index], before)
		return after, globalname.Rules[index].Calls, err
	case 1:
		return Transform(before)
	case 2:
		return TransformLookup(before)
	default:
		return "", 0, fmt.Errorf("foreign served batch stage")
	}
}

func ReadBatchPins(root string) ([]runphysical.Pin, error) {
	raw := make([][]byte, 3)
	for index, path := range []string{globalname.Path, Path, LookupPath} {
		var err error
		raw[index], err = os.ReadFile(filepath.Join(root, path))
		if err != nil {
			return nil, err
		}
	}
	return BatchPins(raw[0], raw[1], raw[2])
}

func ValidateBatchCurrentSource(root string, pins []runphysical.Pin) error {
	setup, err := ReadSetupPins(root)
	if err != nil {
		return err
	}
	if err := ValidateSetupCurrentSource(root, setup); err != nil {
		return err
	}
	for _, pin := range pins {
		raw, err := os.ReadFile(filepath.Join(root, pin.File))
		if err != nil {
			return err
		}
		current, err := UniqueBody(raw, pin.Function)
		if err == nil {
			current, err = channelsettlement.EarlierSource(root, pin.File, pin.Function, current)
		}
		if err == nil {
			current, err = EarlierSource(setup, pin.File, pin.Function, current)
		}
		if err != nil || current != pin.After {
			return fmt.Errorf("served batch actual unique current source changed: %s: %v", pin.Function, err)
		}
	}
	return nil
}

func UniqueBody(raw []byte, name string) (string, error) {
	file, err := parser.ParseFile(token.NewFileSet(), "source.go", raw, parser.AllErrors)
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
		return "", fmt.Errorf("served batch requires exactly one current declaration: %s", name)
	}
	return body, nil
}

// EarlierSource is for source-evidence copies only, never occurrence accounting.
func EarlierSource(pins []runphysical.Pin, file, function, current string) (string, error) {
	for _, pin := range pins {
		if pin.File == file && pin.Function == function {
			if current != pin.After {
				return "", fmt.Errorf("served batch supplied endpoint differs from actual final source: %s", function)
			}
			return pin.Before, nil
		}
	}
	return current, nil
}

type batchHistoryRow map[string]any

func ComposeBatchHistory(prior []byte, pins []runphysical.Pin, recipes bool) ([]byte, error) {
	raw, err := json.Marshal(pins)
	if err != nil || len(pins) != 48 || fmt.Sprintf("%x", sha256.Sum256(raw)) != "8ba8642aa90f7c612978a6e876f7ad4dba82ef61daa8da2739e478377c411705" {
		return nil, fmt.Errorf("served batch history requires the complete finite inventory")
	}
	var rows []batchHistoryRow
	if err := json.Unmarshal(prior, &rows); err != nil {
		return nil, err
	}
	for _, pin := range pins {
		var err error
		rows, err = composeBatchPin(rows, pin, recipes)
		if err != nil {
			return nil, err
		}
	}
	return json.MarshalIndent(rows, "", "  ")
}

// A restoration cannot erase changed metadata, unrelated records or ordering.
func RestoreBatchHistory(current, prior []byte, pins []runphysical.Pin, recipes bool) ([]byte, error) {
	expected, err := ComposeBatchHistory(prior, pins, recipes)
	if err != nil {
		return nil, err
	}
	var rows, want []batchHistoryRow
	if err := json.Unmarshal(current, &rows); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(expected, &want); err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(rows, want) {
		return nil, fmt.Errorf("served batch history changed an endpoint, metadata, inventory or order")
	}
	return append([]byte{}, prior...), nil
}

func composeBatchPin(rows []batchHistoryRow, pin runphysical.Pin, recipes bool) ([]batchHistoryRow, error) {
	return composeBatchPinWithAppends(rows, pin, recipes, batchAppends)
}

func composeBatchPinWithAppends(rows []batchHistoryRow, pin runphysical.Pin, recipes bool, appends map[string]bool) ([]batchHistoryRow, error) {
	match := -1
	for index, row := range rows {
		if row["File"] == pin.File && row["Function"] == pin.Function {
			if match != -1 {
				return nil, fmt.Errorf("served batch historical row is ambiguous: %s", pin.Function)
			}
			match = index
		}
	}
	if (match == -1) != appends[pin.Function] {
		return nil, fmt.Errorf("served batch historical row is missing or foreign: %s", pin.Function)
	}
	if match == -1 {
		row := batchHistoryRow{"File": pin.File, "Function": pin.Function, "Before": pin.Before, "After": pin.After}
		if recipes {
			row["Family"] = "served-observation-family145"
		}
		return append(rows, row), nil
	}
	row := rows[match]
	current := row["After"]
	if recipes && row["Successor"] != nil {
		current = row["Successor"]
	}
	if current != pin.Before || row["Removed"] != nil {
		return nil, fmt.Errorf("served batch original current endpoint changed: %s", pin.Function)
	}
	key := "After"
	if recipes {
		key = "Successor"
	}
	row[key] = pin.After
	return rows, nil
}

var batchAppends = map[string]bool{
	"TestServedRootScenarioCLISetupUsesRuntimeCoordinates":    true,
	"runInboundAdmissionSupportedSurfacePolicyMatrix":         true,
	"TestDormantIngressProvisionRestartSignedInputBothStores": true,
	"requireNoServedReceiptOutcomeDuring":                     true,
	"servedEventPublishBlockingHandlerAuthorityHook":          true,
	"servedEventPublishReceiptOutcomeCount":                   true,
	"servedEventPublishNodeDeliveryCount":                     true,
	"servedEventPublishNodeDeliveryCountValue":                true,
	"servedEventPayloadObject":                                true,
	"servedEventPublishReceiptCountBySubscribers":             true,
	"servedEventPublishEventEntityID":                         true,
	"assertServedDynamicAutoEmitPayload":                      true,
}
