package store_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"sync"
	"testing"
)

var debtServedSetupDeclarationCache sync.Map

func debtServedSetupDeclarations(path string) (map[string]string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	key := debtBaselineChecksum(raw)
	if bodies, ok := debtServedSetupDeclarationCache.Load(key); ok {
		return bodies.(map[string]string), nil
	}
	file, err := parser.ParseFile(token.NewFileSet(), path, raw, parser.AllErrors)
	if err != nil {
		return nil, err
	}
	bodies := map[string]string{}
	for _, declaration := range file.Decls {
		fn, ok := declaration.(*ast.FuncDecl)
		if !ok || fn.Recv != nil {
			continue
		}
		if _, duplicate := bodies[fn.Name.Name]; duplicate {
			return nil, fmt.Errorf("duplicate served setup declaration: %s", fn.Name.Name)
		}
		var out bytes.Buffer
		if err := format.Node(&out, token.NewFileSet(), fn); err != nil {
			return nil, err
		}
		bodies[fn.Name.Name] = out.String()
	}
	debtServedSetupDeclarationCache.Store(key, bodies)
	return bodies, nil
}

var debtServedSetupStages = [2][2]string{
	{"tools/fixture-codemod/pipeline-observations/family145-served-setup-owner-preimages.json", "b6c64415787cef265ba2a50a08e6c8cdbe9df1829cb5ff2f71a1d50e5f8bbcb8"},
	{"tools/fixture-codemod/pipeline-observations/family145-served-setup-role-preimages.json", "e8921f93982c571ac22d582f5bdedee63982789bb8f6b92f87ebb72327d46967"},
}

func debtServedSetupSourcePins(root string) ([]debtRunIdentitySourceBody, error) {
	merged := map[string]debtRunIdentitySourceBody{}
	for index, stage := range debtServedSetupStages {
		raw, err := os.ReadFile(filepath.Join(root, stage[0]))
		if err != nil || debtBaselineChecksum(raw) != stage[1] {
			return nil, fmt.Errorf("served setup immutable stage changed: %s: %v", stage[0], err)
		}
		var pins []debtRunIdentitySourceBody
		if cached, ok := debtSelectedReadRolePinCache.Load(stage[1]); ok {
			pins = slices.Clone(cached.([]debtRunIdentitySourceBody))
		} else {
			if err := json.Unmarshal(raw, &pins); err != nil || len(pins) != [2]int{4, 101}[index] {
				return nil, fmt.Errorf("served setup stage inventory changed")
			}
			debtSelectedReadRolePinCache.Store(stage[1], slices.Clone(pins))
		}
		seen := map[string]bool{}
		for _, pin := range pins {
			key := pin.File + "/" + pin.Function
			if seen[key] || pin.Before == pin.After || pin.After == "" || debtBaselineChecksum([]byte(pin.Before)) != pin.SHA256 {
				return nil, fmt.Errorf("served setup exact predecessor changed: %s", key)
			}
			seen[key] = true
			if previous, ok := merged[key]; ok {
				if previous.After != pin.Before {
					return nil, fmt.Errorf("served setup intermediate link changed: %s", key)
				}
				pin.Before, pin.SHA256 = previous.Before, previous.SHA256
			}
			merged[key] = pin
		}
	}
	var keys []string
	for key := range merged {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var pins []debtRunIdentitySourceBody
	for _, key := range keys {
		pins = append(pins, merged[key])
	}
	if len(pins) != 104 {
		return nil, fmt.Errorf("served setup composed inventory changed")
	}
	return pins, nil
}

func debtServedSetupLiveSources(root string) ([]debtRunIdentitySourceBody, error) {
	pins, err := debtServedSetupSourcePins(root)
	if err != nil {
		return nil, err
	}
	files := map[string]map[string]string{}
	for _, pin := range pins {
		if files[pin.File] == nil {
			files[pin.File], err = debtServedSetupDeclarations(filepath.Join(root, pin.File))
			if err != nil {
				return nil, err
			}
		}
		current, err := debtStandingControlSourcePrevious(root, pin.File, pin.Function, files[pin.File][pin.Function])
		if err == nil {
			current, err = debtSelectedReadRoleSourcePrevious(root, pin.File, pin.Function, current)
		}
		if err == nil {
			current, err = debtChannelSettlementSourcePrevious(root, pin.File, pin.Function, current)
		}
		if err != nil || current != pin.After {
			return nil, fmt.Errorf("served setup actual current source changed: %s: %v (actual=%s expected=%s)", pin.Function, err, debtBaselineChecksum([]byte(current)), debtBaselineChecksum([]byte(pin.After)))
		}
	}
	return pins, nil
}

func debtServedSetupVerifiedPredecessors(root string) (map[[2]string]string, error) {
	pins, err := debtServedSetupLiveSources(root)
	if err != nil {
		return nil, err
	}
	verified := map[[2]string]string{}
	for _, pin := range pins {
		verified[[2]string{pin.File, pin.Function}] = pin.Before
	}
	return verified, nil
}

func debtServedSetupSourcePrevious(root, file, function, current string) (string, error) {
	var err error
	current, err = debtStandingControlSourcePrevious(root, file, function, current)
	if err == nil {
		current, err = debtSelectedReadRoleSourcePrevious(root, file, function, current)
	}
	if err == nil {
		current, err = debtChannelSettlementSourcePrevious(root, file, function, current)
	}
	if err != nil {
		return "", err
	}
	pins, err := debtServedSetupSourcePins(root)
	if err != nil {
		return "", err
	}
	for _, pin := range pins {
		if pin.File != file || pin.Function != function {
			continue
		}
		if _, err := debtServedSetupLiveSources(root); err != nil {
			return "", err
		}
		if current != pin.After {
			return "", fmt.Errorf("served setup supplied source differs from actual final source: %s", function)
		}
		return pin.Before, nil
	}
	return current, nil
}

func debtServedSetupCopySourceEvidence(t *testing.T, root, copy string) {
	t.Helper()
	pins, err := debtServedSetupLiveSources(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, stage := range debtServedSetupStages {
		raw, err := os.ReadFile(filepath.Join(root, stage[0]))
		if err != nil {
			t.Fatal(err)
		}
		debtWriteModuleSource(t, copy, stage[0], string(raw))
	}
	for _, pin := range pins {
		debtSourceBlobCopyDeclaration(t, copy, pin.File, pin.Function, pin.After)
	}
	debtChannelSettlementCopySources(t, root, copy)
	debtSelectedReadRoleCopySources(t, root, copy)
	debtStandingControlCopySources(t, root, copy)
	debtChannelProcessCopySources(t, root, copy)
}

func TestPersistenceAuthorityDebtServedSetupSourceInverseRejectsStaleAndForeignCuts(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	pins, err := debtServedSetupLiveSources(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, pin := range pins {
		bodies, err := debtServedSetupDeclarations(filepath.Join(root, pin.File))
		if err != nil {
			t.Fatal(err)
		}
		got, err := debtServedSetupSourcePrevious(root, pin.File, pin.Function, bodies[pin.Function])
		if err != nil || got != pin.Before {
			t.Fatal("exact setup source inverse failed:", pin.Function, err)
		}
		if _, err := debtServedSetupSourcePrevious(root, pin.File, pin.Function, pin.Before); err == nil {
			t.Fatal("old supplied body substituted for actual source:", pin.Function)
		}
		if got, err := debtServedSetupSourcePrevious(root, "foreign.go", pin.Function, pin.After); err != nil || got != pin.After {
			t.Fatal("foreign setup source received an inverse:", err)
		}
	}
	copy := t.TempDir()
	debtServedSetupCopySourceEvidence(t, root, copy)
	debtSourceBlobCopyDeclaration(t, copy, pins[0].File, pins[0].Function, pins[0].Before)
	if _, err := debtServedSetupSourcePrevious(copy, pins[1].File, pins[1].Function, pins[1].After); err == nil {
		t.Fatal("unchanged supplied string bypassed another stale actual source")
	}
}
