package selectedreadrole

import (
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
	"strings"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
)

const DeliveryRecoveryPredecessor = "3bb704336cdbce8e70a17e0190de6b96b453707a"
const DeliveryRecoveryPath = "tools/fixture-codemod/pipeline-observations/family145-delivery-recovery-preimages.json"
const DeliveryRecoveryFile = "internal/runtime/conformance/delivery_lifecycle_conformance_test.go"
const DeliveryRecoveryStartupFile = "internal/runtime/node_delivery_startup_recovery_test.go"
const deliveryRecoveryFileDigest = "1251641db3f6d30732ac90f9d3b4d58726bd04717ec38f73a812450b73a81c0f"
const deliveryRecoveryDigest = "375a5ec3d99170811825c11a522ad34e69f984f3392c9c6e18fee0a05ec070ec"
const deliveryRecoveryDeclarationCount = 22

func ReadDeliveryRecoveryPins(root string) ([]EntityToolPin, error) {
	raw, err := os.ReadFile(filepath.Join(root, DeliveryRecoveryPath))
	if err != nil || fmt.Sprintf("%x", sha256.Sum256(raw)) != deliveryRecoveryFileDigest {
		return nil, fmt.Errorf("delivery recovery complete manifest bytes changed: %v", err)
	}
	key := "channel-process-pins/" + deliveryRecoveryFileDigest
	if cached, ok := sourceCache.Load(key); ok {
		return append([]EntityToolPin(nil), cached.([]EntityToolPin)...), nil
	}
	var pins []EntityToolPin
	if err := json.Unmarshal(raw, &pins); err != nil {
		return nil, err
	}
	canonical, err := json.Marshal(pins)
	if err != nil || len(pins) != deliveryRecoveryDeclarationCount || fmt.Sprintf("%x", sha256.Sum256(canonical)) != deliveryRecoveryDigest {
		return nil, fmt.Errorf("delivery recovery declaration inventory changed: %v", err)
	}
	sourceCache.Store(key, append([]EntityToolPin(nil), pins...))
	return pins, nil
}

func ValidateDeliveryRecoveryCurrent(root string, pins []EntityToolPin) error {
	authenticated, err := ReadDeliveryRecoveryPins(root)
	if err != nil || !reflect.DeepEqual(authenticated, pins) {
		return fmt.Errorf("delivery recovery supplied inventory is not authenticated: %v", err)
	}
	files, key, err := channelProcessCurrentFiles(root, pins)
	if err != nil {
		return err
	}
	key = "channel-process/" + deliveryRecoveryDigest + "/" + key
	if _, ok := sourceCache.Load(key); ok {
		return nil
	}
	if err := validateDeliveryRecoveryCarrier(root, files[DeliveryRecoveryFile]); err != nil {
		return err
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
			raw, err := exec.Command("git", "-C", root, "show", DeliveryRecoveryPredecessor+":"+pin.File).Output()
			if err != nil {
				return err
			}
			previous[pin.File], err = standingIngressDeclarations(raw)
			if err != nil {
				return err
			}
		}
		if previous[pin.File][pin.Function] != pin.Before || pin.Before == pin.After || pin.Removed != (pin.After == "") {
			return fmt.Errorf("delivery recovery signed declaration changed: %s", pin.Function)
		}
		if current[pin.File][pin.Function] != pin.After {
			return fmt.Errorf("delivery recovery actual source changed: %s", pin.Function)
		}
	}
	sourceCache.Store(key, true)
	return nil
}

const deliveryRecoveryCarrierBefore = `type deliveryLifecycleConformanceBackend struct {
name string
store runtimedelivery.Store
restart runtimedelivery.Store
selected any
db *sql.DB
postgres bool
}`
const deliveryRecoveryCarrierAfter = `type deliveryLifecycleConformanceBackend struct {
name string
store runtimedelivery.Store
restart runtimedelivery.Store
selected any
postgres bool
}`

func validateDeliveryRecoveryCarrier(root string, current []byte) error {
	previous, err := exec.Command("git", "-C", root, "show", DeliveryRecoveryPredecessor+":"+DeliveryRecoveryFile).Output()
	if err != nil {
		return err
	}
	for _, cut := range []struct {
		raw  []byte
		want string
	}{{previous, deliveryRecoveryCarrierBefore}, {current, deliveryRecoveryCarrierAfter}} {
		file, err := parser.ParseFile(token.NewFileSet(), "carrier.go", cut.raw, parser.AllErrors)
		if err != nil {
			return err
		}
		var actual []string
		for _, declaration := range file.Decls {
			group, ok := declaration.(*ast.GenDecl)
			if !ok || group.Tok != token.TYPE {
				continue
			}
			for _, spec := range group.Specs {
				typ, ok := spec.(*ast.TypeSpec)
				if ok && typ.Name.Name == "deliveryLifecycleConformanceBackend" {
					actual = append(actual, requestkey.Node(group))
				}
			}
		}
		if len(actual) != 1 || strings.Join(strings.Fields(actual[0]), " ") != strings.Join(strings.Fields(cut.want), " ") {
			return fmt.Errorf("delivery recovery carrier is not its exact signed field retirement")
		}
	}
	return nil
}

func EarlierDeliveryRecoverySource(root, file, function, current string) (string, error) {
	var err error
	current, err = EarlierServedScenarioSource(root, file, function, current)
	if err != nil {
		return "", err
	}
	if file != DeliveryRecoveryFile && file != DeliveryRecoveryStartupFile {
		return current, nil
	}
	pins, err := ReadDeliveryRecoveryPins(root)
	if err != nil {
		return "", err
	}
	for _, pin := range pins {
		if pin.File != file || pin.Function != function {
			continue
		}
		if err := ValidateDeliveryRecoveryCurrent(root, pins); err != nil {
			return "", err
		}
		return DeliveryRecoveryPrevious(pins, file, function, current)
	}
	return current, nil
}

// The caller authenticates the complete pins and actual source once before a
// finite batch comparison. This is the same exact per-declaration inverse.
func DeliveryRecoveryPrevious(pins []EntityToolPin, file, function, current string) (string, error) {
	for _, pin := range pins {
		if pin.File != file || pin.Function != function {
			continue
		}
		if current != pin.After {
			return "", fmt.Errorf("delivery recovery supplied source is not its live endpoint: %s", function)
		}
		return pin.Before, nil
	}
	return current, nil
}

func PriorDeliveryRecoveryHistory(root string, recipes bool) ([]byte, error) {
	path := "tools/fixture-codemod/pipeline-observations/family145-transitions.json"
	if recipes {
		path = "tools/fixture-codemod/pipeline-observations/recipes.json"
	}
	return exec.Command("git", "-C", root, "show", DeliveryRecoveryPredecessor+":"+path).Output()
}

func ComposeDeliveryRecoveryHistory(root string, prior []byte, recipes bool) ([]byte, error) {
	pins, err := ReadDeliveryRecoveryPins(root)
	if err != nil {
		return nil, err
	}
	if err := ValidateDeliveryRecoveryCurrent(root, pins); err != nil {
		return nil, err
	}
	trusted, err := PriorDeliveryRecoveryHistory(root, recipes)
	var rows, signed []map[string]any
	if err != nil || json.Unmarshal(prior, &rows) != nil || json.Unmarshal(trusted, &signed) != nil || !reflect.DeepEqual(rows, signed) {
		return nil, fmt.Errorf("delivery recovery history is not its complete signed predecessor: %v", err)
	}
	for _, pin := range pins {
		before := len(rows)
		if recipes {
			rows, err = advanceEntityToolHistory(rows, pin, true)
		} else {
			rows, err = appendDeliveryRecoveryTransition(rows, pin)
		}
		if err != nil {
			return nil, err
		}
		if recipes && len(rows) > before {
			rows[before]["Family"] = "native-delivery-recovery-carrier-family145"
		}
	}
	return json.Marshal(rows)
}

func appendDeliveryRecoveryTransition(rows []map[string]any, pin EntityToolPin) ([]map[string]any, error) {
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

func EarlierDeliveryRecoveryHistory(root string, current []byte, recipes bool) ([]byte, error) {
	prior, err := PriorDeliveryRecoveryHistory(root, recipes)
	if err != nil {
		return nil, err
	}
	expected, err := ComposeDeliveryRecoveryHistory(root, prior, recipes)
	if err != nil {
		return nil, err
	}
	var have, want any
	if json.Unmarshal(current, &have) != nil || json.Unmarshal(expected, &want) != nil || !reflect.DeepEqual(have, want) {
		return nil, fmt.Errorf("delivery recovery current history contains foreign changes")
	}
	return prior, nil
}

// Older owners still validate their complete predecessor. Peel this stage only
// when one of its exact new endpoints is actually present in the supplied ledger.
func BeforeDeliveryRecoveryHistory(root string, current []byte, recipes bool) ([]byte, error) {
	var err error
	current, err = BeforeServedScenarioHistory(root, current, recipes)
	if err != nil {
		return nil, err
	}
	var rows []map[string]any
	if err := json.Unmarshal(current, &rows); err != nil {
		return nil, err
	}
	pins, err := ReadDeliveryRecoveryPins(root)
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
				return EarlierDeliveryRecoveryHistory(root, current, recipes)
			}
		}
	}
	return current, nil
}
