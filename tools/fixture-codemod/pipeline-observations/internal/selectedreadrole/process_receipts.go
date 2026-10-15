package selectedreadrole

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
)

const ProcessPredecessor = "54e2e053202a29218e3ce8a88edfacb1b2e34715"
const ProcessPath = "tools/fixture-codemod/pipeline-observations/family145-process-read-observer-preimages.json"
const ProcessDigest = "21c382c784e8dab595de6dda9f3e10e25775a118e631d174a411bb9412eb6de1"

var ProcessFunctions = map[string][]string{
	"internal/serveapp/main_runtime_test.go": {
		"loadServedDirectiveOperationByKey", "requireServedAgentDirectivePersistence",
		"requireServedDirectiveFailureEnvelope", "requireServedTestSetupPersistence",
	},
	"internal/serveapp/agent_memory_supported_surface_test.go": {
		"loadStandingMemorySessions", "loadStandingTelegramEvidenceCounts",
		"requireStandingMemoryAttemptCount", "waitForStandingMemoryCompletion",
	},
	"internal/serveapp/agent_mock_supported_surface_test.go": {
		"assertMockAtLeast", "assertMockCount", "assertMockSupportedEvidence",
		"mockConnectorDecisionCardState", "mockConnectorFailureEvidence",
		"mockJSONTextExpression", "mockPlaceholder", "openMockProofDB",
		"waitForMockAgentTurns", "waitForMockConnectorAttempts", "waitForMockConnectorDecisionCard",
	},
}

func ProcessBody(raw []byte, name string) (string, error) {
	file, err := parser.ParseFile(token.NewFileSet(), "source.go", raw, parser.AllErrors)
	if err != nil {
		return "", err
	}
	body, count := "", 0
	for _, decl := range file.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Recv == nil && fn.Name.Name == name {
			body, count = requestkey.Node(fn), count+1
		}
	}
	if count > 1 || (count == 0 && name != "mockJSONTextExpression" && name != "mockPlaceholder") {
		return "", fmt.Errorf("process observation declaration missing or duplicated: %s", name)
	}
	return body, nil
}

func ReadProcessPins(root string) ([]runphysical.Pin, error) {
	raw, err := os.ReadFile(filepath.Join(root, ProcessPath))
	if err != nil || fmt.Sprintf("%x", sha256.Sum256(raw)) != ProcessDigest {
		return nil, fmt.Errorf("process observation immutable inventory changed: %v", err)
	}
	var pins []runphysical.Pin
	if err := json.Unmarshal(raw, &pins); err != nil || len(pins) != 19 {
		return nil, fmt.Errorf("process observation exact declaration set changed: %v", err)
	}
	seen := map[string]bool{}
	for _, pin := range pins {
		key := pin.File + "/" + pin.Function
		if !ProcessFunction(pin.File, pin.Function) || seen[key] || pin.Before == "" || (pin.Before == pin.After && pin.Function != "waitForMockConnectorDecisionCard") || fmt.Sprintf("%x", sha256.Sum256([]byte(pin.Before))) != pin.SHA256 {
			return nil, fmt.Errorf("process observation complete declaration changed: %s", key)
		}
		seen[key] = true
	}
	return pins, nil
}

func ValidateProcessCurrent(root string, pins []runphysical.Pin) error {
	for _, pin := range pins {
		raw, err := os.ReadFile(filepath.Join(root, pin.File))
		if err != nil {
			return err
		}
		current, err := ProcessBody(raw, pin.Function)
		if err != nil || current != pin.After {
			return fmt.Errorf("process observation endpoint changed: %s: %v", pin.Function, err)
		}
	}
	return nil
}

func EarlierProcessSource(root, file, function, current string) (string, error) {
	for _, name := range ProcessFunctions[file] {
		if name != function {
			continue
		}
		pins, err := ReadProcessPins(root)
		if err != nil {
			return "", err
		}
		if err := ValidateProcessCurrent(root, pins); err != nil {
			return "", err
		}
		for _, pin := range pins {
			if pin.File == file && pin.Function == function && current == pin.After {
				return pin.Before, nil
			}
		}
		return "", fmt.Errorf("process observation supplied endpoint is not current: %s", function)
	}
	return current, nil
}

func PriorProcessHistory(root string, recipes bool) ([]byte, error) {
	path := "tools/fixture-codemod/pipeline-observations/family145-transitions.json"
	if recipes {
		path = "tools/fixture-codemod/pipeline-observations/recipes.json"
	}
	return exec.Command("git", "-C", root, "show", ProcessPredecessor+":"+path).Output()
}

func ComposeProcessHistory(root string, prior []byte, recipes bool) ([]byte, error) {
	signed, err := PriorProcessHistory(root, recipes)
	if err != nil || !bytes.Equal(signed, prior) {
		return nil, fmt.Errorf("process observation history is not its complete signed predecessor: %v", err)
	}
	pins, err := ReadProcessPins(root)
	if err != nil {
		return nil, err
	}
	var rows []map[string]any
	if err := json.Unmarshal(prior, &rows); err != nil {
		return nil, err
	}
	for _, pin := range pins {
		if pin.Before == pin.After {
			continue
		}
		for _, row := range rows {
			if row["File"] == pin.File && row["Function"] == pin.Function {
				return nil, fmt.Errorf("process observation append replaces historical record: %s", pin.Function)
			}
		}
		row := map[string]any{"File": pin.File, "Function": pin.Function, "Before": pin.Before, "After": pin.After}
		if pin.After == "" {
			row["Removed"] = true
		}
		if recipes {
			row["Family"] = "native-independent-process-read-observer"
			if pin.After == "" {
				// Existing pure-retirement receipts retain their historical body;
				// checkpointing requires absence instead of restoring the escape.
				row["After"] = pin.Before
			}
		}
		rows = append(rows, row)
	}
	raw, err := json.MarshalIndent(rows, "", "  ")
	return append(raw, '\n'), err
}

// Accept only the exact signed prior receipt or its eighteen append-only rows.
// Historical tools may consume the prior receipt without pretending it is live.
func EarlierProcessHistory(root string, current []byte, recipes bool) ([]byte, error) {
	prior, err := PriorProcessHistory(root, recipes)
	if err != nil {
		return nil, err
	}
	if equalProcessHistory(current, prior) {
		return prior, nil
	}
	pins, err := ReadProcessPins(root)
	if err != nil {
		return nil, err
	}
	if err := ValidateProcessCurrent(root, pins); err != nil {
		return nil, err
	}
	expected, err := ComposeProcessHistory(root, prior, recipes)
	if err != nil || !equalProcessHistory(current, expected) {
		return nil, fmt.Errorf("process observation history changed fields, membership or ordering: %v", err)
	}
	return prior, nil
}

func equalProcessHistory(left, right []byte) bool {
	var actual, expected []map[string]any
	return json.Unmarshal(left, &actual) == nil && json.Unmarshal(right, &expected) == nil && reflect.DeepEqual(actual, expected)
}

func ProcessFunction(file, name string) bool {
	for _, function := range ProcessFunctions[file] {
		if name == function {
			return true
		}
	}
	return false
}
