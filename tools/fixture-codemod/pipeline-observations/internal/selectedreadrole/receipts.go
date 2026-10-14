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
	"strings"
	"sync"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runphysical"
)

const Predecessor = "3a4a08d4156ce28e74529ec3d78ac2640ca98e66"
const Path = "tools/fixture-codemod/pipeline-observations/family145-selected-read-role-preimages.json"
const Digest = "dd9421b12197694434ac1134f95ce1d28e4ab9fad9e5530e830ae9a0393aa5be"

var sourceCache sync.Map

var functions = func() map[string]bool {
	out := map[string]bool{}
	for _, name := range strings.Fields(`TestA2PortfolioPublicTypedIngressRefusesBeforeMutationBothStores a2PortfolioAdmissionDomainCounts TestRunServeRuntimeDevScratchRunForkLifecycleSQLite TestIssue2394ServedOneSecondCommitPreservesTwoFullChunksBothStores assertIssue2394TwoFullChunkHistory assertIssue2394OneSecondPayloads runIssue2394OriginalReporterHTTP assertIssue2394ReporterEvents assertIssue2394ReporterClients readServedForkDeliveryEvidence TestServedForkConnectedDeliveryRouteEvidenceOnBothStores TestServedForkAccumulatorRetainedGenerationBothStores readForkHandlerAccumulators TestServedForkLoopGenerationStateEffectBothStores readForkLoopActivation requireForkLoopStateEffect requireSupplementalForkAcquisitionSettlement requireForkReceiverExecution requireForkReceiverNoPublicationOrNotice requireForkReceiverMutationOwners requireOptionalForkReceiverHeaderProgress requireForkReceiverDelivery runForkReceiverOwnershipJourney requireForkReceiverBusinessMutation requireServedForkConsumerAuthority logServedForkWorkflowRows logServedForkHandlerDiagnostics TestServedLifecycleEmitterCompetingExitPublication lifecycleCompetingTimerDue requireLifecycleFrozenGateControlRefusal requireLifecycleRootLoopForkRevision readLifecycleStoredLoop readLifecycleNodeRecoveryDelivery TestServedCompiledLoopNodeRecoveryReexecutionOnBothStores readLifecycleTemplateGate requireLifecycleTemplateInstance logLifecycleTemplateCompletionRows prepareLifecycleTemplateSiblings requireLifecycleTemplateVerdict completeLifecycleTemplateSiblings TestServedCompiledTransitionTemplateSiblingForkCapabilityRefusalOnBothStores TestExplicitTargetRestrictsConnectedSiblingsBothStores assertNoUnselectedTargetActivation`) {
		out[name] = true
	}
	return out
}()

func ReadPins(root string) ([]runphysical.Pin, error) {
	raw, err := os.ReadFile(filepath.Join(root, Path))
	if err != nil || fmt.Sprintf("%x", sha256.Sum256(raw)) != Digest {
		return nil, fmt.Errorf("selected read-role immutable inventory changed: %v", err)
	}
	var pins []runphysical.Pin
	if err := json.Unmarshal(raw, &pins); err != nil || len(pins) != 43 {
		return nil, fmt.Errorf("selected read-role declaration inventory changed")
	}
	seen := map[string]bool{}
	for _, pin := range pins {
		key := pin.File + "/" + pin.Function
		if seen[key] || !functions[pin.Function] || !strings.HasPrefix(pin.File, "internal/serveapp/") || pin.Before == pin.After || pin.After == "" || fmt.Sprintf("%x", sha256.Sum256([]byte(pin.Before))) != pin.SHA256 {
			return nil, fmt.Errorf("selected read-role complete predecessor changed: %s", key)
		}
		seen[key] = true
	}
	return pins, nil
}

func Body(raw []byte, name string) (string, error) {
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
	if count != 1 {
		return "", fmt.Errorf("selected read-role current declaration missing or duplicated: %s", name)
	}
	return body, nil
}

func ValidateCurrent(root string, pins []runphysical.Pin) error {
	files := map[string][]byte{}
	hash := sha256.New()
	for _, pin := range pins {
		if _, seen := files[pin.File]; seen {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(root, pin.File))
		if err != nil {
			return err
		}
		files[pin.File] = raw
		fmt.Fprintf(hash, "%s:%x\n", pin.File, sha256.Sum256(raw))
	}
	encoded, err := json.Marshal(pins)
	if err != nil {
		return err
	}
	key := fmt.Sprintf("%x/%x", hash.Sum(nil), sha256.Sum256(encoded))
	if _, ok := sourceCache.Load(key); ok {
		return nil
	}
	for _, pin := range pins {
		current, err := Body(files[pin.File], pin.Function)
		if err != nil || current != pin.After {
			return fmt.Errorf("selected read-role actual source changed: %s/%s: %v", pin.File, pin.Function, err)
		}
	}
	sourceCache.Store(key, true)
	return nil
}

func EarlierSource(root, file, function, current string) (string, error) {
	if !strings.HasPrefix(file, "internal/serveapp/") || !functions[function] {
		return current, nil
	}
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
			return "", fmt.Errorf("selected read-role supplied source is not its actual current endpoint: %s", function)
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
	return exec.Command("git", "-C", root, "show", Predecessor+":"+path).Output()
}

func ComposeHistory(root string, prior []byte, recipes bool) ([]byte, error) {
	signed, err := PriorHistory(root, recipes)
	if err != nil || !bytes.Equal(signed, prior) {
		return nil, fmt.Errorf("selected read-role history is not its complete signed predecessor: %v", err)
	}
	pins, err := ReadPins(root)
	if err != nil {
		return nil, err
	}
	var rows []map[string]any
	if err := json.Unmarshal(prior, &rows); err != nil {
		return nil, err
	}
	appends := 0
	for _, pin := range pins {
		var added bool
		rows, added, err = advance(rows, pin, recipes)
		if err != nil {
			return nil, err
		}
		if added {
			appends++
		}
	}
	want := 23
	if recipes {
		want = 22
	}
	if appends != want {
		return nil, fmt.Errorf("selected read-role exact append set changed: %d/%d", appends, want)
	}
	raw, err := json.MarshalIndent(rows, "", "  ")
	return append(raw, '\n'), err
}

func advance(rows []map[string]any, pin runphysical.Pin, recipes bool) ([]map[string]any, bool, error) {
	index := -1
	for i, row := range rows {
		if row["File"] == pin.File && row["Function"] == pin.Function {
			if index != -1 {
				return nil, false, fmt.Errorf("duplicate selected read-role history endpoint: %s", pin.Function)
			}
			index = i
		}
	}
	if index == -1 {
		row := map[string]any{"File": pin.File, "Function": pin.Function, "Before": pin.Before, "After": pin.After}
		if recipes {
			row["Family"] = "selected-served-read-role"
		}
		return append(rows, row), true, nil
	}
	row := rows[index]
	endpoint, _ := row["After"].(string)
	if successor, ok := row["Successor"].(string); ok {
		endpoint = successor
	}
	// The one old source retained original layout. Only its AST-identical
	// spelling bridges; historical bytes, metadata and row order are unchanged.
	if recipes && pin.File == "internal/serveapp/dev_scratch_runtime_test.go" && pin.Function == "TestRunServeRuntimeDevScratchRunForkLifecycleSQLite" {
		normal, err := requestkey.Normalize(endpoint)
		if err != nil {
			return nil, false, err
		}
		endpoint = normal
	}
	if endpoint != pin.Before || row["Removed"] != nil {
		return nil, false, fmt.Errorf("selected read-role original endpoint changed: %s", pin.Function)
	}
	field := "After"
	if recipes {
		field = "Successor"
	}
	row[field] = pin.After
	return rows, false, nil
}

func EarlierHistory(root string, current []byte, recipes bool) ([]byte, error) {
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
	var actual, want []map[string]any
	if json.Unmarshal(current, &actual) != nil || json.Unmarshal(expected, &want) != nil || !reflect.DeepEqual(actual, want) {
		return nil, fmt.Errorf("selected read-role history changed original fields, endpoint or ordering")
	}
	return prior, nil
}
