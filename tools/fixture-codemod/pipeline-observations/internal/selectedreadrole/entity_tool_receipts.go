package selectedreadrole

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/division-sh/swarm/internal/checkoutsource"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
)

const EntityToolPredecessor = "6a82aaca9326883cbaa296730da773396d9a8801"
const EntityToolPath = "tools/fixture-codemod/pipeline-observations/family145-entity-tool-gate-preimages.json"
const EntityToolDigest = "5a8f6d5842ff3e7bac0578534613dc0355ba96cb854fdeaac3446a4b8eb9676a"
const entityToolFileDigest = "4636b7d80b7e8403fdb5ab52ef34b08aaa8293031b27b1f3dc1a404dee68fc5b"

type EntityToolPin struct {
	File, Function, Before, After string
	Removed                       bool `json:"Removed,omitempty"`
}

func ReadEntityToolPins(root string) ([]EntityToolPin, error) {
	raw, err := os.ReadFile(filepath.Join(root, EntityToolPath))
	if err != nil {
		return nil, err
	}
	if fmt.Sprintf("%x", sha256.Sum256(raw)) != entityToolFileDigest {
		return nil, fmt.Errorf("entity-tool complete manifest bytes changed")
	}
	var pins []EntityToolPin
	if err := json.Unmarshal(raw, &pins); err != nil {
		return nil, err
	}
	canonical, err := json.Marshal(pins)
	if err != nil || len(pins) != 43 || fmt.Sprintf("%x", sha256.Sum256(canonical)) != EntityToolDigest {
		return nil, fmt.Errorf("entity-tool complete source inventory changed: %v", err)
	}
	return pins, nil
}

func entityToolDeclarations(raw []byte) (map[string]string, map[string]int, error) {
	file, err := parser.ParseFile(token.NewFileSet(), "source.go", raw, parser.AllErrors)
	if err != nil {
		return nil, nil, err
	}
	bodies, references := map[string]string{}, map[string]int{}
	for _, declaration := range file.Decls {
		fn, ok := declaration.(*ast.FuncDecl)
		if !ok {
			continue
		}
		if _, exists := bodies[fn.Name.Name]; exists {
			return nil, nil, fmt.Errorf("duplicate entity-tool declaration: %s", fn.Name.Name)
		}
		bodies[fn.Name.Name] = requestkey.Node(fn)
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			if id, ok := node.(*ast.Ident); ok {
				references[id.Name]++
			}
			return true
		})
	}
	return bodies, references, nil
}

func ValidateEntityToolCurrent(root string, pins []EntityToolPin) error {
	authenticated, err := ReadEntityToolPins(root)
	if err != nil || !reflect.DeepEqual(authenticated, pins) {
		return fmt.Errorf("entity-tool supplied inventory is not authenticated: %v", err)
	}
	current, key, err := entityToolCurrentFiles(root, pins)
	if err != nil {
		return err
	}
	referenceFiles, referenceKey, err := entityToolReferenceFiles(root)
	if err != nil {
		return err
	}
	key += referenceKey
	if _, ok := sourceCache.Load(key); ok {
		return nil
	}
	references, err := entityToolReferenceCounts(referenceFiles)
	if err != nil {
		return err
	}
	previous := map[string]map[string]string{}
	for _, pin := range pins {
		if previous[pin.File] != nil {
			continue
		}
		raw, err := exec.Command("git", "-C", root, "show", EntityToolPredecessor+":"+pin.File).Output()
		if err != nil {
			return err
		}
		previous[pin.File], _, err = entityToolDeclarations(raw)
		if err != nil {
			return err
		}
	}
	for _, pin := range pins {
		if previous[pin.File][pin.Function] != pin.Before {
			return fmt.Errorf("entity-tool signed predecessor changed: %s", pin.Function)
		}
		body, found := current[pin.File][pin.Function]
		if pin.Removed {
			if found || references[pin.Function] != 0 {
				return fmt.Errorf("retired entity-tool declaration/reference survived: %s", pin.Function)
			}
		} else if !found || body != pin.After {
			return fmt.Errorf("entity-tool live sibling changed: %s", pin.Function)
		}
	}
	sourceCache.Store(key, true)
	return nil
}

func entityToolCurrentFiles(root string, pins []EntityToolPin) (map[string]map[string]string, string, error) {
	current := map[string]map[string]string{}
	hash := sha256.New()
	for _, pin := range pins {
		if current[pin.File] != nil {
			continue
		}
		path := filepath.Join(root, pin.File)
		owned, err := checkoutsource.Member(root, path)
		if err != nil || !owned {
			return nil, "", fmt.Errorf("entity-tool source is not owned: %s: %v", pin.File, err)
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
		current[pin.File], _, err = entityToolDeclarations(raw)
		if err != nil {
			return nil, "", err
		}
	}
	return current, fmt.Sprintf("entity-tool/%s/%x/", root, hash.Sum(nil)), nil
}

// Unexported retired helpers can survive only inside their owning package.
// Scan every owned Go file, including package initializers and excluded files.
func entityToolReferenceFiles(root string) (map[string][]byte, string, error) {
	files := map[string][]byte{}
	hash := sha256.New()
	err := checkoutsource.WalkDir(root, filepath.Join(root, "internal/runtime/tools"), func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") {
			return err
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		files[path] = raw
		fmt.Fprintf(hash, "%s:%x\n", path, sha256.Sum256(raw))
		return nil
	})
	return files, fmt.Sprintf("%x", hash.Sum(nil)), err
}

func entityToolReferenceCounts(files map[string][]byte) (map[string]int, error) {
	references := map[string]int{}
	for path, raw := range files {
		file, err := parser.ParseFile(token.NewFileSet(), path, raw, parser.AllErrors)
		if err != nil {
			return nil, err
		}
		ast.Inspect(file, func(node ast.Node) bool {
			if id, ok := node.(*ast.Ident); ok {
				references[id.Name]++
			}
			return true
		})
	}
	return references, nil
}

// This inverse is called at the live-source entry, never on historical text.
func EarlierEntityToolSource(root, file, function, current string) (string, error) {
	var integrationErr error
	current, integrationErr = CatchUp2542HistoricalFunction(root, file, function, current)
	if integrationErr != nil {
		return "", integrationErr
	}
	pins, err := ReadEntityToolPins(root)
	if err != nil {
		return "", err
	}
	for _, pin := range pins {
		if pin.File != file || pin.Function != function {
			continue
		}
		if err := ValidateEntityToolCurrent(root, pins); err != nil {
			return "", err
		}
		if pin.Removed {
			if current != "" {
				return "", fmt.Errorf("retired entity-tool supplied declaration is not absent: %s", function)
			}
		} else if current != pin.After {
			return "", fmt.Errorf("entity-tool supplied source is not its live endpoint: %s", function)
		}
		return pin.Before, nil
	}
	return current, nil
}

func PriorEntityToolHistory(root string, recipes bool) ([]byte, error) {
	path := "tools/fixture-codemod/pipeline-observations/family145-transitions.json"
	if recipes {
		path = "tools/fixture-codemod/pipeline-observations/recipes.json"
	}
	return exec.Command("git", "-C", root, "show", EntityToolPredecessor+":"+path).Output()
}

func ComposeEntityToolHistory(root string, prior []byte, recipes bool) ([]byte, error) {
	pins, err := ReadEntityToolPins(root)
	if err != nil {
		return nil, err
	}
	if err := ValidateEntityToolCurrent(root, pins); err != nil {
		return nil, err
	}
	trusted, err := PriorEntityToolHistory(root, recipes)
	var rows, signed []map[string]any
	if err != nil || json.Unmarshal(prior, &rows) != nil || json.Unmarshal(trusted, &signed) != nil || !reflect.DeepEqual(rows, signed) {
		return nil, fmt.Errorf("entity-tool history is not its complete signed predecessor: %v", err)
	}
	for _, pin := range pins {
		rows, err = advanceEntityToolHistory(rows, pin, recipes)
		if err != nil {
			return nil, err
		}
	}
	return json.Marshal(rows)
}

func advanceEntityToolHistory(rows []map[string]any, pin EntityToolPin, recipes bool) ([]map[string]any, error) {
	match := -1
	for i, row := range rows {
		if row["File"] == pin.File && row["Function"] == pin.Function {
			if match != -1 {
				return nil, fmt.Errorf("duplicate entity-tool history: %s", pin.Function)
			}
			match = i
		}
	}
	if match < 0 {
		row := map[string]any{"File": pin.File, "Function": pin.Function, "Before": pin.Before, "After": pin.After}
		if recipes {
			row["Family"] = "native-entity-tool-gate-family145"
		}
		if pin.Removed {
			row["Removed"] = true
			if recipes {
				row["After"] = pin.Before
			}
		}
		return append(rows, row), nil
	}
	row := rows[match]
	current := row["After"]
	if recipes && row["Successor"] != nil {
		current = row["Successor"]
	}
	canonical, err := requestkey.Normalize(fmt.Sprint(current))
	if err != nil || canonical != pin.Before || row["Removed"] == true {
		return nil, fmt.Errorf("entity-tool current historical endpoint differs: %s", pin.Function)
	}
	if pin.Removed {
		row["Removed"] = true
		return rows, nil
	}
	field := "After"
	if recipes {
		field = "Successor"
	}
	row[field] = pin.After
	return rows, nil
}

func EarlierEntityToolHistory(root string, current []byte, recipes bool) ([]byte, error) {
	prior, err := PriorEntityToolHistory(root, recipes)
	if err != nil {
		return nil, err
	}
	expected, err := ComposeEntityToolHistory(root, prior, recipes)
	if err != nil {
		return nil, err
	}
	var have, want any
	if json.Unmarshal(current, &have) != nil || json.Unmarshal(expected, &want) != nil || !reflect.DeepEqual(have, want) {
		return nil, fmt.Errorf("entity-tool current history contains foreign changes")
	}
	return prior, nil
}
