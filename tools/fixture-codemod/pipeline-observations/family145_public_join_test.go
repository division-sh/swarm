package main

import (
	"crypto/sha256"
	_ "embed"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/checkoutsource"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/publicjoin"
)

//go:embed family145-public-join-preimages.json
var family145PublicJoinBytes []byte

var publicJoinHelpers = map[string]string{
	"a2ReadJoinPublicObligation": "Endpoint", "a2RequireJoinPublicGraph": "BundleHash",
	"a2ReadJoinPublicEvent": "Endpoint", "a2WaitJoinPublicRun": "Endpoint",
	"a2RequireJoinPublicClients": "Endpoint", "a2JoinPublicCLI": "Endpoint",
	"a2RequireJoinPublicTrace": "Endpoint",
}

func publicJoinPins(t *testing.T) []publicjoin.Pin {
	t.Helper()
	pins, err := publicjoin.Decode(family145PublicJoinBytes)
	if err != nil {
		t.Fatal(err)
	}
	return pins
}

func family145PublicJoinCallerInverse(function, current string) string {
	pins, err := publicjoin.Decode(family145PublicJoinBytes)
	if err != nil {
		panic(err)
	}
	for _, pin := range pins {
		if pin.Function == function {
			return publicjoin.Previous(pins, pin.File, function, current)
		}
	}
	return current
}

func family145CheckPublicJoinWholeRule(t *testing.T, file, function, current string) bool {
	t.Helper()
	for _, pin := range publicJoinPins(t) {
		if pin.File == file && pin.Function == function {
			if current != pin.After {
				t.Fatal("public join caller escaped complete source rule:", file, function)
			}
			return true
		}
	}
	return false
}

func TestFamily145PublicJoinWholeChainPreserves31CallsSevenHelpersAndAllPublicOracles(t *testing.T) {
	calls, helpers := 0, 0
	seen := map[string]bool{}
	for _, pin := range publicJoinPins(t) {
		key := pin.File + "/" + pin.Function
		if seen[key] || fmt.Sprintf("%x", sha256.Sum256([]byte(pin.Before))) != pin.SHA256 {
			t.Fatal("changed or duplicate complete public join preimage:", key)
		}
		seen[key] = true
		calls += pin.Calls
		if publicJoinHelpers[pin.Function] != "" {
			helpers++
		}
		if publicJoinExpected(pin) != pin.After {
			t.Fatal("public join changed more than the original endpoint/hash projection:", key)
		}
		raw, err := os.ReadFile(filepath.Join("../../..", pin.File))
		if err != nil {
			t.Fatal(err)
		}
		if servedSetupEarlierSource(t, pin.File, pin.Function, family145PublicA2CurrentDeclaration(t, raw, pin.Function)) != pin.After {
			t.Fatal("current public join escaped complete source conservation:", key)
		}
		publicJoinHostiles(t, pin)
	}
	if calls != 31 || helpers != 7 || len(seen) != 10 {
		t.Fatalf("public join inventory=%d/%d/%d want31/7/10", calls, helpers, len(seen))
	}
}

func publicJoinExpected(pin publicjoin.Pin) string {
	expected := pin.Before
	projection := publicJoinHelpers[pin.Function]
	if projection != "" {
		argument := "endpoint"
		if projection == "BundleHash" {
			argument = "bundleHash"
		}
		expected = strings.Replace(expected, "rt servedControlProofRuntime", argument+" string", 1)
		expected = strings.ReplaceAll(expected, "rt."+projection, argument)
	}
	for helper, field := range publicJoinHelpers {
		argument := "rt." + field
		if projection != "" {
			argument = "endpoint"
		}
		expected = strings.ReplaceAll(expected, helper+"(t, rt,", helper+"(t, "+argument+",")
	}
	return expected
}

func publicJoinHostiles(t *testing.T, pin publicjoin.Pin) {
	t.Helper()
	pins := publicJoinPins(t)
	if publicjoin.Previous(pins, pin.File, pin.Function, pin.After) != pin.Before {
		t.Fatal("public join complete inverse lost original source")
	}
	for _, location := range [][2]string{{pin.File + ".foreign", pin.Function}, {pin.File, pin.Function + "Foreign"}} {
		if publicjoin.Previous(pins, location[0], location[1], pin.After) != pin.After {
			t.Fatal("public join admitted foreign file or declaration")
		}
	}
	finite := recipe{File: pin.File, Function: pin.Function, Before: pin.Before, After: pin.After}
	for _, cut := range []string{"endpoint", "bundleHash", "rt.Endpoint", "rt.BundleHash", "run_id", "event_id", "entity.get", "run.diagnose", "run.trace", "JoinRef", "Generation", "DeepEqual", "DeadlineAt", "stable == 4", "25 * time.Millisecond", "stale_arrival", "join_closed", "NextCursor", "process.stop", "arm.Key()"} {
		if !strings.Contains(pin.After, cut) {
			continue
		}
		mutant := strings.Replace(pin.After, cut, "foreignJoinCut", 1)
		if _, _, err := rewriteFunction(pin.File, []byte("package proof\n"+mutant), finite); err == nil {
			t.Fatal("lost public join assertion/identity/settlement cut accepted:", pin.Function, cut)
		}
		if publicjoin.Previous(pins, pin.File, pin.Function, mutant) != mutant {
			t.Fatal("altered full public join body normalized")
		}
	}
}

func TestFamily145PublicJoinOwnedInventoryRejectsBroadAndEscapedHelperAuthority(t *testing.T) {
	root := "../../.."
	calls, helpers := 0, 0
	err := checkoutsource.WalkDir(root, filepath.Join(root, "internal/serveapp"), func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") {
			return err
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.AllErrors)
		if err != nil {
			return err
		}
		c, h := publicJoinInventory(t, file)
		calls += c
		helpers += h
		return nil
	})
	if err != nil || calls != 31 || helpers != 7 {
		t.Fatalf("public join owned inventory calls=%d helpers=%d err=%v", calls, helpers, err)
	}
}

func publicJoinInventory(t *testing.T, file *ast.File) (calls, helpers int) {
	t.Helper()
	allowed := map[token.Pos]bool{}
	ast.Inspect(file, func(node ast.Node) bool {
		switch n := node.(type) {
		case *ast.FuncDecl:
			if field := publicJoinHelpers[n.Name.Name]; field != "" {
				helpers++
				allowed[n.Name.Pos()] = true
				publicJoinRequireProjection(t, n, field)
			}
		case *ast.CallExpr:
			if id, ok := n.Fun.(*ast.Ident); ok && publicJoinHelpers[id.Name] != "" {
				calls++
				allowed[id.Pos()] = true
				if len(n.Args) < 2 || family145NodeString(n.Args[1]) == "rt" {
					t.Error("public join caller restored broad carrier")
				}
			}
		}
		return true
	})
	ast.Inspect(file, func(node ast.Node) bool {
		if id, ok := node.(*ast.Ident); ok && publicJoinHelpers[id.Name] != "" && !allowed[id.Pos()] {
			t.Error("public join helper escaped its finite direct call inventory")
		}
		return true
	})
	return calls, helpers
}

func publicJoinRequireProjection(t *testing.T, function *ast.FuncDecl, field string) {
	t.Helper()
	if len(function.Type.Params.List) < 2 {
		t.Error("public join helper lost its declared projection")
		return
	}
	parameter := function.Type.Params.List[1]
	name := "endpoint"
	if field == "BundleHash" {
		name = "bundleHash"
	}
	if len(parameter.Names) != 1 || parameter.Names[0].Name != name || family145NodeString(parameter.Type) != "string" {
		t.Error("public join helper restored broad authority")
	}
}
