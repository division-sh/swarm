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
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/checkoutsource"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/channeleventcount"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
)

//go:embed family145-channel-event-count-preimages.json
var family145ChannelEventCountBytes []byte

func channelEventCountPins(t *testing.T) []channeleventcount.Pin {
	t.Helper()
	pins, err := channeleventcount.Decode(family145ChannelEventCountBytes)
	if err != nil {
		t.Fatal(err)
	}
	return pins
}

func family145ChannelEventCountPrevious(t *testing.T, file, function, current string) string {
	t.Helper()
	return channeleventcount.Previous(channelEventCountPins(t), file, function, current)
}

func family145ChannelEventCountCallerInverse(function, current string) string {
	pins, err := channeleventcount.Decode(family145ChannelEventCountBytes)
	if err != nil {
		panic(err)
	}
	for _, pin := range pins {
		if pin.Function == function {
			return channeleventcount.Previous(pins, pin.File, function, current)
		}
	}
	return current
}

func family145CheckChannelEventCountWholeRule(t *testing.T, file, function, current string) bool {
	t.Helper()
	for _, pin := range channelEventCountPins(t) {
		if pin.File == file && pin.Function == function {
			if current != pin.After {
				t.Fatal("channel event count escaped exact identity/assertion/cut:", file, function)
			}
			return true
		}
	}
	return false
}

func TestFamily145ChannelEventCountWholeRuleRetainsEveryScopeAndAssertion(t *testing.T) {
	proof := sourceBlobDeclarationsForInvocation(t)
	calls, seen := 0, map[string]bool{}
	for _, pin := range channelEventCountPins(t) {
		key := pin.File + "/" + pin.Function
		if seen[key] || fmt.Sprintf("%x", sha256.Sum256([]byte(pin.Before))) != pin.SHA256 {
			t.Fatal("duplicate or changed channel event count source:", key)
		}
		seen[key] = true
		original, err := exec.Command("git", "show", channeleventcount.Predecessor+":"+pin.File).Output()
		if err != nil || requestKeyDeclaration(t, original, pin.Function) != pin.Before {
			t.Fatal("channel event count predecessor is not exact:", key, err)
		}
		current, err := os.ReadFile(filepath.Join("../../..", pin.File))
		if err != nil || proof.previous(t, pin.File, pin.Function, requestKeyDeclaration(t, current, pin.Function)) != pin.After {
			t.Fatal("channel event count current source escaped its rule:", key, err)
		}
		after, count, err := channeleventcount.Transform(pin.Before)
		if err != nil || after != pin.After || count != pin.Calls {
			t.Fatal("channel event count transform changed its reviewed source:", key, err)
		}
		calls += count
		channelEventCountHostiles(t, pin)
	}
	if calls != 15 || len(seen) != 11 {
		t.Fatalf("channel event count inventory=%d/%d want15/11", calls, len(seen))
	}
}

func channelEventCountHostiles(t *testing.T, pin channeleventcount.Pin) {
	t.Helper()
	finite := recipe{File: pin.File, Function: pin.Function, Before: pin.Before, After: pin.After}
	pins := channelEventCountPins(t)
	for _, cut := range []string{"channelAnchorStorageObservation", "CountRunNamedEventStorage", "seed.RunID", "fork.ForkRunID", "runID", "mailbox.card_decided", "mailbox.card_superseded", "decisions", "supersessionCount", "events", " != ", " == ", "t.Fatal", "parentBefore", "time.After", "time.Sleep", "readParent", "kill()", "h.stop(t)", "context.Background()"} {
		if !strings.Contains(pin.After, cut) {
			continue
		}
		mutant := strings.Replace(pin.After, cut, "foreignCountCut", 1)
		if _, _, err := rewriteFunction(pin.File, []byte("package proof\n"+mutant), finite); err == nil || channeleventcount.Previous(pins, pin.File, pin.Function, mutant) != mutant {
			t.Fatal("foreign context/identity/assertion/lifetime received inverse:", pin.Function, cut)
		}
	}
	if channeleventcount.Previous(pins, "foreign.go", pin.Function, pin.After) != pin.After || channeleventcount.Previous(pins, pin.File, pin.Function+"Foreign", pin.After) != pin.After {
		t.Fatal("channel event count inverse accepted a foreign complete key")
	}
}

func TestFamily145ChannelEventCountTransformRefusesForeignBindingAndKeepsOuterDestination(t *testing.T) {
	const statement = "if err := db.QueryRow(`SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name='mailbox.card_decided'`, runID).Scan(&count); err != nil || count != 2 { t.Fatal(err) }"
	const prefix = "func TestChannelLearnedObjectPublicJourney() { var count int; "
	before := prefix + statement + "; consume(count) }"
	after, calls, err := channeleventcount.Transform(before)
	if err != nil || calls != 1 || !strings.Contains(after, "count, err = ") || strings.Contains(after, "count, err := ") || !strings.Contains(after, "consume(count)") {
		t.Fatal("count result no longer updates its original destination:", after, err)
	}
	for _, mutant := range []string{
		strings.Replace(before, "db.QueryRow", "foreign.QueryRow", 1),
		strings.Replace(before, "&count", "&row.Count", 1),
		strings.Replace(before, "runID).Scan", "runID, extra).Scan", 1),
		strings.Replace(before, "err :=", "_ :=", 1),
		strings.Replace(before, "TestChannelLearnedObjectPublicJourney", "foreignConsumer", 1),
	} {
		if _, _, err := channeleventcount.Transform(mutant); err == nil {
			t.Fatal("unreviewed carrier/destination/arity/consumer accepted:", mutant)
		}
	}
}

func TestFamily145ChannelEventCountWholeOwnedInventoryHasNoOldQuery(t *testing.T) {
	root, counts := "../../..", map[string]int{}
	err := checkoutsource.WalkDir(root, filepath.Join(root, "internal/serveapp"), func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasPrefix(entry.Name(), "channel") || !strings.HasSuffix(path, ".go") {
			return err
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.AllErrors)
		if err != nil {
			return err
		}
		return channelEventCountInventoryFile(path, file, counts)
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, pin := range channelEventCountPins(t) {
		if counts[pin.Function] != pin.Calls {
			t.Fatalf("channel event count calls %s=%d want%d", pin.Function, counts[pin.Function], pin.Calls)
		}
		delete(counts, pin.Function)
	}
	if len(counts) != 0 {
		t.Fatal("unreviewed channel event-count callers:", counts)
	}
}

func channelEventCountInventoryFile(path string, file *ast.File, counts map[string]int) error {
	for _, declaration := range file.Decls {
		fn, ok := declaration.(*ast.FuncDecl)
		if !ok {
			continue
		}
		_, oldQueries, err := channeleventcount.Transform(requestkey.Node(fn))
		if err != nil || oldQueries != 0 {
			return fmt.Errorf("channel event count raw query survived: %s/%s: %v", path, fn.Name.Name, err)
		}
		if channeleventcount.Consumers[fn.Name.Name] {
			counts[fn.Name.Name] += channelEventCountCalls(fn)
		}
	}
	return nil
}

func channelEventCountCalls(fn *ast.FuncDecl) int {
	count := 0
	ast.Inspect(fn, func(node ast.Node) bool {
		if call, ok := node.(*ast.CallExpr); ok {
			if method, ok := call.Fun.(*ast.SelectorExpr); ok && method.Sel.Name == "CountRunNamedEventStorage" {
				count++
			}
		}
		return true
	})
	return count
}
