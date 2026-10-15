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
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/forknotice"
)

//go:embed family145-fork-notice-preimages.json
var family145ForkNoticeBytes []byte

func forkNoticePins(t *testing.T) []forknotice.Pin {
	t.Helper()
	pins, err := forknotice.Decode(family145ForkNoticeBytes)
	if err != nil {
		t.Fatal(err)
	}
	return pins
}

func family145ForkNoticeCallerInverse(function, current string) string {
	pins, err := forknotice.Decode(family145ForkNoticeBytes)
	if err != nil {
		panic(err)
	}
	for _, pin := range pins {
		if pin.Function == function {
			return forknotice.Previous(pins, pin.File, function, current)
		}
	}
	return current
}

func family145CheckForkNoticeWholeRule(t *testing.T, file, function, current string) bool {
	t.Helper()
	for _, pin := range forkNoticePins(t) {
		if pin.File == file && pin.Function == function {
			if current != pin.After {
				t.Fatal("fork notice escaped complete owner/caller rule:", file, function)
			}
			return true
		}
	}
	return false
}

func TestFamily145ForkNoticeWholeChainRetainsAll16CallsAndOriginalPhysicalWitness(t *testing.T) {
	calls, helpers := 0, 0
	seen := map[string]bool{}
	for _, pin := range forkNoticePins(t) {
		key := pin.File + "/" + pin.Function
		if seen[key] || fmt.Sprintf("%x", sha256.Sum256([]byte(pin.Before))) != pin.SHA256 {
			t.Fatal("changed/duplicate original notice witness:", key)
		}
		seen[key] = true
		calls += pin.Calls
		if pin.Function == "readForkReceiverNoticeDomain" {
			helpers++
			requireForkNoticePhysicalOwner(t, pin)
		} else if expected := strings.ReplaceAll(pin.Before, "readForkReceiverNoticeDomain(t, rt,", "readForkReceiverNoticeDomain(t, rt.ReceiverStateReader,"); expected != pin.After {
			t.Fatal("notice propagation changed predicate/assertion/lifetime beyond selected reader:", key)
		}
		raw, err := os.ReadFile(filepath.Join("../../..", pin.File))
		if err != nil {
			t.Fatal(err)
		}
		if servedSetupEarlierSource(t, pin.File, pin.Function, family145PublicA2CurrentDeclaration(t, raw, pin.Function)) != pin.After {
			t.Fatal("current notice body escaped its whole rule:", key)
		}
		forkNoticeHostiles(t, pin)
	}
	if calls != 16 || helpers != 1 || len(seen) != 4 {
		t.Fatalf("notice rule inventory=%d/%d/%d want16/1/4", calls, helpers, len(seen))
	}
	if _, err := forknotice.Decode(append(append([]byte(nil), family145ForkNoticeBytes...), ' ')); err == nil {
		t.Fatal("altered finite source manifest accepted")
	}
}

func requireForkNoticePhysicalOwner(t *testing.T, pin forknotice.Pin) {
	t.Helper()
	const helper = `func readForkReceiverNoticeDomain(t *testing.T, reader receiverProofStateReader, runID string) []string {
	t.Helper()
	out, err := storetest.ReadForkReceiverNoticeStorage(context.Background(), reader, runID)
	if err != nil {
		t.Fatal(err)
	}
	return out
}`
	if family145ServedDebugNormalize(t, helper) != pin.After {
		t.Fatal("notice fixture restored independent SQL/decoding/construction")
	}
	raw, err := os.ReadFile("../../../internal/store/internal/mailboxpersistence/fork_notice_observation.go")
	if err != nil {
		t.Fatal(err)
	}
	owner := family145PublicA2CurrentDeclaration(t, raw, "ReadForkReceiverNoticeStorageTx")
	for _, cut := range []string{"`SELECT m.* FROM mailbox m JOIN events e ON e.event_id=m.source_event_id WHERE e.run_id=$1`", "rows.Columns()", "rows.Scan(pointers...)", "values[i] = string(raw)", "json.Marshal(values)", "sort.Strings(out)"} {
		if !strings.Contains(pin.Before, cut) || !strings.Contains(owner, cut) {
			t.Fatal("native notice owner lost original join/full row/bytes/order:", cut)
		}
	}
	for _, cut := range []string{"out := []string{}", "rows.Err()", "return nil, err", "defer rows.Close()"} {
		if !strings.Contains(owner, cut) {
			t.Fatal("native notice owner lost empty/refusal/row cleanup contract:", cut)
		}
	}
	bridge, err := os.ReadFile("../../../internal/store/internal/runtimepersistence/test_fork_notice_observation.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, cut := range []string{"validateSelectedForkStorageIdentity", "validateChannelObservationOwner", "readServedDeliveryObservation(ctx, selected,", "mailboxpersistence.ReadForkReceiverNoticeStorageTx"} {
		if !strings.Contains(string(bridge), cut) {
			t.Fatal("notice bypassed exact selected read boundary:", cut)
		}
	}
}

func forkNoticeHostiles(t *testing.T, pin forknotice.Pin) {
	t.Helper()
	pins := forkNoticePins(t)
	if forknotice.Previous(pins, pin.File, pin.Function, pin.After) != pin.Before || forknotice.Previous(pins, "foreign.go", pin.Function, pin.After) != pin.After || forknotice.Previous(nil, pin.File, pin.Function, pin.After) != pin.After {
		t.Fatal("notice inverse lost exact source or admitted foreign/missing rule")
	}
	finite := recipe{File: pin.File, Function: pin.Function, Before: pin.Before, After: pin.After}
	for _, cut := range []string{"ReceiverStateReader", "reader", "ReadForkReceiverNoticeStorage", "runID", "reflect.DeepEqual", "seed.RunID", "siblingSeed.RunID", "frontier", "fork.ForkRunID", "release", "t.Fatal", "sourceNotices", "childNotices"} {
		if !strings.Contains(pin.After, cut) {
			continue
		}
		mutant := strings.Replace(pin.After, cut, "foreignNoticeCut", 1)
		if _, _, err := rewriteFunction(pin.File, []byte("package proof\n"+mutant), finite); err == nil || forknotice.Previous(pins, pin.File, pin.Function, mutant) != mutant {
			t.Fatal("lost notice reader/identity/refusal/replay/lifetime cut accepted:", pin.Function, cut)
		}
	}
}

func TestFamily145ForkNoticeOwnedInventoryRetiresEveryBroadCaller(t *testing.T) {
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
		allowed := map[token.Pos]bool{}
		ast.Inspect(file, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.FuncDecl:
				if n.Name.Name == "readForkReceiverNoticeDomain" {
					helpers++
					allowed[n.Name.Pos()] = true
				}
			case *ast.CallExpr:
				if id, ok := n.Fun.(*ast.Ident); ok && id.Name == "readForkReceiverNoticeDomain" {
					calls++
					allowed[id.Pos()] = true
					if len(n.Args) != 3 || n.Ellipsis.IsValid() || family145NodeString(n.Args[1]) != "rt.ReceiverStateReader" {
						t.Error("notice caller restored broad or foreign capability")
					}
				}
			}
			return true
		})
		ast.Inspect(file, func(node ast.Node) bool {
			if id, ok := node.(*ast.Ident); ok && id.Name == "readForkReceiverNoticeDomain" && !allowed[id.Pos()] {
				t.Error("notice helper escaped direct owned call inventory")
			}
			return true
		})
		return nil
	})
	if err != nil || calls != 16 || helpers != 1 {
		t.Fatalf("notice inventory calls=%d helpers=%d err=%v", calls, helpers, err)
	}
}
