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
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/channelintent"
)

//go:embed family145-channel-intent-preimages.json
var family145ChannelIntentBytes []byte

func channelIntentPins(t *testing.T) []channelintent.Pin {
	t.Helper()
	pins, err := channelintent.Decode(family145ChannelIntentBytes)
	if err != nil {
		t.Fatal(err)
	}
	return pins
}

func family145ChannelIntentCallerInverse(function, current string) string {
	current = family145ChannelAnchorCallerInverse(function, current)
	pins, err := channelintent.Decode(family145ChannelIntentBytes)
	if err != nil {
		panic(err)
	}
	for _, pin := range pins {
		if pin.Function == function {
			return channelintent.Previous(pins, pin.File, function, current)
		}
	}
	return current
}

func family145CheckChannelIntentWholeRule(t *testing.T, file, function, current string) bool {
	t.Helper()
	current = family145ChannelAnchorPrevious(t, file, function, current)
	for _, pin := range channelIntentPins(t) {
		if pin.File == file && pin.Function == function {
			if current != pin.After {
				t.Fatal("intent owner/capture/lifetime escaped full source rule:", file, function)
			}
			return true
		}
	}
	return false
}

func TestFamily145ChannelIntentOriginal34CallsAndCompleteSiblingRepairsAreSourcePinned(t *testing.T) {
	calls, original, repaired, retired, bridges := 0, 0, 0, 0, 0
	seen := map[string]bool{}
	for _, pin := range channelIntentPins(t) {
		key := pin.File + "/" + pin.Function
		if seen[key] || fmt.Sprintf("%x", sha256.Sum256([]byte(pin.Before))) != pin.SHA256 {
			t.Fatal("duplicate/changed intent predecessor:", key)
		}
		seen[key] = true
		raw, err := os.ReadFile(filepath.Join("../../..", pin.File))
		if err != nil {
			t.Fatal(err)
		}
		if family145ChannelAnchorPrevious(t, pin.File, pin.Function, servedSetupEarlierSource(t, pin.File, pin.Function, intentDeclaration(t, raw, pin.Function))) != pin.After {
			t.Fatal("intent current body escaped finite rule:", key)
		}
		verifyIntentPredecessor(t, pin)
		switch pin.Class {
		case "original-rule":
			got, count, err := channelintent.Transform(pin.Before)
			if err != nil || got != pin.After || count != pin.Calls {
				t.Fatal("intent rule changed assertions/cuts/dispositions:", key, err)
			}
			calls += count
			original++
		case "approved-sibling-repair":
			repaired++
		case "retired-interpreter":
			retired++
		case "bounded-bridge":
			bridges++
		default:
			t.Fatal("unclassified intent source:", key)
		}
		intentHostiles(t, pin)
	}
	if calls != 34 || original != 19 || repaired != 10 || retired != 4 || bridges != 8 {
		t.Fatalf("intent inventory changed=%d/%d/%d/%d/%d", calls, original, repaired, retired, bridges)
	}
}

func verifyIntentPredecessor(t *testing.T, pin channelintent.Pin) {
	t.Helper()
	if pin.Before == "" {
		return
	}
	raw, err := exec.Command("git", "show", channelintent.Predecessor+":"+pin.File).Output()
	if err != nil || intentDeclaration(t, raw, pin.Function) != pin.Before {
		t.Fatal("intent preimage is not its signed predecessor:", pin.File, pin.Function, err)
	}
}

func intentDeclaration(t *testing.T, raw []byte, name string) string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "source.go", raw, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	for _, decl := range file.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Recv == nil && fn.Name.Name == name {
			return channelintent.Node(fn)
		}
		if group, ok := decl.(*ast.GenDecl); ok && group.Tok == token.TYPE && len(group.Specs) == 1 && "type/"+group.Specs[0].(*ast.TypeSpec).Name.Name == name {
			return channelintent.Node(group)
		}
	}
	return ""
}

func intentHostiles(t *testing.T, pin channelintent.Pin) {
	t.Helper()
	if pin.After == "" || strings.HasPrefix(pin.Function, "type/") || pin.Before == "" {
		return
	}
	pins := channelIntentPins(t)
	if channelintent.Previous(pins, pin.File, pin.Function, pin.After) != pin.Before || channelintent.Previous(pins, "foreign.go", pin.Function, pin.After) != pin.After {
		t.Fatal("intent inverse accepted foreign/missing source")
	}
	finite := recipe{File: pin.File, Function: pin.Function, Before: pin.Before, After: pin.After}
	for _, cut := range []string{"intentIdentity", "successorIntent", "intentReader", "Interface.Key()", "Provider", "deadline", "5 * time.Second", "20 * time.Millisecond", "first && second", "found", "release()", "kill()", "reflect.DeepEqual", "!= beforeACK", "selectedChannelDraftState", "childIntent", "State != \"pending\"", "Disposition != \"\"", "t.Fatal", "originalSource", "parentBefore"} {
		if !strings.Contains(pin.After, cut) {
			continue
		}
		mutant := strings.Replace(pin.After, cut, "unreviewedIntentCut", 1)
		if _, _, err := rewriteFunction(pin.File, []byte("package proof\n"+mutant), finite); err == nil || channelintent.Previous(pins, pin.File, pin.Function, mutant) != mutant {
			t.Fatal("lost intent identity/assertion/cut accepted:", pin.Function, cut)
		}
	}
}

func TestFamily145ChannelIntentSiblingDeadlinesAndExactPostingIdentityStayExplicit(t *testing.T) {
	pins := channelIntentPins(t)
	find := func(name string) string {
		for _, pin := range pins {
			if pin.Function == name {
				return pin.After
			}
		}
		t.Fatal("missing intent obligation:", name)
		return ""
	}
	stale := find("postUncertainCopyStaleControls")
	for _, cut := range []string{"first && second", "fmt.Sprint(update)", "fmt.Sprint(update+1)", "fmt.Sprint(update+3)", "fmt.Sprint(update+2)", "http.StatusOK"} {
		if !strings.Contains(stale, cut) {
			t.Fatal("stale controls lost distinct occurrence/duplicate evidence:", cut)
		}
	}
	if strings.Count(stale, "time.Now().Add(15 * time.Second)") != 2 || strings.Contains(stale, "waitObjectChannelDisposition(") {
		t.Fatal("stale action pair/cancel lost shared deadline or teaching lost separate deadline")
	}
	source := find("TestChannelSourceLifecyclePublicJourney")
	if !strings.Contains(source, "time.Now().Add(5 * time.Second)") || !strings.Contains(source, `observeChannelDisposition(t, reader, childIntent, "unsupported")`) {
		t.Fatal("selected fork refusal lost its exact occurrence or five-second gate")
	}
	ack := find("TestChannelActionAcknowledgmentAbruptProcessDeathPublicJourney")
	if strings.Count(ack, "ObserveChannelIntent(context.Background(), intentQuery)") != 2 || !strings.Contains(ack, "!found") || !strings.Contains(ack, `result.State != "pending" || result.Disposition != ""`) || !strings.Contains(ack, `result.State != "settled" || result.Disposition != "applied"`) {
		t.Fatal("ACK pending barrier/recovered settlement do not observe the same exact identity")
	}
	posting := find("postChannelSourceControlObserved")
	if !strings.Contains(posting, `fmt.Sprint(payload["update_id"])`) || !strings.Contains(posting, "return postChannelSourceUpdate(t, callback, signing, payload), query") {
		t.Fatal("HTTP status/token/card was mistaken for an admitted provider occurrence")
	}
	trigger, err := os.ReadFile("../../../packs/provider-triggers/telegram/trigger.yaml")
	if err != nil || !strings.Contains(string(trigger), "delivery_id:\n  json_path: $.update_id\n  required: true\n") {
		t.Fatal("source callback occurrence no longer follows the actual admitted trigger", err)
	}
}

func TestFamily145ChannelIntentOwnedCensusHasNoRetiredInterpreterOrRawWaiter(t *testing.T) {
	root := "../../.."
	count := 0
	err := checkoutsource.WalkDir(root, filepath.Join(root, "internal/serveapp"), func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") {
			return err
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.AllErrors)
		if err != nil {
			return err
		}
		ast.Inspect(file, func(node ast.Node) bool {
			if id, ok := node.(*ast.Ident); ok {
				switch id.Name {
				case "waitChannelRejectedCallback", "waitReceptionistActionRejection", "waitChannelTextTeaching", "waitChannelDraftCallbackRejection":
					t.Error("retired intent interpreter survived:", path, id.Name)
				}
			}
			if call, ok := node.(*ast.CallExpr); ok {
				if id, ok := call.Fun.(*ast.Ident); ok && id.Name == "waitObjectChannelDisposition" {
					count++
					if len(call.Args) < 4 || family145NodeString(call.Args[1]) == "db" || strings.Contains(family145NodeString(call.Args[2]), "operator_channel_") {
						t.Error("raw/table-dispatch intent waiter survived:", path)
					}
				}
			}
			return true
		})
		return nil
	})
	if err != nil || count != 43 {
		t.Fatalf("intent waiter census count=%d want43 (34 original + 9 sibling) err=%v", count, err)
	}
}

func TestFamily145ChannelIntentTransformRefusesUnknownCarrierTableAndVariadicCalls(t *testing.T) {
	const before = `func proof(t *testing.T) {
waitObjectChannelDisposition(t, db, "operator_channel_action_intents", "occurrence", "rejected")
}`
	after, calls, err := channelintent.Transform(before)
	if err != nil || calls != 1 || !strings.Contains(after, `objectIntentQuery(p.intentIdentity, channeldelivery.IntentAction, "occurrence"), "rejected"`) {
		t.Fatal("original identity/kind/disposition propagation failed:", after, calls, err)
	}
	for _, mutation := range []struct{ from, to string }{
		{"t, db", "t, foreign"},
		{"t, db", "other, db"},
		{`"operator_channel_action_intents"`, `"events"`},
		{`"operator_channel_action_intents"`, "table"},
		{`"rejected"`, "allowed..."},
	} {
		if _, _, err := channelintent.Transform(strings.Replace(before, mutation.from, mutation.to, 1)); err == nil {
			t.Fatal("unclassified intent call accepted:", mutation)
		}
	}
}

func TestFamily145ChannelIntentNewBridgesAreExactAndCannotLoseIdentityOrRefusal(t *testing.T) {
	pins := channelIntentPins(t)
	bridges, probes := 0, 0
	for _, pin := range pins {
		if pin.Class != "bounded-bridge" {
			continue
		}
		bridges++
		for _, cut := range []string{"Kind", "ProviderEventID", "Identity.Interface", "Activation.Interface", "Recovery.Provider", "row.Operation == nil", "provider == \"\"", "ObserveChannelIntent", "if err != nil", "channelDispositionAccepted", "if !found", "pending", "return false", "return true", "update_id", "Close()", "intentReader = nil"} {
			if !strings.Contains(pin.After, cut) {
				continue
			}
			mutant := strings.Replace(pin.After, cut, "foreignIntentBridge", 1)
			if mutant == pin.After || channelintent.Previous(pins, pin.File, pin.Function, mutant) != mutant {
				t.Fatal("unknown bridge mutation accepted:", pin.Function, cut)
			}
			// Malformed mutations are rejected by parsing; valid ones must not
			// normalize back to the exact reviewed bridge.
			normal, err := channelintent.Normalize(mutant)
			if err == nil && normal == pin.After {
				t.Fatal("bridge mutation disappeared from source evidence:", pin.Function, cut)
			}
			probes++
		}
	}
	if bridges != 8 || probes < 15 {
		t.Fatalf("bounded bridge hostile inventory=%d/%d", bridges, probes)
	}
}
