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
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/checkoutsource"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/channelsettlement"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/globalname"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runphysical"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/servedsettlement"
)

//go:embed family145-global-event-name-preimages.json
var globalEventNameBytes []byte

func TestFamily145GlobalEventNameWholeRulePreservesEveryNameScopeAssertionAndCut(t *testing.T) {
	pins, err := globalname.Decode(globalEventNameBytes)
	if err != nil {
		t.Fatal(err)
	}
	reads := 0
	for index, pin := range pins {
		old, err := exec.Command("git", "show", globalname.Predecessor+":"+pin.File).Output()
		if err != nil || requestKeyDeclaration(t, old, pin.Function) != pin.Before || fmt.Sprintf("%x", sha256.Sum256([]byte(pin.Before))) != pin.SHA256 {
			t.Fatal("global name signed complete predecessor changed:", pin.Function, err)
		}
		current, err := os.ReadFile(filepath.Join("../../..", pin.File))
		if err != nil || globalNameEarlierCurrentDeclaration(t, pin.File, pin.Function, requestKeyDeclaration(t, current, pin.Function)) != pin.After {
			t.Fatal("global name current declaration changed:", pin.Function, err)
		}
		after, err := runphysical.Transform(globalname.Rules[index], pin.Before)
		if err != nil || after != pin.After || pin.Calls != globalname.Rules[index].Calls {
			t.Fatal("global name escaped its finite read/role edits:", pin.Function, err)
		}
		finite := recipe{File: pin.File, Function: pin.Function, Before: pin.Before, After: pin.After}
		out, changed, err := rewriteFunction(pin.File, []byte("package proof\n"+pin.Before), finite)
		if err != nil || !changed {
			t.Fatal("global name finite rewrite refused:", pin.Function, err)
		}
		if _, changed, err := rewriteFunction(pin.File, out, finite); err != nil || changed {
			t.Fatal("global name finite rewrite is not inert:", pin.Function, err)
		}
		for _, cut := range []string{"CountEventNameStorage", "ReceiverStateReader", "context.Background()", "eventName", "platform.boot", "widget.scored", "inbound.telegram", "mailbox.card_decided", " != ", "err != nil", "t.Fatal", "h.stop", "h.start", "cancel()", "ReadApplicationSnapshot", "observationLocation", "Count(*)", "code != 0"} {
			if !strings.Contains(pin.After, cut) {
				continue
			}
			mutant := strings.Replace(pin.After, cut, "unreviewedGlobalNameCut", 1)
			if _, _, err := rewriteFunction(pin.File, []byte("package proof\n"+mutant), finite); err == nil {
				t.Fatal("changed scope/name/context/error/assertion/role/lifetime accepted:", pin.Function, cut)
			}
		}
		reads += pin.Calls
	}
	if reads != 8 {
		t.Fatal("global name physical read inventory changed:", reads)
	}
}

func globalNameEarlierCurrentDeclaration(t *testing.T, file, function, current string) string {
	t.Helper()
	pins, err := servedsettlement.Decode(servedSettlementBytes)
	if err != nil {
		t.Fatal(err)
	}
	for _, pin := range pins {
		if pin.File == file && pin.Function == function {
			actual := servedSettlementEarlierDeclaration(t, file, function, current)
			if actual != pin.After {
				t.Fatal("global-name comparison lost the exact later settlement stage:", function)
			}
			return pin.Before
		}
	}
	previous, err := channelsettlement.EarlierSource("../../..", file, function, current)
	if err != nil {
		t.Fatal(err)
	}
	return previous
}

func TestFamily145GlobalEventNameReusesOriginalNativeOwnerAndAddsOnlyClosedObservationRole(t *testing.T) {
	for _, owner := range [][2]string{
		{"internal/store/storetest/api_event_storage.go", "CountEventNameStorage"},
		{"internal/store/internal/runtimepersistence/test_event_support.go", "CountEventNameStorageForTest"},
		{"internal/store/internal/backend/eventrecord/run_name_cardinality.go", "CountGlobalNamedEventStorageTx"},
		{"internal/serveapp/channel_anchor_storage_owner_test.go", "channelAnchorStorageObservation"},
		{"internal/serveapp/channel_anchor_storage_owner_test.go", "openChannelAnchorStorageAtLocation"},
		{"internal/store/construction/read_only_inspection.go", "OpenReadOnlyInspection"},
		{"internal/store/storetest/channel_observation.go", "OpenChannelObservation"},
	} {
		old, err := exec.Command("git", "show", globalname.Predecessor+":"+owner[0]).Output()
		current, readErr := os.ReadFile(filepath.Join("../../..", owner[0]))
		if err != nil || readErr != nil || sourceBlobNamedDeclarations(t, old, owner[1]) != sourceBlobNamedDeclarations(t, current, owner[1]) {
			t.Fatal("global name native owner or inspection lifetime changed:", owner, err, readErr)
		}
	}
	path := "internal/store/storetest/channel_observation.go"
	old, err := exec.Command("git", "show", globalname.Predecessor+":"+path).Output()
	current, readErr := os.ReadFile(filepath.Join("../../..", path))
	if err != nil || readErr != nil {
		t.Fatal(err, readErr)
	}
	current, err = channelsettlement.EarlierObservationSource("../../..", current)
	if err != nil {
		t.Fatal(err)
	}
	globalNameObservationDelta(t, old, current)
	bridge, err := os.ReadFile("../../../internal/serveapp/global_event_name_observation_test.go")
	want := `func readServedGlobalEventNameCount(ctx context.Context,reader servedEventCountReader,eventName string)(int,error){if reader==nil{return 0,fmt.Errorf("global event-name observation requires the original selected reader")};var count int;err:=reader.InspectSnapshot(ctx,func(ctx context.Context)error{var err error;count,err=storetest.CountEventNameStorage(ctx,reader,eventName);return err});if err!=nil{return 0,err};return count,nil}`
	if err != nil || requestKeyDeclaration(t, bridge, "readServedGlobalEventNameCount") != requestKeyDeclaration(t, []byte("package proof\n"+want), "readServedGlobalEventNameCount") {
		t.Fatal("global name live bridge changed its original owner/snapshot/context/error cut:", err)
	}
}

func globalNameObservationDelta(t *testing.T, old, current []byte) {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "observation.go", current, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	declarations := []ast.Decl{}
	fields, methods := 0, 0
	for _, declaration := range file.Decls {
		if fn, ok := declaration.(*ast.FuncDecl); ok && fn.Name.Name == "CountEventNameStorage" {
			methods++
			want := `func(o channelObservation)CountEventNameStorage(ctx context.Context,eventName string)(int,error){var count int;err:=o.selected.InspectSnapshot(ctx,func(ctx context.Context)error{var err error;count,err=private.CountEventNameStorageForTest(ctx,o.selected,eventName);return err});if err!=nil{return 0,err};return count,nil}`
			parsed, err := parser.ParseFile(token.NewFileSet(), "bridge.go", "package proof\n"+want, parser.AllErrors)
			if err != nil || family145NodeString(fn) != family145NodeString(parsed.Decls[0]) {
				t.Fatal("channel global name method changed its native owner/context/snapshot/error cut:", err)
			}
			continue
		}
		fields += removeGlobalNameObservationField(declaration)
		declarations = append(declarations, declaration)
	}
	file.Decls = declarations
	before, err := parser.ParseFile(token.NewFileSet(), "old.go", old, parser.AllErrors)
	if err != nil || fields != 1 || methods != 1 || family145NodeString(file) != family145NodeString(before) {
		t.Fatal("global count addition changed other observer fields/methods/cleanup or added another role:", fields, methods, err)
	}
}

func removeGlobalNameObservationField(declaration ast.Decl) int {
	group, ok := declaration.(*ast.GenDecl)
	if !ok {
		return 0
	}
	count := 0
	for _, spec := range group.Specs {
		typed, ok := spec.(*ast.TypeSpec)
		if !ok || typed.Name.Name != "ChannelAnchorObservation" {
			continue
		}
		face, ok := typed.Type.(*ast.InterfaceType)
		if !ok {
			return -1
		}
		var fields []*ast.Field
		for _, field := range face.Methods.List {
			if len(field.Names) == 1 && field.Names[0].Name == "CountEventNameStorage" {
				count++
				if family145NodeString(field.Type) != "func(context.Context, string) (int, error)" {
					return -1
				}
				continue
			}
			fields = append(fields, field)
		}
		face.Methods.List = fields
	}
	return count
}

func TestFamily145GlobalEventNameStandaloneSQLInventoryIsClosed(t *testing.T) {
	err := checkoutsource.WalkDir("../../..", "../../../internal/serveapp", func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") {
			return err
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.AllErrors)
		if err != nil {
			return err
		}
		if globalNameDirectSQL(file) {
			return fmt.Errorf("%s: standalone global event-name count bypasses the original native owner", path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, sql := range []string{"SELECT COUNT(*) FROM events WHERE event_name = ?", "SELECT COUNT(*) FROM events WHERE event_name=$1", "SELECT COUNT(*) FROM events WHERE event_name='mailbox.card_decided'", "SELECT COUNT(*) FROM events WHERE event_name='platform.boot'", "SELECT COUNT(*) FROM events WHERE event_name='before_and_after'", "SELECT COUNT(*) FROM events WHERE event_name='orphan.event'"} {
		file, err := parser.ParseFile(token.NewFileSet(), "escape.go", "package proof\nvar escape=func(){db.QueryRow("+strconv.Quote(sql)+",name)}", parser.AllErrors)
		if err != nil || !globalNameDirectSQL(file) {
			t.Fatal("global event-name initializer SQL escape admitted:", sql, err)
		}
	}
}

var globalNameSQLShape = regexp.MustCompile(`(?i)^\s*select\s+count\(\*\)\s+from\s+events\s+where\s+event_name\s*=\s*(?:\?|\$1|'(?:[^']|'')*')\s*$`)

func globalNameDirectSQL(file *ast.File) bool {
	found := false
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		method, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || method.Sel.Name != "QueryRow" && method.Sel.Name != "QueryRowContext" {
			return true
		}
		for _, arg := range call.Args {
			literal, ok := arg.(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING {
				continue
			}
			query, err := strconv.Unquote(literal.Value)
			if err != nil {
				found = true
				continue
			}
			if globalNameSQLShape.MatchString(query) {
				found = true
			}
		}
		return true
	})
	return found
}
