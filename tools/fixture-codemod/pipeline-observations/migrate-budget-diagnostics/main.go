package main

import (
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"strconv"
	"strings"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runphysical"
)

const predecessor = "a552cdb3a2953add81cea117bafcadddd9803c11"
const sourcePath = "internal/serveapp/channel_delivery_budget_supported_surface_test.go"
const manifestPath = "tools/fixture-codemod/pipeline-observations/family145-budget-diagnostics-preimages.json"

type projection struct {
	Name, Package, Import, File, Query string
	ExistingScanner                    bool
}

var projections = []projection{
	{"Spend", "budgetspendstore", "internal/store/internal/budgetspend", "internal/store/internal/budgetspend/channel_budget_diagnostics.go", "", false},
	{"Effects", "effectpersistence", "internal/store/internal/backend/effectpersistence", "internal/store/internal/backend/effectpersistence/channel_budget_diagnostics.go", "", true},
	{"Agents", "agentpersistence", "internal/store/internal/backend/agentpersistence", "internal/store/internal/backend/agentpersistence/channel_budget_diagnostics.go", "", false},
	{"Deliveries", "delivery", "internal/store/internal/backend/delivery", "internal/store/internal/backend/delivery/read_projections.go", "", false},
	{"BudgetEvents", "eventrecord", "internal/store/internal/backend/eventrecord", "internal/store/internal/backend/eventrecord/channel_budget_diagnostics.go", "", false},
	{"Notices", "mailboxpersistence", "internal/store/internal/mailboxpersistence", "internal/store/internal/mailboxpersistence/channel_budget_diagnostics.go", "", false},
}

func main() {
	write := flag.Bool("write", false, "write the finite owner and caller transformation")
	flag.Parse()
	must(inventory("."))
	before := signed(sourcePath)
	logger := declaration(before, "logBudgetNoticeFailure")
	queries := sqlLiterals(logger)
	if len(queries) != len(projections) {
		panic("signed six-domain query inventory changed")
	}
	for i := range projections {
		projections[i].Query = queries[i]
	}
	scanner := declaration(signed("internal/serveapp/channel_delivery_settlement_diagnostic_test.go"), "readChannelDeliveryDiagnosticRows")
	start := strings.Index(scanner, "defer rows.Close()")
	if start < 0 {
		panic("original NULL-preserving scanner disappeared")
	}
	scanner = "func scanChannelBudgetDiagnosticRows(rows *sql.Rows)([]map[string]string,error){\n" + scanner[start:]
	current, err := os.ReadFile(sourcePath)
	must(err)
	updated, pins := transformSource(before, current)
	if *write {
		generateOwners(before, scanner)
		generatePorts()
		must(os.WriteFile(sourcePath, updated, 0644))
		raw, err := json.MarshalIndent(pins, "", "  ")
		must(err)
		must(os.WriteFile(manifestPath, append(raw, '\n'), 0644))
	}
	fmt.Printf("budget diagnostic finite transform: %d declarations, six domain diagnostics, two positive reads\n", len(pins))
}

func signed(path string) []byte {
	raw, err := exec.Command("git", "show", predecessor+":"+path).Output()
	must(err)
	return raw
}

func declaration(raw []byte, name string) string {
	file, err := parser.ParseFile(token.NewFileSet(), "source.go", raw, parser.AllErrors)
	must(err)
	var result string
	for _, decl := range file.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Recv == nil && fn.Name.Name == name {
			if result != "" {
				panic("duplicate finite declaration")
			}
			result = requestkey.Node(fn)
		}
	}
	if result == "" {
		panic("missing finite declaration: " + name)
	}
	return result
}

func sqlLiterals(source string) []string {
	file, err := parser.ParseFile(token.NewFileSet(), "query.go", "package proof\n"+source, parser.AllErrors)
	must(err)
	var result []string
	ast.Inspect(file, func(node ast.Node) bool {
		literal, ok := node.(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING {
			return true
		}
		value, err := strconv.Unquote(literal.Value)
		must(err)
		if strings.HasPrefix(value, "SELECT ") {
			result = append(result, value)
		}
		return true
	})
	return result
}

func pin(name, before, after string) runphysical.Pin {
	return runphysical.Pin{File: sourcePath, Function: name, Before: before, After: after, SHA256: fmt.Sprintf("%x", sha256.Sum256([]byte(before))), Calls: 1}
}

func writeGo(path, source string) {
	raw, err := format.Source([]byte(source))
	must(err)
	must(os.WriteFile(path, raw, 0644))
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
