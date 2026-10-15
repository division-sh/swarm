package main

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
)

const deliveryRecoveryPredecessor = "3bb704336cdbce8e70a17e0190de6b96b453707a"
const deliveryConformanceFile = "internal/runtime/conformance/delivery_lifecycle_conformance_test.go"

type deliverySchemaRewrite struct {
	constant string
	fields   []string
}

var deliverySchemaRewrites = map[string]deliverySchemaRewrite{
	"event/run mismatch":                                     {"DeliveryConstraintEventRunMismatch", []string{"OtherRunID", "DeliveryID"}},
	"in-progress without open attempt":                       {"DeliveryConstraintMissingOpenAttempt", []string{"At", "DeliveryID"}},
	"nonexistent exact agent session":                        {"DeliveryConstraintMissingSession", []string{"SessionID", "RunID", "AgentID", "DeliveryID", "Version"}},
	"session owned by another delivery run and agent":        {"DeliveryConstraintForeignSession", []string{"SessionID", "DeliveryID", "RunID", "AgentID", "Version"}},
	"unreferenced second open attempt":                       {"DeliveryConstraintSecondOpenAttempt", []string{"DeliveryID", "Version", "Token", "At", "ExpiresAt"}},
	"settled attempt without required evidence":              {"DeliveryConstraintMissingSettlementEvidence", []string{"DeliveryID", "At"}},
	"settled attempt without outcome":                        {"DeliveryConstraintMissingOutcome", []string{"DeliveryID", "At"}},
	"settled attempt without completion time":                {"DeliveryConstraintMissingCompletionTime", []string{"DeliveryID", "At"}},
	"expired attempt cannot carry settlement duration":       {"DeliveryConstraintExpiredDuration", []string{"DeliveryID", "At"}},
	"open attempt cannot own a linked dead letter":           {"DeliveryConstraintOpenDeadLetter", []string{"RecordID", "EventID", "DeliveryID", "Version"}},
	"partial-null settlement reference cannot bypass the FK": {"DeliveryConstraintPartialSettlementReference", []string{"RecordID", "EventID", "DeliveryID"}},
	"contradictory open settlement tag":                      {"DeliveryConstraintContradictoryClosure", []string{"DeliveryID", "Version"}},
	"dead-letter without typed failure":                      {"DeliveryConstraintMissingTypedFailure", []string{"DeliveryID"}},
	"expired attempt cannot own a linked dead letter":        {"DeliveryConstraintExpiredDeadLetter", []string{"RecordID", "DeliveryID"}},
}

type deliverySchemaSource struct {
	name, postgres, sqlite string
	args                   []string
}

func deliverySchemaCalls(raw []byte) (*token.FileSet, *ast.File, []*ast.CallExpr) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, deliveryConformanceFile, raw, parser.ParseComments)
	must(err)
	var calls []*ast.CallExpr
	ast.Inspect(file, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		name, ok := call.Fun.(*ast.Ident)
		if ok && name.Name == "assertDeliverySQLRejected" {
			calls = append(calls, call)
		}
		return true
	})
	return fset, file, calls
}

func deliverySchemaOriginals() map[string]deliverySchemaSource {
	raw, err := exec.Command("git", "show", deliveryRecoveryPredecessor+":"+deliveryConformanceFile).Output()
	must(err)
	_, _, calls := deliverySchemaCalls(raw)
	sources := map[string]deliverySchemaSource{}
	for _, call := range calls {
		if len(call.Args) != 6 {
			panic("schema probe signature changed")
		}
		unquote := func(i int) string {
			literal, ok := call.Args[i].(*ast.BasicLit)
			if !ok {
				panic("schema probe is not a literal")
			}
			out, err := strconv.Unquote(literal.Value)
			must(err)
			return out
		}
		name := unquote(2)
		rule, ok := deliverySchemaRewrites[name]
		if !ok {
			panic("unreviewed schema probe: " + name)
		}
		args, ok := call.Args[5].(*ast.CompositeLit)
		if !ok || len(args.Elts) != len(rule.fields) {
			panic("schema probe argument count changed")
		}
		row := deliverySchemaSource{name: name, postgres: unquote(3), sqlite: unquote(4)}
		for _, arg := range args.Elts {
			row.args = append(row.args, node(arg))
		}
		if _, exists := sources[name]; exists {
			panic("duplicate schema probe")
		}
		sources[name] = row
	}
	if len(sources) != 14 {
		panic("incomplete schema probe inventory")
	}
	return sources
}

func rewriteDeliveryRecoverySchema() {
	sources := deliverySchemaOriginals()
	raw, err := os.ReadFile(deliveryConformanceFile)
	must(err)
	fset, _, calls := deliverySchemaCalls(raw)
	if len(calls) != 14 {
		panic("current schema probe inventory changed")
	}
	for i := len(calls) - 1; i >= 0; i-- {
		call := calls[i]
		if len(call.Args) != 6 {
			panic("current schema probe signature changed")
		}
		literal, ok := call.Args[2].(*ast.BasicLit)
		if !ok {
			panic("current probe label changed")
		}
		name, err := strconv.Unquote(literal.Value)
		must(err)
		row, ok := sources[name]
		if !ok {
			panic("foreign current schema probe")
		}
		rule := deliverySchemaRewrites[name]
		if !deliverySchemaCurrentMatches(call, row) {
			panic("current probe differs from exact signed predecessor: " + name)
		}
		var fields []string
		for index, field := range rule.fields {
			fields = append(fields, field+":"+row.args[index])
		}
		replacement := fmt.Sprintf("assertDeliverySQLRejected(t, backend, storetest.%s, storetest.DeliveryConstraintInput{%s})", rule.constant, strings.Join(fields, ","))
		start, end := fset.Position(call.Pos()).Offset, fset.Position(call.End()).Offset
		raw = append(append(append([]byte{}, raw[:start]...), replacement...), raw[end:]...)
	}
	raw, err = format.Source(raw)
	must(err)
	must(os.WriteFile(deliveryConformanceFile, raw, 0644))
	appendDeliverySchemaCatalog(sources)
}

func deliverySchemaCurrentMatches(call *ast.CallExpr, row deliverySchemaSource) bool {
	if node(call.Args[0]) != "t" || node(call.Args[1]) != "backend" {
		return false
	}
	for i, want := range []string{row.postgres, row.sqlite} {
		literal, ok := call.Args[i+3].(*ast.BasicLit)
		if !ok {
			return false
		}
		got, err := strconv.Unquote(literal.Value)
		if err != nil || got != want {
			return false
		}
	}
	args, ok := call.Args[5].(*ast.CompositeLit)
	if !ok || node(args.Type) != "[]any" || len(args.Elts) != len(row.args) {
		return false
	}
	for i, arg := range args.Elts {
		if node(arg) != row.args[i] {
			return false
		}
	}
	return true
}

func appendDeliverySchemaCatalog(sources map[string]deliverySchemaSource) {
	var names []string
	for name := range sources {
		names = append(names, name)
	}
	sort.Strings(names)
	var out bytes.Buffer
	fmt.Fprintln(&out, "\n// Exact finite constraint probes captured from the signed conformance predecessor.")
	fmt.Fprintln(&out, "const (")
	for _, name := range names {
		fmt.Fprintf(&out, "%s DeliveryConstraintCase = %q\n", deliverySchemaRewrites[name].constant, name)
	}
	fmt.Fprintln(&out, ")\nvar deliveryConstraintQueries = map[DeliveryConstraintCase]struct{postgres,sqlite string}{")
	for _, name := range names {
		row := sources[name]
		fmt.Fprintf(&out, "%s:{%q,%q},\n", deliverySchemaRewrites[name].constant, deliverySchemaPostgresSQL(row), row.sqlite)
	}
	fmt.Fprintln(&out, "}\nfunc deliveryConstraintArguments(postgres bool, input DeliveryConstraintInput)([]any,error){switch input.Case{")
	for _, name := range names {
		rule := deliverySchemaRewrites[name]
		var args []string
		for _, field := range rule.fields {
			args = append(args, "input."+field)
		}
		fmt.Fprintf(&out, "case %s:\n", rule.constant)
		if name == "in-progress without open attempt" {
			fmt.Fprintln(&out, "if !postgres{return []any{input.At,input.At,input.DeliveryID},nil}")
		}
		fmt.Fprintf(&out, "return []any{%s},nil\n", strings.Join(args, ","))
	}
	fmt.Fprintln(&out, "};return nil,fmt.Errorf(\"unknown delivery constraint probe %q\",input.Case)}")
	const path = "internal/store/internal/backend/delivery/adapter.go"
	raw, err := os.ReadFile(path)
	must(err)
	if bytes.Contains(raw, []byte("var deliveryConstraintQueries")) {
		panic("schema catalog already exists; refusing duplicate rewrite")
	}
	raw, err = format.Source(append(raw, out.Bytes()...))
	must(err)
	must(os.WriteFile(path, raw, 0644))
}

func deliverySchemaPostgresSQL(row deliverySchemaSource) string {
	switch row.name {
	case "settled attempt without required evidence", "settled attempt without outcome", "settled attempt without completion time", "expired attempt cannot carry settlement duration":
		const old = "$2 + INTERVAL '1 minute'"
		if strings.Count(row.postgres, old) != 1 {
			panic("exact PostgreSQL constraint clock fragment changed")
		}
		return strings.Replace(row.postgres, old, "$2::timestamptz + INTERVAL '1 minute'", 1)
	default:
		return row.postgres
	}
}
