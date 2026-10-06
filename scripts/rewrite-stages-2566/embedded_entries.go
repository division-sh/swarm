package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os/exec"
	"strconv"
	"strings"
)

// Preparation reads the independently reviewed pre-rewrite source, not the
// candidate's entry facts. Permanent tests need only the resulting selectors.
const reviewedEntrySourceRevision = "83482f4ad0df7536975f427a25d60f2a997e4e23"

type goLiteralSite struct {
	Position string
	Function string
	Ordinal  int
	Body     string
}

func goLiteralSites(name string, body []byte) ([]goLiteralSite, error) {
	positions := token.NewFileSet()
	file, err := parser.ParseFile(positions, name, body, 0)
	if err != nil {
		return nil, err
	}
	counts := map[string]int{}
	var sites []goLiteralSite
	for _, declaration := range file.Decls {
		function := "global"
		if named, ok := declaration.(*ast.FuncDecl); ok {
			function = named.Name.Name
		}
		ast.Inspect(declaration, func(node ast.Node) bool {
			literal, ok := node.(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING {
				return true
			}
			value, unquoteErr := strconv.Unquote(literal.Value)
			if unquoteErr != nil {
				return true
			}
			counts[function]++
			sites = append(sites, goLiteralSite{positions.Position(literal.Pos()).String(), function, counts[function], value})
			return true
		})
	}
	return sites, nil
}

func embeddedEntryGoldens(root string, change change) ([]entryGolden, error) {
	if change.File == "platform-spec.yaml" {
		var entries []entryGolden
		for _, proof := range change.Equivalence {
			if proof.Site != `$["contract_formats"]["hello_world_example"]["schema_yaml"]` {
				return nil, fmt.Errorf("unclassified embedded spec source: %s", proof.Site)
			}
			entries = append(entries, entryGolden{File: change.File, Source: change.File, Flow: "hello_world_example", Entry: proof.Entry, Order: proof.AfterOrder, Finals: proof.Finals, EmbeddedPath: []string{"contract_formats", "hello_world_example", "schema_yaml"}})
		}
		return entries, nil
	}
	if len(change.Equivalence) == 0 || !strings.HasSuffix(change.File, ".go") {
		return nil, nil
	}
	command := exec.Command("git", "show", reviewedEntrySourceRevision+":"+change.File)
	command.Dir = root
	body, err := command.Output()
	if err != nil {
		return nil, err
	}
	sites, err := goLiteralSites(change.File, body)
	if err != nil {
		return nil, err
	}
	byPosition := map[string]goLiteralSite{}
	for _, site := range sites {
		byPosition[site.Position] = site
	}
	var entries []entryGolden
	for _, proof := range change.Equivalence {
		site, ok := byPosition[proof.Site]
		if !ok {
			return nil, fmt.Errorf("%s: reviewed source selector missing", proof.Site)
		}
		entries = append(entries, entryGolden{File: change.File, Source: change.File, Flow: fmt.Sprintf("%s/literal-%d", site.Function, site.Ordinal), Entry: proof.Entry, Order: proof.AfterOrder, Finals: proof.Finals, Function: site.Function, Literal: site.Ordinal})
	}
	return entries, nil
}
