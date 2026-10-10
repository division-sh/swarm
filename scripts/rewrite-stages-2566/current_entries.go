package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strings"
)

// Current coordinates may move; the reviewed historical decision never does.
type currentEntrySelector struct {
	File        string `json:"file"`
	Function    string `json:"function"`
	Literal     int    `json:"literal"`
	Materialize bool   `json:"materialize,omitempty"`
}

const deliveryEntryOwner = "internal/runtime/testfixtures/canonicalrouting/pipeline_delivery_authority.go"

func validateCurrentEntrySelector(entry entryGolden) error {
	current := entry.Current
	if current == nil {
		return nil
	}
	if entry.Function == "" || len(entry.EmbeddedPath) != 0 || !filepath.IsLocal(current.File) ||
		filepath.ToSlash(filepath.Clean(current.File)) != current.File || !strings.HasSuffix(current.File, ".go") ||
		current.Function == "" || current.Literal <= 0 {
		return fmt.Errorf("invalid current selector for %s/%s", entry.File, entry.Flow)
	}
	constructor := current.File == deliveryEntryOwner &&
		(current.Function == "CopyPipelineDeliveryAuthority" || current.Function == "CopyPipelineDeliveryRetry")
	if current.Materialize != constructor || (constructor && current.Literal != 2) {
		return fmt.Errorf("unsupported materialized entry source: %+v", current)
	}
	return nil
}

func retainCurrentEntrySelectors(reviewed, retained []entryGolden) ([]entryGolden, error) {
	indices := map[string]int{}
	for index, entry := range reviewed {
		key := entry.File + "/" + entry.Flow
		if _, duplicate := indices[key]; duplicate {
			return nil, fmt.Errorf("ambiguous reviewed decision %s", key)
		}
		indices[key] = index
	}
	seen := map[string]bool{}
	for _, entry := range retained {
		if entry.Current == nil {
			continue
		}
		key := entry.File + "/" + entry.Flow
		index, found := indices[key]
		if !found || seen[key] {
			return nil, fmt.Errorf("unmatched or duplicate current selector %s", key)
		}
		seen[key] = true
		current := entry.Current
		entry.Current = nil
		if !reflect.DeepEqual(entry, reviewed[index]) {
			return nil, fmt.Errorf("current selector changed historical decision %s", key)
		}
		entry.Current = current
		if err := validateCurrentEntrySelector(entry); err != nil {
			return nil, err
		}
		reviewed[index].Current = current
	}
	return reviewed, nil
}

func readEntryLiteral(root, file, function string, ordinal int) (goLiteralSite, error) {
	body, err := os.ReadFile(filepath.Join(root, file))
	if err != nil {
		return goLiteralSite{}, err
	}
	if function != "global" {
		parsed, err := parser.ParseFile(token.NewFileSet(), file, body, 0)
		if err != nil {
			return goLiteralSite{}, err
		}
		matches := 0
		for _, declaration := range parsed.Decls {
			if named, ok := declaration.(*ast.FuncDecl); ok && named.Name.Name == function {
				matches++
			}
		}
		if matches != 1 {
			return goLiteralSite{}, fmt.Errorf("missing or ambiguous entry source function %s:%s (%d declarations)", file, function, matches)
		}
	}
	sites, err := goLiteralSites(file, body)
	if err != nil {
		return goLiteralSite{}, err
	}
	var found []goLiteralSite
	for _, site := range sites {
		if site.Function == function && site.Ordinal == ordinal {
			found = append(found, site)
		}
	}
	if len(found) != 1 {
		return goLiteralSite{}, fmt.Errorf("reviewed embedded source disappeared or is ambiguous: %s:%s/literal-%d (%d matches)", file, function, ordinal, len(found))
	}
	return found[0], nil
}
