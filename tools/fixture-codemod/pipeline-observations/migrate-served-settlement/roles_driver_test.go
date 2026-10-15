package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"reflect"
	"testing"

	"golang.org/x/tools/go/packages"
)

func typedRoleProof(t *testing.T, source string) *packages.Package {
	t.Helper()
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, "proof.go", source, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	info := &types.Info{Defs: map[*ast.Ident]types.Object{}, Uses: map[*ast.Ident]types.Object{}, Types: map[ast.Expr]types.TypeAndValue{}}
	config := &types.Config{}
	typed, err := config.Check("github.com/division-sh/swarm/internal/serveapp", set, []*ast.File{file}, info)
	if err != nil {
		t.Fatal(err)
	}
	return &packages.Package{Fset: set, Types: typed, TypesInfo: info, Syntax: []*ast.File{file}}
}

func TestServedRoleDiscoveryClosesDependenciesButKeepsRawShadowedAndWholeValueUses(t *testing.T) {
	pkg := typedRoleProof(t, `package serveapp
type servedControlProofRuntime struct { Endpoint string; DB int }
type foreign struct{ Endpoint string }
func leaf(rt servedControlProofRuntime) string { return rt.Endpoint }
func parent(rt servedControlProofRuntime) string { return leaf(rt) }
func raw(rt servedControlProofRuntime) int { return rt.DB }
func whole(rt servedControlProofRuntime) servedControlProofRuntime { return rt }
func shadow(rt servedControlProofRuntime) string { return func(rt foreign)string{return rt.Endpoint}(foreign{rt.Endpoint}) }
`)
	roles := discoverServedRoles([]*packages.Package{pkg}, nil)
	if len(roles) != 2 || !reflect.DeepEqual(roles["leaf"].Fields, []string{"Endpoint"}) || !reflect.DeepEqual(roles["parent"].Fields, []string{"Endpoint"}) {
		t.Fatal("role discovery escaped dependency/authority boundary:", roles)
	}
	blocked := discoverServedRoles([]*packages.Package{pkg}, map[string]bool{"leaf": true})
	if len(blocked) != 0 {
		t.Fatal("blocked child left a partial parent migration:", blocked)
	}
}

func TestServedRoleArgumentExpansionNeverRepeatsDiscardsOrReordersEvaluation(t *testing.T) {
	for _, proof := range []struct {
		before        string
		fields, after []string
	}{
		{"rt", []string{"Backend", "Endpoint"}, []string{"rt.Backend", "rt.Endpoint"}},
		{"rt.servedControlProofRuntime", []string{"BundleHash", "Runtime"}, []string{"rt.servedControlProofRuntime.BundleHash", "rt.servedControlProofRuntime.Runtime"}},
		{"servedControlProofRuntime{Endpoint: endpointOnce()}", []string{"Endpoint"}, []string{"endpointOnce()"}},
	} {
		arg, err := parser.ParseExpr(proof.before)
		if err != nil {
			t.Fatal(err)
		}
		after, err := servedRoleArgumentValues(arg, proof.fields)
		if err != nil || !reflect.DeepEqual(after, proof.after) {
			t.Fatal("exact argument evaluation changed:", after, err)
		}
	}
	for _, before := range []string{"makeRuntime()", "h.runtime", "servedControlProofRuntime{Endpoint: second(),Backend:first()}", "servedControlProofRuntime{Backend:first(),Endpoint:second(),DB:third()}"} {
		arg, err := parser.ParseExpr(before)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := servedRoleArgumentValues(arg, []string{"Backend", "Endpoint"}); err == nil {
			t.Fatal("unbound/repeated/discarded/reordered evaluation accepted:", before)
		}
	}
}

func TestServedRoleExcludedAndIndirectConsumersHoldTheWholeHelper(t *testing.T) {
	roles := map[string]servedRole{"leaf": {Fields: []string{"Endpoint"}}}
	for _, source := range []string{
		"//go:build issue2413\n\npackage serveapp\nfunc excluded(){leaf(rt)}",
		"package serveapp\nvar callback=leaf",
		"package serveapp\nvar initializer=func(){leaf(rt)}",
	} {
		untyped := untypedServedRoleReferences(map[string][]byte{"fixture.go": []byte(source)}, roles, nil)
		if len(untyped) != 1 || untyped["leaf"] != "fixture.go" {
			t.Fatal("untyped source permitted a partial helper ABI change:", untyped)
		}
	}
}
