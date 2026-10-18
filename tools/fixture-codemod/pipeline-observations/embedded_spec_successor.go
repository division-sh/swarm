package main

import (
	"bytes"
	"crypto/sha256"
	_ "embed"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"

	"golang.org/x/tools/go/ast/astutil"
)

// USER6197217312 and reviewer-g6201157565 retire only the spec selector.
// Every earlier transformation and successor remains in this frozen packet.
//
//go:embed testdata/embedded_spec_successors_1801.json
var embeddedSpecSuccessorBytes1801 []byte

type embeddedSpecSuccessorPacket1801 struct {
	Source, Authorization, PopulationSHA256 string
	Rows                                    []fixtureSetupSuccessor2617
}

func loadEmbeddedSpecSuccessors1801(raw []byte) (embeddedSpecSuccessorPacket1801, error) {
	var packet embeddedSpecSuccessorPacket1801
	if fmt.Sprintf("%x", sha256.Sum256(raw)) != "461fec6b4bf08382294ffa6120f66ac9cd92b999a6c493fc06657a3d30576ace" {
		return packet, fmt.Errorf("embedded-spec successor evidence changed")
	}
	if err := json.Unmarshal(raw, &packet); err != nil {
		return packet, err
	}
	if packet.Source != "53788da1332cbecaf805b13722316257d7ac6829" ||
		packet.Authorization != "https://github.com/division-sh/swarm/issues/1801#issuecomment-6201157565" || len(packet.Rows) != 28 {
		return packet, fmt.Errorf("wrong embedded-spec source, authorization or finite population")
	}
	return packet, nil
}

// Historical proofs and the current endpoint resolver share this one transform.
func embeddedSpecFieldDeletion1801(source string) (string, error) {
	file, err := parser.ParseFile(token.NewFileSet(), "prior.go", "package proof\n"+source, 0)
	if err != nil {
		return "", err
	}
	astutil.Apply(file, func(cursor *astutil.Cursor) bool {
		switch node := cursor.Node().(type) {
		case *ast.Ident:
			if node.Name == "CLISourcePlatformSpecPaths" {
				node.Name = "CLISourcePaths"
			}
		case *ast.CompositeLit:
			deleteEmbeddedSpecCompositeField1801(node)
		case *ast.CallExpr:
			switch formattedNativeReadNode(node.Fun) {
			case "initializeServePlatformStateStores", "cliapp.NewSwarmWorkflowModule":
				if len(node.Args) == 3 {
					node.Args = node.Args[:2]
				}
			case "loadServePlatformSpecDocument":
				if len(node.Args) == 1 {
					node.Args = nil
				}
			}
		case *ast.AssignStmt:
			if node.Tok == token.ASSIGN && len(node.Lhs) == 1 && len(node.Rhs) == 1 &&
				formattedNativeReadNode(node.Lhs[0]) == "opts.PlatformSpecPath" && formattedNativeReadNode(node.Rhs[0]) == "defaultPlatformSpecPath" {
				cursor.Delete()
				return false
			}
		}
		return true
	}, nil)
	return canonicalFunction(formattedNativeReadNode(file.Decls[0]))
}

func deleteEmbeddedSpecCompositeField1801(node *ast.CompositeLit) {
	if node.Type == nil {
		return
	}
	switch formattedNativeReadNode(node.Type) {
	case "cliapp.ServeOptions", "cliapp.CLISourcePlatformSpecPaths",
		"admittedFixtureSelectedContractSourceLoader", "SourceArtifactSelectedContractSourceLoader",
		"runforkexecution.SourceArtifactSelectedContractSourceLoader", "runtimerunforkexecution.SourceArtifactSelectedContractSourceLoader":
		var kept []ast.Expr
		for _, field := range node.Elts {
			if kv, ok := field.(*ast.KeyValueExpr); ok && formattedNativeReadNode(kv.Key) == "PlatformSpecPath" {
				continue
			}
			kept = append(kept, field)
		}
		node.Elts = kept
	}
}

func formattedNativeReadNode(node ast.Node) string {
	var out bytes.Buffer
	if err := format.Node(&out, token.NewFileSet(), node); err != nil {
		panic(err)
	}
	return out.String()
}
