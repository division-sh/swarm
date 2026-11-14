package runtimepersistence

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"go/ast"
	"go/format"
	"go/token"
	"strconv"
)

type reviewedDeliveryObservation struct {
	path, declaration, context, digest string
	ownerName                          string
	ownerDigests                       [2]string
	literals                           []string
}

// Reviewer-a #642/6202927132 admits only these declarations and literal uses.
var reviewedDeliveryObservations = []reviewedDeliveryObservation{
	{
		"internal/store/internal/backend/runforkpersistence/test_selected_deployment_observation.go",
		"readSelectedDeploymentResourceWaitTx",
		"515dad80d026ca8c5a1246930c6fed6980f0882d1ecbedcf9924cbf2d118c128",
		"6df92bfe83a83c5685040a20a44305b7ab94bc619ba7f73e093b2defde2f50e3",
		"ReadSelectedDeploymentResourceWaitForTest",
		[2]string{"7386500ed651e7df8910ac65d6e53b9bee21efd1a1e94d179a5fbe2c547e1362", "daa7c05ebec221a4d0fa4c4b15b39d666e346af7a6b11dd6174eb317056f498f"},
		[]string{"4575a21d4ae03774efaa417977d6515d8e106d6cff94b4f5ea08b541ab67da7b"},
	},
	{
		"internal/store/internal/backend/runforkpersistence/test_selected_deployment_observation.go",
		"readSelectedDeploymentForkDiagnosticsTx",
		"515dad80d026ca8c5a1246930c6fed6980f0882d1ecbedcf9924cbf2d118c128",
		"f956aeaecdd9aab31a71b1a250f54dce38213a18e9f6bafa0df1a535a3589543",
		"ReadSelectedDeploymentForkDiagnosticsForTest",
		[2]string{"7eac10eb2d1cebff9224f8155542414fa75b75e9da6cfd424fa62583975b042b", "8577f638ab02cf2b8313ee25b5afbf4e3b9ead2befa8c6197d4ebb30e48449dd"},
		[]string{
			"4575a21d4ae03774efaa417977d6515d8e106d6cff94b4f5ea08b541ab67da7b",
			"3f9d86e12487f8417437391d9f89a262ebe98b4c458bcab4131d626d5311c1bd",
			"9c85825de8d10337a89ea44fe023bfe7dc94feeb591654b49071ef6aff6b9dc0",
			"9f4e3301baf44e2dfd45029eabdcf365f3d8e3d0451704df0b5165cee756744a",
			"f5dea214f37e354784c00b7e61256d32f4c09d5bbebd1381914fed1819242347",
			"ac507aa89c6fe68ad1cde5a98b475db73d6b6ae7712de40f6896a3ee67ecea75",
		},
	},
}

func deliveryObservationDigest(node ast.Node) string {
	var out bytes.Buffer
	if err := format.Node(&out, token.NewFileSet(), node); err != nil {
		return ""
	}
	return fmt.Sprintf("%x", sha256.Sum256(out.Bytes()))
}

func deliveryObservationContext(file *ast.File) string {
	var out bytes.Buffer
	fmt.Fprintln(&out, file.Name.Name)
	for _, imported := range file.Imports {
		if err := format.Node(&out, token.NewFileSet(), imported); err != nil {
			return ""
		}
		out.WriteByte('\n')
	}
	return fmt.Sprintf("%x", sha256.Sum256(out.Bytes()))
}

func deliveryObservationDeclaration(file *ast.File, name string) *ast.FuncDecl {
	var found *ast.FuncDecl
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != name {
			continue
		}
		if found != nil || fn.Recv != nil {
			return nil
		}
		found = fn
	}
	return found
}

func deliveryObservationMethod(file *ast.File, name, receiver string) *ast.FuncDecl {
	var found *ast.FuncDecl
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != name || fn.Recv == nil || len(fn.Recv.List) != 1 {
			continue
		}
		star, ok := fn.Recv.List[0].Type.(*ast.StarExpr)
		if !ok {
			continue
		}
		ident, ok := star.X.(*ast.Ident)
		if !ok || ident.Name != receiver {
			continue
		}
		if found != nil {
			return nil
		}
		found = fn
	}
	return found
}

func deliveryObservationLiterals(fn *ast.FuncDecl) []*ast.BasicLit {
	var found []*ast.BasicLit
	ast.Inspect(fn, func(node ast.Node) bool {
		literal, ok := node.(*ast.BasicLit)
		if ok && literal.Kind == token.STRING {
			query, err := strconv.Unquote(literal.Value)
			if err == nil && executableDeliverySQL.MatchString(query) {
				found = append(found, literal)
			}
		}
		return true
	})
	return found
}

func reviewedDeliveryObservationLiterals(path string, file, owner *ast.File) map[token.Pos]string {
	approved := map[token.Pos]string{}
	for _, site := range reviewedDeliveryObservations {
		if path != site.path || deliveryObservationContext(file) != site.context || deliveryObservationContext(owner) != site.context {
			continue
		}
		postgres := deliveryObservationMethod(owner, site.ownerName, "RunForkPostgresOwner")
		sqlite := deliveryObservationMethod(owner, site.ownerName, "RunForkSQLiteOwner")
		if postgres == nil || sqlite == nil || deliveryObservationDigest(postgres) != site.ownerDigests[0] || deliveryObservationDigest(sqlite) != site.ownerDigests[1] {
			continue
		}
		fn := deliveryObservationDeclaration(file, site.declaration)
		if fn == nil || deliveryObservationDigest(fn) != site.digest {
			continue
		}
		literals := deliveryObservationLiterals(fn)
		if len(literals) != len(site.literals) {
			continue
		}
		matches := true
		for index, literal := range literals {
			query, err := strconv.Unquote(literal.Value)
			if err != nil || !selectedForkDeliveryReadSQL(query) || fmt.Sprintf("%x", sha256.Sum256([]byte(query))) != site.literals[index] {
				matches = false
			}
		}
		if matches {
			for _, literal := range literals {
				approved[literal.Pos()] = site.path + "/" + site.declaration
			}
		}
	}
	return approved
}
