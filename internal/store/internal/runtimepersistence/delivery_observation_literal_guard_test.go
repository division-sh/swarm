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
	literals                           []string
}

// Reviewer-a #642/6202927132 admits only these declarations and literal uses.
var reviewedDeliveryObservations = []reviewedDeliveryObservation{
	{
		"internal/store/internal/runtimepersistence/test_pipeline_storage_observation.go",
		"ReadSelectedFanOutOutcomeCorrespondenceForTest",
		"e17afeaade9082bae101f8b0cd447e5bd9ced67eec5249c0eaf775067e4cddc2",
		"9a160829a4c1293420b34e4fa58afed63e51ecde1c9409d5b8e960d7981b2fc3",
		[]string{"2db2449750c63f011603169fba0e0c559dba63bb857cfdeb3ac19a8eea3ebc06"},
	},
	{
		"internal/store/internal/runtimepersistence/test_served_delivery_observation.go",
		"ReadSelectedDeploymentResourceWaitForTest",
		"cc3ccbefef8379863820615d14216e63c08a91a4c6d715c99d5a434b1bbf4d2e",
		"c27507fd759570b37a45836c902ea7fcfe521eb68e359e330dde0dbf4cf1109b",
		[]string{"4575a21d4ae03774efaa417977d6515d8e106d6cff94b4f5ea08b541ab67da7b"},
	},
	{
		"internal/store/internal/runtimepersistence/test_served_delivery_observation.go",
		"ReadSelectedDeploymentForkDiagnosticsForTest",
		"cc3ccbefef8379863820615d14216e63c08a91a4c6d715c99d5a434b1bbf4d2e",
		"0ad260e3efdf07c398c4aed2a6ba209a037b395c114e579afb2593c654d4f751",
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
	if deliveryObservationContext(owner) != "2d19ebe9dbb52707eb2c3b8bd02b1aaab935f8c3abda50a8397be23637f69939" {
		return approved
	}
	reader := deliveryObservationDeclaration(owner, "readServedDeliveryObservation")
	if reader == nil || deliveryObservationDigest(reader) != "f26cedfcd88e03e1480a7de1652058c39b3b7bf54f35177a957448d01073ae26" {
		return approved
	}
	for _, site := range reviewedDeliveryObservations {
		if path != site.path || deliveryObservationContext(file) != site.context {
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
