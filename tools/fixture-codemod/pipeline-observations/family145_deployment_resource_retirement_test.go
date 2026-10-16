package main

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
)

func deploymentResourceCarrierClosed(raw []byte) error {
	file, err := parser.ParseFile(token.NewFileSet(), "source.go", raw, parser.AllErrors)
	if err != nil {
		return err
	}
	for _, declaration := range file.Decls {
		decl, ok := declaration.(*ast.GenDecl)
		if !ok {
			continue
		}
		for _, spec := range decl.Specs {
			typeSpec, ok := spec.(*ast.TypeSpec)
			if !ok || typeSpec.Name.Name != "deploymentResourceFixture" {
				continue
			}
			shape, ok := typeSpec.Type.(*ast.StructType)
			if !ok || len(shape.Fields.List) != 6 {
				return os.ErrInvalid
			}
			want := []string{"selected", "postgresDSN", "source", "runtime", "topology", "ctx"}
			for index, field := range shape.Fields.List {
				if len(field.Names) != 1 || field.Names[0].Name != want[index] {
					return os.ErrInvalid
				}
			}
			return nil
		}
	}
	return os.ErrNotExist
}

type deploymentResourceOwnerAddition struct {
	file, digest, retainedCut, scopeCut string
	names                               []string
}

// These two older whole-file controls are already red at the signed predecessor.
// Prove only this batch's exact additions; do not rotate their historical pins.
func deploymentResourceOwnerAdditionMatches(pin deploymentResourceOwnerAddition, current, previous []byte) error {
	var err error
	current, err = earlierStandingIngressOwnerFile("../../..", pin.file, current)
	if err != nil {
		return err
	}
	if fmt.Sprintf("%x", sha256.Sum256(current)) != pin.digest {
		return fmt.Errorf("deployment-resource whole owner changed: %s", pin.file)
	}
	for _, name := range pin.names {
		body, _, _, err := deliveryStatusSourceDeclaration(current, name)
		if err != nil {
			return err
		}
		current, err = removePublicRollbackDeclaration(current, name, body)
		if err != nil {
			return err
		}
	}
	restored, err := format.Source(current)
	if err != nil || !bytes.Equal(restored, previous) {
		return fmt.Errorf("deployment-resource addition changed retained signed owner: %s: %v", pin.file, err)
	}
	return nil
}
