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
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/checkoutsource"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runphysical"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/servedsettlement"
)

//go:embed family145-served-publication-lookup-preimages.json
var servedPublicationLookupBytes []byte

func servedPublicationLookupPins(t *testing.T) []runphysical.Pin {
	t.Helper()
	pins, err := servedsettlement.DecodeLookup(servedPublicationLookupBytes)
	if err != nil {
		t.Fatal(err)
	}
	return pins
}

func servedPublicationLookupEarlierDeclaration(t *testing.T, file, function, current string) string {
	t.Helper()
	for _, pin := range servedPublicationLookupPins(t) {
		if pin.File != file || pin.Function != function {
			continue
		}
		raw, err := os.ReadFile(filepath.Join("../../..", file))
		if err != nil || servedSetupEarlierSource(t, file, function, requestKeyDeclaration(t, raw, function)) != pin.After || current != pin.After {
			t.Fatal("lookup composition requires the actual unique final source:", function, err)
		}
		return pin.Before
	}
	return current
}
