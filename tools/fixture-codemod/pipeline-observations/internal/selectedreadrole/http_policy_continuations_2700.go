package selectedreadrole

import (
	"bytes"
	"crypto/sha256"
	_ "embed"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
)

// #2700/6252046415 and 6252046258: finite, actual-source-authenticated
// continuations of immutable proof history, never inputs to the live census.
//
//go:embed testdata/http_policy_correspondences_2700.json
var http2700CorrespondenceBytes []byte

const HTTP2700CorrespondenceDigest = "5341096c7c1d612b1b427de6433a5ae35f08ea14a74862b4f4f3921cfb512784"
const http2700CurrentLedger = "afe1f636e21c4016bb38f088145aa94a9597344e05692d5bfa4e8ed9c5f44c97"

type HTTP2700Source struct {
	Ref, FileSHA256, DeclarationSHA256, CanonicalSHA256 string
}

type HTTP2700Edit struct {
	Start          int
	Prior, Current []string
}

type HTTP2700Continuation struct {
	FileEdits                                                                []HTTP2700Edit
	Index, Multiplicity, RemovedPolicySites                                  int
	Class, Candidate, PriorRecipeRef, CurrentSourceRef, CurrentFileSHA256    string
	CurrentDeclarationSHA256, BeforeSHA256, AfterSHA256, PriorEndpointSHA256 string
	Decision                                                                 string
	Original                                                                 json.RawMessage
	PriorSource                                                              HTTP2700Source
	Edits                                                                    []HTTP2700Edit
}

type HTTP2700Packet struct {
	FrozenSource, DiscoveryLogSHA256 string
	InspectedRecipeCount             int
	Rows                             []HTTP2700Continuation
	OutOfClass                       []json.RawMessage
}

func HTTP2700Correspondences() (HTTP2700Packet, error) {
	var packet HTTP2700Packet
	if fmt.Sprintf("%x", sha256.Sum256(http2700CorrespondenceBytes)) != HTTP2700CorrespondenceDigest {
		return packet, fmt.Errorf("HTTP policy continuation packet changed")
	}
	if err := json.Unmarshal(http2700CorrespondenceBytes, &packet); err != nil {
		return packet, err
	}
	if packet.FrozenSource != "0d64f764057322d379cbb5491113a1a20416df4d" ||
		packet.InspectedRecipeCount != 1856 || len(packet.Rows) != 13 || len(packet.OutOfClass) != 12 {
		return packet, fmt.Errorf("HTTP policy continuation inventory changed")
	}
	return packet, nil
}

func http2700RecipeIdentity(raw []byte) (family, path, function string, err error) {
	var identity struct{ Family, File, Function string }
	err = json.Unmarshal(raw, &identity)
	return identity.Family, identity.File, identity.Function, err
}

func http2700Canonical(body string) (string, string, error) {
	file, err := parser.ParseFile(token.NewFileSet(), "continuation.go", "package proof\n"+body, 0)
	if err != nil || len(file.Decls) != 1 {
		return "", "", fmt.Errorf("HTTP policy continuation is not one declaration: %v", err)
	}
	fn, ok := file.Decls[0].(*ast.FuncDecl)
	if !ok || fn.Recv != nil {
		return "", "", fmt.Errorf("HTTP policy continuation has a foreign declaration")
	}
	return requestkey.Node(fn), fn.Name.Name, nil
}

func HTTP2700PriorEndpoint(row HTTP2700Continuation) (string, error) {
	var original struct{ After, Successor string }
	if err := json.Unmarshal(row.Original, &original); err != nil {
		return "", err
	}
	if original.Successor != "" {
		return original.Successor, nil
	}
	return original.After, nil
}

func HTTP2700Reverse(row HTTP2700Continuation) (string, error) {
	got, err := http2700ReverseLines(row.Candidate, row.Edits)
	if err != nil {
		return "", err
	}
	prior, err := HTTP2700PriorEndpoint(row)
	if err != nil || got != prior {
		return "", fmt.Errorf("HTTP policy continuation does not restore its complete prior endpoint")
	}
	return prior, nil
}

func http2700ReverseLines(current string, edits []HTTP2700Edit) (string, error) {
	lines := strings.SplitAfter(current, "\n")
	end := len(lines)
	for index := len(edits) - 1; index >= 0; index-- {
		edit := edits[index]
		stop := edit.Start + len(edit.Current)
		if edit.Start < 0 || stop > end || !reflect.DeepEqual(lines[edit.Start:stop], edit.Current) {
			return "", fmt.Errorf("HTTP policy continuation changed its exact edit %d", index)
		}
		lines = append(append(append([]string{}, lines[:edit.Start]...), edit.Prior...), lines[stop:]...)
		end = edit.Start
	}
	return strings.Join(lines, ""), nil
}

func http2700CurrentSource(root string, row HTTP2700Continuation, actual string) error {
	_, path, _, err := http2700RecipeIdentity(row.Original)
	if err != nil {
		return err
	}
	raw, err := os.ReadFile(filepath.Join(root, path))
	if err != nil || fmt.Sprintf("%x", sha256.Sum256(raw)) != row.CurrentFileSHA256 {
		return fmt.Errorf("HTTP policy actual source changed: %s: %v", path, err)
	}
	candidate, name, err := http2700Canonical(row.Candidate)
	if err != nil {
		return err
	}
	observed, err := Body(raw, name)
	if err != nil || observed != candidate {
		return fmt.Errorf("HTTP policy actual declaration changed: %s: %v", name, err)
	}
	got, _, err := http2700Canonical(actual)
	if err != nil || got != candidate {
		return fmt.Errorf("HTTP policy current input is not its live endpoint: %s", name)
	}
	return nil
}

// Only the complete matching current tuple receives the prior historical pair.
func HTTP2700PriorRecipe(root string, raw []byte, actual string) ([]byte, string, bool, error) {
	packet, err := HTTP2700Correspondences()
	if err != nil {
		return nil, "", false, err
	}
	family, path, function, err := http2700RecipeIdentity(raw)
	if err != nil {
		return nil, "", false, err
	}
	for _, row := range packet.Rows {
		oldFamily, oldPath, oldFunction, err := http2700RecipeIdentity(row.Original)
		if err != nil {
			return nil, "", false, err
		}
		if family != oldFamily || path != oldPath || function != oldFunction {
			continue
		}
		return http2700PriorPair(root, row, raw, actual)
	}
	return raw, actual, false, nil
}

func http2700PriorPair(root string, row HTTP2700Continuation, raw []byte, actual string) ([]byte, string, bool, error) {
	var expected map[string]any
	if err := json.Unmarshal(row.Original, &expected); err != nil {
		return nil, "", true, err
	}
	expected["Successor"] = row.Candidate
	want, err := json.Marshal(expected)
	if err != nil {
		return nil, "", true, err
	}
	wantDigest, err := CatchUp2542JSONDigest(want)
	if err != nil {
		return nil, "", true, err
	}
	gotDigest, err := CatchUp2542JSONDigest(raw)
	if err != nil || wantDigest != gotDigest {
		return nil, "", true, fmt.Errorf("HTTP policy current recipe changed its identity/history: %v", err)
	}
	if err := http2700CurrentSource(root, row, actual); err != nil {
		return nil, "", true, err
	}
	prior, err := HTTP2700Reverse(row)
	return bytes.Clone(row.Original), prior, true, err
}

func HTTP2700HistoricalRecipes(root string, current []byte) ([]byte, error) {
	digest, err := CatchUp2542JSONDigest(current)
	if err != nil {
		return nil, err
	}
	if digest != http2700CurrentLedger {
		return current, nil
	}
	packet, err := HTTP2700Correspondences()
	if err != nil {
		return nil, err
	}
	var rows []json.RawMessage
	if err := json.Unmarshal(current, &rows); err != nil {
		return nil, err
	}
	for _, row := range packet.Rows {
		prior, _, _, err := http2700PriorPair(root, row, rows[row.Index], row.Candidate)
		if err != nil {
			return nil, err
		}
		rows[row.Index] = prior
	}
	return json.Marshal(rows)
}

// Compose an already-authenticated native source comparison with its exact
// newer policy endpoint. Other historical text receives no new inverse.
func HTTP2700HistoricalFunction(root, path, name, current string) (string, error) {
	packet, err := HTTP2700Correspondences()
	if err != nil {
		return "", err
	}
	for _, row := range packet.Rows {
		_, file, _, err := http2700RecipeIdentity(row.Original)
		if err != nil {
			return "", err
		}
		if file != path {
			continue
		}
		candidate, declared, err := http2700Canonical(row.Candidate)
		if err != nil {
			return "", err
		}
		observed, _, parseErr := http2700Canonical(current)
		if declared != name || parseErr != nil || observed != candidate {
			continue
		}
		if err := http2700CurrentSource(root, row, current); err != nil {
			return "", err
		}
		prior, err := HTTP2700Reverse(row)
		if err != nil {
			return "", err
		}
		body, _, err := http2700Canonical(prior)
		return body, err
	}
	return current, nil
}

// These two complete files have exact reviewed inverses. Other files contain
// out-of-class differences and receive no file-level historical substitution.
func HTTP2700HistoricalSource(root, path string, current []byte) ([]byte, error) {
	if path != "internal/serveapp/semantic_numeric_activity_test.go" && path != "internal/apiv1/operator_event_replay_test.go" {
		return current, nil
	}
	packet, err := HTTP2700Correspondences()
	if err != nil {
		return nil, err
	}
	for _, row := range packet.Rows {
		_, file, _, err := http2700RecipeIdentity(row.Original)
		if err != nil {
			return nil, err
		}
		if file != path || fmt.Sprintf("%x", sha256.Sum256(current)) != row.CurrentFileSHA256 {
			continue
		}
		if err := http2700CurrentSource(root, row, row.Candidate); err != nil {
			return nil, err
		}
		prior, err := http2700ReverseLines(string(current), row.FileEdits)
		if err != nil || fmt.Sprintf("%x", sha256.Sum256([]byte(prior))) != row.PriorSource.FileSHA256 {
			return nil, fmt.Errorf("HTTP policy signed predecessor changed: %s: %v", path, err)
		}
		return []byte(prior), nil
	}
	return current, nil
}
