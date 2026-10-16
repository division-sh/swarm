package selectedreadrole

import (
	"crypto/sha256"
	_ "embed"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
)

// This finite inverse serves historical transformation receipts only. Current
// source and recipe correspondence are checked separately against the complete
// composed packet; these views never rewrite a checkout or grant SQL authority.
// Authorization: #2542/6194296317.
//
//go:embed testdata/catch_up_2542.json
var catchUp2542Bytes []byte

const catchUp2542Lane = "dec6f1a009400d230b0bb92c83c0997ae35b2938"
const catchUp2542Master = "19dc188f6d57985a3360a619ea57b890b3c148dd"

type CatchUp2542Row struct {
	Key, Disposition, BaseSHA256, MasterSHA256, LaneSHA256, ComposedSHA256, SourceSHA256 string
}

type CatchUp2542File struct {
	File, ComposedSHA256, MasterSHA256, LaneSHA256 string
}

type CatchUp2542Packet struct {
	Base, Master, Lane, Authorization, ComposedSHA256 string
	Rows                                              []CatchUp2542Row
	Files                                             []CatchUp2542File
}

var catchUp2542Once sync.Once
var catchUp2542Packet CatchUp2542Packet
var catchUp2542Error error

func CatchUp2542Receipts() (CatchUp2542Packet, error) {
	packet, err := readCatchUp2542Packet()
	packet.Rows = append([]CatchUp2542Row(nil), packet.Rows...)
	packet.Files = append([]CatchUp2542File(nil), packet.Files...)
	return packet, err
}

func readCatchUp2542Packet() (CatchUp2542Packet, error) {
	catchUp2542Once.Do(func() {
		if fmt.Sprintf("%x", sha256.Sum256(catchUp2542Bytes)) != "ddadfba0b9aff02fa963daebb07d95f9bda9b71ea84d2ea3c45cec466275aa5e" {
			catchUp2542Error = fmt.Errorf("finite catch-up composition packet changed")
			return
		}
		catchUp2542Error = json.Unmarshal(catchUp2542Bytes, &catchUp2542Packet)
		if catchUp2542Error == nil && (catchUp2542Packet.Lane != catchUp2542Lane || catchUp2542Packet.Master != catchUp2542Master) {
			catchUp2542Error = fmt.Errorf("finite catch-up history inputs differ from their signed packet")
		}
	})
	return catchUp2542Packet, catchUp2542Error
}

func CatchUp2542JSONDigest(raw []byte) (string, error) {
	key := fmt.Sprintf("catch-up2542/json/%x", sha256.Sum256(raw))
	if cached, ok := sourceCache.Load(key); ok {
		return cached.(string), nil
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", err
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	digest := fmt.Sprintf("%x", sha256.Sum256(canonical))
	sourceCache.Store(key, digest)
	return digest, nil
}

func CatchUp2542HistoricalRecipes(root string, current []byte, master bool) ([]byte, error) {
	packet, err := readCatchUp2542Packet()
	if err != nil {
		return nil, err
	}
	digest, err := CatchUp2542JSONDigest(current)
	if err != nil || digest != packet.ComposedSHA256 {
		return nil, fmt.Errorf("catch-up historical view requires its complete exact composed ledger: %v", err)
	}
	ref := packet.Lane
	if master {
		ref = packet.Master
	}
	return exec.Command("git", "-C", root, "show", ref+":tools/fixture-codemod/pipeline-observations/recipes.json").Output()
}

// Older archives and intermediate inverses keep their own exact validators.
// Only this one fully authenticated composed ledger has a catch-up inverse.
func catchUp2542LaneLedger(root string, current []byte) ([]byte, error) {
	packet, err := readCatchUp2542Packet()
	if err != nil {
		return nil, err
	}
	digest, err := CatchUp2542JSONDigest(current)
	if err != nil {
		return nil, err
	}
	if digest != packet.ComposedSHA256 {
		return current, nil
	}
	return CatchUp2542HistoricalRecipes(root, current, false)
}

func CatchUp2542HistoricalSource(root, path string, current []byte) ([]byte, error) {
	var err error
	current, err = Repair2698HistoricalSource(root, path, current)
	if err != nil {
		return nil, err
	}
	current, err = NativeEntry2542HistoricalSource(root, path, current)
	if err != nil {
		return nil, err
	}
	packet, err := readCatchUp2542Packet()
	if err != nil {
		return nil, err
	}
	digest := fmt.Sprintf("%x", sha256.Sum256(current))
	for _, file := range packet.Files {
		if file.File != path || file.ComposedSHA256 != digest || file.LaneSHA256 == "" || file.ComposedSHA256 == file.LaneSHA256 {
			continue
		}
		key := "catch-up2542/lane-source/" + path + "/" + file.LaneSHA256
		if cached, ok := sourceCache.Load(key); ok {
			return append([]byte(nil), cached.([]byte)...), nil
		}
		raw, err := exec.Command("git", "-C", root, "show", packet.Lane+":"+path).Output()
		if err != nil || fmt.Sprintf("%x", sha256.Sum256(raw)) != file.LaneSHA256 {
			return nil, fmt.Errorf("catch-up signed lane source differs: %s: %v", path, err)
		}
		sourceCache.Store(key, append([]byte(nil), raw...))
		return raw, nil
	}
	return current, nil
}

func CatchUp2542HistoricalFunction(root, path, name, current string) (string, error) {
	var err error
	current, err = repair2698HistoricalFunction(root, path, name, current)
	if err != nil {
		return "", err
	}
	current, err = nativeEntry2542HistoricalFunction(root, path, name, current)
	if err != nil {
		return "", err
	}
	packet, err := readCatchUp2542Packet()
	if err != nil {
		return "", err
	}
	listed := false
	for _, file := range packet.Files {
		if file.File == path && file.LaneSHA256 != "" && file.ComposedSHA256 != "absent" {
			listed = true
			break
		}
	}
	if !listed {
		return current, nil
	}
	raw, err := os.ReadFile(filepath.Join(root, path))
	if err != nil {
		return "", err
	}
	key := fmt.Sprintf("catch-up2542/function/%s/%s/%s/%x/%x", root, path, name, sha256.Sum256(raw), sha256.Sum256([]byte(current)))
	if cached, ok := sourceCache.Load(key); ok {
		return cached.(string), nil
	}
	file, err := parser.ParseFile(token.NewFileSet(), "receipt.go", "package proof\n"+current, parser.AllErrors)
	if err != nil || len(file.Decls) != 1 {
		return current, nil
	}
	fn, ok := file.Decls[0].(*ast.FuncDecl)
	if !ok {
		return current, nil
	}
	actual, err := Body(raw, fn.Name.Name)
	if err != nil || actual != current {
		return current, nil
	}
	prior, err := CatchUp2542HistoricalSource(root, path, raw)
	if err != nil {
		return "", err
	}
	if string(prior) == string(raw) {
		return current, nil
	}
	previous, err := Body(prior, name)
	if err == nil {
		sourceCache.Store(key, previous)
	}
	return previous, err
}
