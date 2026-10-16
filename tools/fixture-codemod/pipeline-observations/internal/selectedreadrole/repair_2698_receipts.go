package selectedreadrole

import (
	"bytes"
	"crypto/sha256"
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
)

// The historical view is limited to the exact settled #2698 source repair.
// Neither the recipes nor any prior source/history packet is rewritten.
//
//go:embed testdata/repair_2698_sources.json
var repair2698Sources []byte

const Repair2698Before = "e857a1e482a2e08ab63f2e8169b4472e608cfdc2"
const Repair2698After = "dcd9143c696a854e16c072114f656887fd1a10bd"
const Repair2698SourceDigest = "d2ad1963243b89c6325e855cb175bd62d7f59987fc6a8a0abf89eab7f9e19ace"

type Repair2698Source struct{ File, BeforeSHA256, AfterSHA256 string }

func Repair2698Sources() ([]Repair2698Source, error) {
	if fmt.Sprintf("%x", sha256.Sum256(repair2698Sources)) != Repair2698SourceDigest {
		return nil, fmt.Errorf("native repair source packet changed")
	}
	var packet struct {
		BeforeCommit, AfterCommit, Authorization string
		Files                                    []Repair2698Source
	}
	if err := json.Unmarshal(repair2698Sources, &packet); err != nil {
		return nil, err
	}
	if packet.BeforeCommit != Repair2698Before || packet.AfterCommit != Repair2698After || len(packet.Files) != 41 ||
		packet.Authorization != "https://github.com/division-sh/swarm/pull/2698#issuecomment-6205327685" {
		return nil, fmt.Errorf("native repair lost its exact source inputs or authorization")
	}
	return packet.Files, nil
}

// Authenticate the supplied actual bytes before reading immutable git history.
// Unknown and intermediate bytes receive no inverse, including in copied trees.
func Repair2698HistoricalSource(root, path string, current []byte) ([]byte, error) {
	pins, err := Repair2698Sources()
	if err != nil {
		return nil, err
	}
	digest := fmt.Sprintf("%x", sha256.Sum256(current))
	for _, pin := range pins {
		if pin.File != path || pin.AfterSHA256 != digest {
			continue
		}
		key := "repair2698/" + root + "/" + path + "/" + pin.BeforeSHA256
		if cached, ok := sourceCache.Load(key); ok {
			return bytes.Clone(cached.([]byte)), nil
		}
		previous, err := exec.Command("git", "-C", root, "show", Repair2698Before+":"+path).Output()
		if err != nil || fmt.Sprintf("%x", sha256.Sum256(previous)) != pin.BeforeSHA256 {
			return nil, fmt.Errorf("native repair lost its signed predecessor %s: %v", path, err)
		}
		sourceCache.Store(key, bytes.Clone(previous))
		return previous, nil
	}
	return current, nil
}

func repair2698HistoricalFunction(root, path, name, current string) (string, error) {
	pins, err := Repair2698Sources()
	if err != nil {
		return "", err
	}
	listed := false
	for _, pin := range pins {
		listed = listed || pin.File == path
	}
	if !listed {
		return current, nil
	}
	raw, err := os.ReadFile(filepath.Join(root, path))
	if err != nil {
		return "", err
	}
	actual, err := Body(raw, name)
	if err != nil || actual != current {
		return current, nil // Historical/intermediate text is not a live certificate.
	}
	previous, err := Repair2698HistoricalSource(root, path, raw)
	if err != nil || bytes.Equal(previous, raw) {
		return current, err
	}
	// Compose through the unchanged earlier integration packet exactly once.
	previous, err = CatchUp2542HistoricalSource(root, path, previous)
	if err != nil {
		return "", err
	}
	body, err := Body(previous, name)
	if err != nil {
		return current, nil // A new native setup declaration has no old recipe.
	}
	return body, nil
}
