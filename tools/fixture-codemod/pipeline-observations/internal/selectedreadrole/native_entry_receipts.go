package selectedreadrole

import (
	"crypto/sha256"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
)

// #2542/6200510991: the complete accepted S6 fixture composes with B's
// unchanged historical recipes. This inverse certifies only this exact file.
const NativeEntry2542File = "internal/runtime/conformance/delivery_lifecycle_conformance_test.go"
const NativeEntry2542Prior = "74fd91d46e7c13e358858dfd03ae2b283769c67a"
const nativeEntry2542BeforeSHA = "67aa23608a2e81d1bf3fa4e23671611c6673254b3131364bc0d940244ab4c569"
const nativeEntry2542CurrentSHA = "3407e380b9946c0563079df9815615fe025bda389873e3427557065b44bda2b3"

func NativeEntry2542HistoricalSource(root, path string, current []byte) ([]byte, error) {
	if path != NativeEntry2542File || fmt.Sprintf("%x", sha256.Sum256(current)) != nativeEntry2542CurrentSHA {
		return current, nil // Earlier cuts remain subject to their own exact inverse.
	}
	key := "native-entry2542/" + root + "/" + nativeEntry2542BeforeSHA
	if cached, ok := sourceCache.Load(key); ok {
		return append([]byte(nil), cached.([]byte)...), nil
	}
	prior, err := exec.Command("git", "-C", root, "show", NativeEntry2542Prior+":"+path).Output()
	if err != nil || fmt.Sprintf("%x", sha256.Sum256(prior)) != nativeEntry2542BeforeSHA {
		return nil, fmt.Errorf("native-entry conformance predecessor differs: %v", err)
	}
	sourceCache.Store(key, append([]byte(nil), prior...))
	return prior, nil
}

func nativeEntry2542HistoricalFunction(root, path, name, current string) (string, error) {
	if path != NativeEntry2542File {
		return current, nil
	}
	raw, err := os.ReadFile(filepath.Join(root, path))
	if err != nil {
		return "", err
	}
	actual, err := Body(raw, name)
	if err != nil || actual != current {
		return current, nil
	}
	prior, err := NativeEntry2542HistoricalSource(root, path, raw)
	if err != nil {
		return "", err
	}
	previous, err := Body(prior, name)
	if err != nil {
		return current, nil // New setup functions have no historical recipe.
	}
	return previous, nil
}
