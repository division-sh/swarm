package selectedreadrole

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestRepair2698SourceCompositionPreservesBothSignedCuts(t *testing.T) {
	root := filepath.Clean("../../../../..")
	pins, err := Repair2698Sources()
	if err != nil {
		t.Fatal(err)
	}
	previousPath := ""
	for _, pin := range pins {
		t.Run(pin.File, func(t *testing.T) {
			if pin.File <= previousPath || !filepath.IsLocal(pin.File) || filepath.ToSlash(filepath.Clean(pin.File)) != pin.File {
				t.Fatal("source packet is not a unique canonical sorted inventory")
			}
			previousPath = pin.File
			current, err := os.ReadFile(filepath.Join(root, pin.File))
			if err != nil || fmt.Sprintf("%x", sha256.Sum256(current)) != pin.AfterSHA256 {
				t.Fatal("actual source differs from the finite repaired endpoint:", err)
			}
			for _, cut := range []struct{ ref, digest string }{{Repair2698Before, pin.BeforeSHA256}, {Repair2698After, pin.AfterSHA256}} {
				raw, err := exec.Command("git", "-C", root, "show", cut.ref+":"+pin.File).Output()
				if err != nil || fmt.Sprintf("%x", sha256.Sum256(raw)) != cut.digest {
					t.Fatal("source packet differs from its signed cut:", cut.ref, err)
				}
			}
			previous, err := Repair2698HistoricalSource(root, pin.File, current)
			if err != nil || fmt.Sprintf("%x", sha256.Sum256(previous)) != pin.BeforeSHA256 {
				t.Fatal("complete historical source was not restored:", err)
			}
			again, err := Repair2698HistoricalSource(root, pin.File, previous)
			if err != nil || !bytes.Equal(again, previous) {
				t.Fatal("native repair was applied more than once:", err)
			}
			for _, source := range [][]byte{append(bytes.Clone(current), '\n'), bytes.Replace(current, []byte("func "), []byte("func foreign_"), 1)} {
				if bytes.Equal(source, current) {
					t.Fatal("hostile source did not change")
				}
				got, err := Repair2698HistoricalSource(root, pin.File, source)
				if err == nil && !bytes.Equal(got, source) {
					t.Fatal("warm source cache certified changed current bytes")
				}
			}
			got, err := Repair2698HistoricalSource(root, pin.File+".foreign", current)
			if err != nil || !bytes.Equal(got, current) {
				t.Fatal("foreign source path received an inverse:", err)
			}
		})
	}
}

func TestRepair2698FunctionReadsCopiedSourceNotGitdirCheckout(t *testing.T) {
	root := filepath.Clean("../../../../..")
	const path = "internal/runtime/node_delivery_startup_recovery_test.go"
	const function = "TestPipelineCoordinatorRecoveryContinuesAfterCommittedDeadLetterParity"
	copy := t.TempDir()
	gitdir, err := exec.Command("git", "-C", root, "rev-parse", "--absolute-git-dir").Output()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(copy, ".git"), []byte("gitdir: "+strings.TrimSpace(string(gitdir))+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(root, path))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(filepath.Join(copy, path)), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(copy, path), raw, 0600); err != nil {
		t.Fatal(err)
	}
	actual, err := Body(raw, function)
	if err != nil {
		t.Fatal(err)
	}
	// The earlier packet is embedded; actual source always comes from this copy.
	prior, err := repair2698HistoricalFunction(copy, path, function, actual)
	if err != nil || prior == actual {
		t.Fatal("exact copied current source has no predecessor:", err)
	}
	hostile := bytes.Replace(raw, []byte("func "+function+"("), []byte("func "+function+"Foreign("), 1)
	if bytes.Equal(hostile, raw) {
		t.Fatal("copied-source mutation did not apply")
	}
	if err := os.WriteFile(filepath.Join(copy, path), hostile, 0600); err != nil {
		t.Fatal(err)
	}
	if got, err := repair2698HistoricalFunction(copy, path, function, actual); err == nil && got != actual {
		t.Fatal("pristine gitdir checkout hid changed copied source")
	}
	if err := os.Remove(filepath.Join(copy, path)); err != nil {
		t.Fatal(err)
	}
	if _, err := repair2698HistoricalFunction(copy, path, function, actual); err == nil {
		t.Fatal("warm source cache hid a missing actual sibling")
	}
}
