package selectedreadrole

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestSessionInventory2577ContinuationPreservesExactHistoryAndActualSource(t *testing.T) {
	root := filepath.Clean("../../../../..")
	packet, err := readSessionInventory2577Packet(root)
	if err != nil {
		t.Fatal(err)
	}
	actual, err := os.ReadFile(filepath.Join(root, SessionInventory2577Path))
	if err != nil {
		t.Fatal(err)
	}
	before, err := sessionInventory2577SignedSource(root, packet.BeforeCommit, packet.Files[0].BeforeSHA256)
	if err != nil || exactSessionInventory2577Selection(before, actual) != nil {
		t.Fatal("reviewed plan-selection delta differs:", err)
	}
	previous, err := Repair2698SuccessorHistoricalSource(root, SessionInventory2577Path, actual)
	if err != nil || !bytes.Equal(previous, before) {
		t.Fatal("exact one-file successor refused:", err)
	}
	history, err := readCatchUp2542Packet()
	if err != nil {
		t.Fatal(err)
	}
	lane, err := exec.Command("git", "-C", root, "show", history.Lane+":"+SessionInventory2577Path).Output()
	if err != nil {
		t.Fatal(err)
	}
	for repetition := 0; repetition < 2; repetition++ {
		got, err := CatchUp2542HistoricalSource(root, SessionInventory2577Path, actual)
		if err != nil || !bytes.Equal(got, lane) {
			t.Fatal("immutable catch-up lane source changed:", err)
		}
		const name = "configuredTelegramChannelBindingWithTextLimit"
		currentBody, err := Body(actual, name)
		if err != nil {
			t.Fatal(err)
		}
		wantBody, err := Body(lane, name)
		if err != nil {
			t.Fatal(err)
		}
		gotBody, err := CatchUp2542HistoricalFunction(root, SessionInventory2577Path, name, currentBody)
		if err != nil || gotBody != wantBody {
			t.Fatal("function history lost the existing catch-up owner:", err)
		}
	}
	current, err := os.ReadFile(filepath.Join(root, SessionInventory2577Path))
	if err != nil || !bytes.Equal(current, actual) {
		t.Fatal("historical observation changed live source:", err)
	}
	for _, changed := range [][]byte{append(bytes.Clone(actual), []byte("\nfunc extraSQL(db interface{ Exec(string) }) { db.Exec(\"SELECT 1\") }\n")...), bytes.Replace(actual, []byte("channelIdentity.ID() !="), []byte("channelIdentity.ID() =="), 1)} {
		if bytes.Equal(changed, actual) || exactSessionInventory2577Selection(before, changed) == nil {
			t.Fatal("extra SQL or changed assertion admitted as plan selection")
		}
	}
}

func TestSessionInventory2577ContinuationRefusesChangedCopiedInputs(t *testing.T) {
	root := filepath.Clean("../../../../..")
	actual, err := os.ReadFile(filepath.Join(root, SessionInventory2577Path))
	if err != nil {
		t.Fatal(err)
	}
	t.Run("current-source-and-warm-function", func(t *testing.T) {
		copy := sessionInventory2577TestCopy(t, root)
		const name = "configuredTelegramChannelBindingWithTextLimit"
		body, err := Body(actual, name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := CatchUp2542HistoricalFunction(copy, SessionInventory2577Path, name, body); err != nil {
			t.Fatal(err)
		}
		changed := append(bytes.Clone(actual), '\n')
		if err := os.WriteFile(filepath.Join(copy, SessionInventory2577Path), changed, 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := CatchUp2542HistoricalFunction(copy, SessionInventory2577Path, name, body); err == nil {
			t.Fatal("warm function cache certified changed copied source")
		}
		if _, err := Repair2698SuccessorHistoricalSource(copy, SessionInventory2577Path, actual); err == nil {
			t.Fatal("pristine gitdir substituted for changed copied source")
		}
		if err := os.Remove(filepath.Join(copy, SessionInventory2577Path)); err != nil {
			t.Fatal(err)
		}
		if _, err := Repair2698SuccessorHistoricalSource(copy, SessionInventory2577Path, actual); err == nil {
			t.Fatal("missing copied source received historical credit")
		}
	})
	for _, kind := range []string{"missing", "changed", "duplicate", "reverse", "foreign", "wrong-header", "wrong-hash"} {
		t.Run(kind, func(t *testing.T) {
			copy := sessionInventory2577TestCopy(t, root)
			path := filepath.Join(copy, SessionInventory2577PacketPath)
			if _, err := Repair2698SuccessorHistoricalSource(copy, SessionInventory2577Path, actual); err != nil {
				t.Fatal(err)
			}
			packet, err := readSessionInventory2577Packet(copy)
			if err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "missing":
				err = os.Remove(path)
			case "changed":
				err = os.WriteFile(path, append(bytes.Clone(sessionInventory2577Packet), '\n'), 0600)
			default:
				switch kind {
				case "duplicate":
					packet.Files = append(packet.Files, packet.Files[0])
				case "reverse":
					packet.BeforeCommit, packet.AfterCommit = packet.AfterCommit, packet.BeforeCommit
				case "foreign":
					packet.Files[0].File += ".foreign"
				case "wrong-header":
					packet.Authorization += ".foreign"
				case "wrong-hash":
					packet.Files[0].AfterSHA256 = strings.Repeat("0", 64)
				}
				var raw []byte
				raw, err = json.Marshal(packet)
				if err == nil {
					err = os.WriteFile(path, raw, 0600)
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := CatchUp2542HistoricalSource(copy, SessionInventory2577Path, actual); err == nil {
				t.Fatal("changed/missing/duplicate/reverse/foreign certificate admitted")
			}
		})
	}
	if got, err := Repair2698SuccessorHistoricalSource(root, SessionInventory2577Path+".foreign", actual); err != nil || !bytes.Equal(got, actual) {
		t.Fatal("foreign path received an inverse:", err)
	}
}

func sessionInventory2577TestCopy(t *testing.T, root string) string {
	t.Helper()
	copy := t.TempDir()
	gitdir, err := exec.Command("git", "-C", root, "rev-parse", "--absolute-git-dir").Output()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(copy, ".git"), []byte("gitdir: "+strings.TrimSpace(string(gitdir))+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{SessionInventory2577Path, SessionInventory2577PacketPath} {
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
	}
	return copy
}
