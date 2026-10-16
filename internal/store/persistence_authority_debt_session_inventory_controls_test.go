package store_test

import (
	"bytes"
	"encoding/json"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const debtSessionInventoryCollectorTo = "pending-exact-collector-digest"

func TestPersistenceAuthorityDebtSessionInventoryPreservesExactSource(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	packet, err := debtSessionInventoryReadPacket(root)
	if err != nil {
		t.Fatal(err)
	}
	actual, err := os.ReadFile(filepath.Join(root, debtSessionInventorySource))
	if err != nil {
		t.Fatal(err)
	}
	before, err := debtRepair2698SignedSource(root, packet.BeforeCommit, debtSessionInventorySource, packet.Files[0].BeforeSHA256)
	if err != nil || debtSessionInventoryExactSelection(before, actual) != nil {
		t.Fatal("reviewed selection delta changed:", err)
	}
	for repetition := 0; repetition < 2; repetition++ {
		previous, err := debtRepair2698SuccessorHistoricalSource(root, debtSessionInventorySource, actual)
		if err != nil || !bytes.Equal(previous, before) {
			t.Fatal("exact current source lost its historical predecessor:", err)
		}
		packet, err := debtCatchUpReadPacket(root)
		if err != nil {
			t.Fatal(err)
		}
		lane, err := debtGit(root, "show", packet.Lane+":"+debtSessionInventorySource)
		if err != nil {
			t.Fatal(err)
		}
		previous, err = debtCatchUpLaneSource(root, debtSessionInventorySource, actual)
		if err != nil || !bytes.Equal(previous, lane) {
			t.Fatal("existing catch-up historical cut changed:", err)
		}
	}
	for _, changed := range [][]byte{append(bytes.Clone(actual), []byte("\nfunc extraSQL(db interface{ Exec(string) }) { db.Exec(\"SELECT 1\") }\n")...), bytes.Replace(actual, []byte("channelIdentity.ID() !="), []byte("channelIdentity.ID() =="), 1)} {
		if bytes.Equal(changed, actual) || debtSessionInventoryExactSelection(before, changed) == nil {
			t.Fatal("extra SQL or changed assertion certified as plan selection")
		}
	}
	current, err := os.ReadFile(filepath.Join(root, debtSessionInventorySource))
	if err != nil || !bytes.Equal(current, actual) {
		t.Fatal("historical preservation changed actual live source:", err)
	}
}

func TestPersistenceAuthorityDebtSessionInventoryRefusesChangedCopiedInputs(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	actual, err := os.ReadFile(filepath.Join(root, debtSessionInventorySource))
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"missing-packet", "changed-packet", "duplicate", "reverse", "foreign", "wrong-header", "wrong-hash", "changed-source", "missing-source"} {
		t.Run(kind, func(t *testing.T) {
			copy := debtSessionInventoryTestCopy(t, root)
			if _, err := debtRepair2698SuccessorHistoricalSource(copy, debtSessionInventorySource, actual); err != nil {
				t.Fatal(err)
			}
			packet, err := debtSessionInventoryReadPacket(copy)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(copy, debtSessionInventoryPacketPath)
			switch kind {
			case "missing-packet":
				err = os.Remove(path)
			case "changed-packet":
				var raw []byte
				raw, err = os.ReadFile(path)
				if err == nil {
					err = os.WriteFile(path, append(raw, '\n'), 0600)
				}
			case "changed-source":
				err = os.WriteFile(filepath.Join(copy, debtSessionInventorySource), append(bytes.Clone(actual), '\n'), 0600)
			case "missing-source":
				err = os.Remove(filepath.Join(copy, debtSessionInventorySource))
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
			if _, err := debtCatchUpLaneSource(copy, debtSessionInventorySource, actual); err == nil {
				t.Fatal("changed certificate/copied source received historical credit")
			}
		})
	}
	if got, err := debtRepair2698SuccessorHistoricalSource(root, debtSessionInventorySource+".foreign", actual); err != nil || !bytes.Equal(got, actual) {
		t.Fatal("foreign source received an inverse:", err)
	}
	t.Run("active-policy-cannot-use-unmarked-predecessor", func(t *testing.T) {
		copy := debtSessionInventoryTestCopy(t, root)
		packet, err := debtSessionInventoryReadPacket(copy)
		if err != nil {
			t.Fatal(err)
		}
		before, err := debtRepair2698SignedSource(copy, packet.BeforeCommit, debtSessionInventorySource, packet.Files[0].BeforeSHA256)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(copy, debtSessionInventorySource), before, 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(filepath.Join(copy, debtSessionInventoryPacketPath)); err != nil {
			t.Fatal(err)
		}
		if err := debtSessionInventorySourcePolicy(copy); err == nil {
			t.Fatal("active continuation accepted missing packet and predecessor source")
		}
	})
}

func TestPersistenceAuthorityDebtSessionInventoryCollectorIsFiniteAndForward(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	if actual, err := debtCollectorDigest(root); err != nil || actual != debtSessionInventoryCollectorTo {
		t.Fatalf("session inventory collector=%s expected=%s: %v", actual, debtSessionInventoryCollectorTo, err)
	}
	trusted, _, err := debtCatchUpSignedBaseline(root, debtSessionInventoryBefore, debtRepair2698Round2CollectorTo)
	if err != nil {
		t.Fatal(err)
	}
	head := trusted
	head.Collector = debtSessionInventoryCollectorTo
	proof := debtDeliveryRetirementProof{Root: root, Actual: maps.Clone(trusted.Sites)}
	if !debtSessionInventoryCollectorTransition(trusted.Collector, head.Collector, trusted, head, proof) {
		t.Fatal("exact unchanged-debt forward edge refused")
	}
	if debtSessionInventoryCollectorTransition(head.Collector, trusted.Collector, head, trusted, proof) ||
		debtSessionInventoryCollectorTransition(trusted.Collector, head.Collector, trusted, head, proof, proof) {
		t.Fatal("reverse or duplicate proof admitted")
	}
	for _, kind := range []string{"increase", "relocation"} {
		t.Run(kind, func(t *testing.T) {
			changed := maps.Clone(trusted.Sites)
			for key, site := range changed {
				if kind == "increase" {
					site.Multiplicity++
					changed[key] = site
				} else {
					delete(changed, key)
					site.File += ".foreign"
					changed[site.identity()] = site
				}
				break
			}
			candidate := head
			candidate.Sites = changed
			changedProof := debtDeliveryRetirementProof{Root: root, Actual: changed}
			if debtSessionInventoryCollectorTransition(trusted.Collector, candidate.Collector, trusted, candidate, changedProof) {
				t.Fatal("count increase or same-count relocation admitted")
			}
		})
	}
}

func debtSessionInventoryTestCopy(t *testing.T, root string) string {
	t.Helper()
	copy := t.TempDir()
	gitdir, err := debtGit(root, "rev-parse", "--absolute-git-dir")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(copy, ".git"), []byte("gitdir: "+strings.TrimSpace(string(gitdir))+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{debtSessionInventorySource, debtSessionInventoryPacketPath} {
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
