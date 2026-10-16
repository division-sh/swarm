package sourceartifact

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestS1ProviderKeyUsesExactCapturedDirectory(t *testing.T) {
	keys := []string{"team-bot", "team_bot", "9-team", strings.Repeat("a", 100), "a-" + strings.Repeat("b", 98)}
	members := map[string][]byte{"schema.yaml": []byte("description: root\n")}
	for _, key := range keys {
		members["child/"+key+"/manifest.yaml"] = []byte(s1ProviderManifest)
		members["child/"+key+"/trigger.yaml"] = []byte("event_name: {literal: raw}\n")
	}
	artifact, err := AdmitMembers(members)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeLogical(artifact.LogicalBlob())
	if err != nil {
		t.Fatal(err)
	}
	for _, source := range []*AdmittedSourceArtifact{artifact, decoded} {
		for _, key := range keys {
			if got, err := source.ProviderKey("child/" + key); err != nil || got != key {
				t.Fatalf("captured provider key changed: got=%q want=%q err=%v", got, key, err)
			}
		}
		for _, selector := range []string{"", ".", "child", "team-bot", "child/TEAM-BOT", "child/./team-bot", "child/team-bot/", "child/team-bot/trigger.yaml", "child/foreign", "child/ team-bot"} {
			if got, err := source.ProviderKey(selector); err == nil || got != "" {
				t.Fatalf("foreign or normalized selector gained a key: selector=%q key=%q err=%v", selector, got, err)
			}
		}
	}
	if got, err := (*AdmittedSourceArtifact)(nil).ProviderKey("team-bot"); err == nil || got != "" {
		t.Fatalf("nil source gained a provider key: %q %v", got, err)
	}
}

func TestS1ProviderRootKeyUsesFrozenPairWithoutChangingContent(t *testing.T) {
	parent := t.TempDir()
	var previous *AdmittedSourceArtifact
	for _, key := range []string{"team-bot", "team_bot"} {
		root := filepath.Join(parent, key)
		writeTestFile(t, root, "manifest.yaml", s1ProviderManifest)
		writeTestFile(t, root, "trigger.yaml", "event_name: {literal: raw}\n")
		artifact, err := AdmitDirectory(root)
		if err != nil {
			t.Fatal(err)
		}
		if previous != nil && (artifact.BundleHash() != previous.BundleHash() || !bytes.Equal(artifact.LogicalBlob(), previous.LogicalBlob())) {
			t.Fatal("provider key changed content identity or bytes")
		}
		previous = artifact
		if got, err := artifact.ProviderKey("."); err != nil || got != key {
			t.Fatalf("captured root key=%q want=%q err=%v", got, key, err)
		}
		rooted, err := artifact.RootedSource()
		if err != nil {
			t.Fatal(err)
		}
		row, err := PersistedFromArtifact(artifact, time.Now().UTC())
		if err != nil {
			t.Fatal(err)
		}
		retained, err := RestoreRootedSource(row, rooted.Admission().Reference())
		if err != nil {
			t.Fatal(err)
		}
		moved := filepath.Join(parent, "moved-"+key)
		if err := os.Rename(root, moved); err != nil {
			t.Fatal(err)
		}
		if err := os.RemoveAll(moved); err != nil {
			t.Fatal(err)
		}
		for _, source := range []RootedSource{rooted, retained} {
			if got, err := source.ProviderKey("."); err != nil || got != key {
				t.Fatalf("frozen provider key changed after source removal: key=%q want=%q err=%v", got, key, err)
			}
			if got, err := source.ProviderKey(key); err == nil || got != "" {
				t.Fatalf("root key became a second membership selector: %q %v", got, err)
			}
		}
		if got, err := retained.Artifact().ProviderKey("."); !errors.Is(err, ErrRootAdmissionMissing) || got != "" {
			t.Fatalf("retained view attached root evidence to shared content: %q %v", got, err)
		}
		decoded, err := row.Decode()
		if err != nil {
			t.Fatal(err)
		}
		if got, err := decoded.ProviderKey("."); !errors.Is(err, ErrRootAdmissionMissing) || got != "" {
			t.Fatalf("content-only provider invented a root key: %q %v", got, err)
		}
	}
}

func TestS1ProviderKeyRefusesInvalidRootedEvidenceBeforeNestedLookup(t *testing.T) {
	root := filepath.Join(t.TempDir(), "shop")
	writeTestFile(t, root, "schema.yaml", "description: root\n")
	writeTestFile(t, root, "team-bot/manifest.yaml", s1ProviderManifest)
	writeTestFile(t, root, "team-bot/trigger.yaml", "{}\n")
	artifact, err := AdmitDirectory(root)
	if err != nil {
		t.Fatal(err)
	}
	valid, err := artifact.RootedSource()
	if err != nil {
		t.Fatal(err)
	}
	if got, err := valid.ProviderKey("team-bot"); err != nil || got != "team-bot" {
		t.Fatalf("valid rooted view lost its nested key: %q %v", got, err)
	}
	for _, source := range []RootedSource{
		{},
		{artifact: artifact},
		{artifact: artifact, admission: RootAdmission{bundleHash: artifact.BundleHash(), rootDirectory: "other"}},
		{artifact: artifact, admission: RootAdmission{bundleHash: "bundle-v2:sha256:" + strings.Repeat("a", 64), rootDirectory: "shop"}},
		{artifact: artifact, admission: RootAdmission{bundleHash: artifact.BundleHash(), rootDirectory: "SHOP"}},
	} {
		if got, err := source.ProviderKey("team-bot"); err == nil || got != "" {
			t.Fatalf("invalid rooted view gained provider identity: %q %v", got, err)
		}
	}
}

func TestS1ProviderScopeKeepsDistinctDirectoryKeys(t *testing.T) {
	members := map[string][]byte{
		"schema.yaml":       []byte("description: root\n"),
		"child/schema.yaml": []byte("description: child\n"),
	}
	for _, directory := range []string{"team-bot", "team_bot", "child/team-bot"} {
		members[directory+"/manifest.yaml"] = []byte(s1ProviderManifest)
		members[directory+"/trigger.yaml"] = []byte("{}\n")
	}
	artifact, err := AdmitMembers(members)
	if err != nil {
		t.Fatal(err)
	}
	providers, err := artifact.ProviderDirectoriesInScope("child")
	if err != nil {
		t.Fatal(err)
	}
	var paths, keys []string
	for _, provider := range providers {
		key, err := artifact.ProviderKey(provider.Path())
		if err != nil {
			t.Fatal(err)
		}
		paths, keys = append(paths, provider.Path()), append(keys, key)
	}
	if !reflect.DeepEqual(paths, []string{"child/team-bot", "team_bot"}) || !reflect.DeepEqual(keys, []string{"team-bot", "team_bot"}) {
		t.Fatalf("nearest selection normalized distinct keys: paths=%v keys=%v", paths, keys)
	}
}
