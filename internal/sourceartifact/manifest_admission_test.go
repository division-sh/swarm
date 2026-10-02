package sourceartifact

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

const manifest2376 = "name: Shop Reception\nversion: 1.2.3\nplatform_version: '>=0.7.0 <1.0.0'\n"

func Test2376ManifestHumanNamePreservesMetadataButNotTerminalControls(t *testing.T) {
	artifact, err := newArtifact([]Entry{
		{label: "schema.yaml", body: []byte("description: root\n")},
		{label: "manifest.yaml", body: []byte("name: \"Shop\\nReception\\u001b[0m\"\nversion: 1.2.3\nplatform_version: '*'\n")},
	})
	if err != nil {
		t.Fatal(err)
	}
	metadata, present := artifact.RootManifest()
	if !present || metadata.Name != "Shop\nReception\x1b[0m" {
		t.Fatalf("source metadata was normalized: %#v", metadata)
	}
	if label := artifact.HumanLabel(); strings.ContainsAny(label, "\n\r\x1b") || label != "Shop Reception [0m@1.2.3" {
		t.Fatalf("terminal control leaked into human label: %q", label)
	}
}

func assert2376ManifestAdmission(t *testing.T, body, field string, valid bool) {
	t.Helper()
	for _, label := range []string{"manifest.yaml", "child/manifest.yaml"} {
		t.Run(label, func(t *testing.T) {
			entries := []Entry{{label: "schema.yaml", disposition: DispositionDeclaration, body: []byte("description: root\n")}, {label: label, disposition: DispositionManifest, body: []byte(body)}}
			if strings.Contains(label, "/") {
				entries = append(entries, Entry{label: "child/schema.yaml", disposition: DispositionDeclaration, body: []byte("description: child\n")})
			}
			sort.Slice(entries, func(i, j int) bool { return entries[i].label < entries[j].label })
			blob, err := encodeLogical(entries)
			if err != nil {
				t.Fatal(err)
			}
			for _, consumer := range []struct {
				name string
				load func() (*AdmittedSourceArtifact, error)
			}{
				{"construct", func() (*AdmittedSourceArtifact, error) { return newArtifact(entries) }},
				{"decode", func() (*AdmittedSourceArtifact, error) { return DecodeLogical(blob) }},
			} {
				t.Run(consumer.name, func(t *testing.T) {
					_, err := consumer.load()
					if valid {
						if err != nil {
							t.Fatal(err)
						}
						return
					}
					if err == nil {
						t.Fatal("invalid flow manifest admitted")
					}
					if !strings.Contains(err.Error(), label) || field != "" && !strings.Contains(err.Error(), field) {
						t.Fatalf("error %q lacks exact manifest %q/field %q evidence", err, label, field)
					}
				})
			}
		})
	}
}

func Test2376ManifestOptionalStrictPresence(t *testing.T) {
	t.Run("absent", func(t *testing.T) {
		if _, err := newArtifact([]Entry{{label: "schema.yaml", body: []byte("description: root\n")}}); err != nil {
			t.Fatal(err)
		}
	})
	fields := []string{"name: Shop Reception\n", "version: 1.2.3\n", "platform_version: '>=0.7.0 <1.0.0'\n"}
	for mask := 0; mask < 8; mask++ {
		t.Run(fmt.Sprintf("presence_%03b", mask), func(t *testing.T) {
			body, missing := "", ""
			for i, field := range fields {
				if mask&(1<<i) != 0 {
					body += field
				} else if missing == "" {
					missing = strings.SplitN(field, ":", 2)[0]
				}
			}
			if body == "" {
				body = "{}\n"
			}
			assert2376ManifestAdmission(t, body, missing, mask == 7)
		})
	}
	for _, shape := range []string{"", "null\n", "''\n", "text\n", "[]\n", "[text]\n"} {
		t.Run("document_"+fmt.Sprintf("%q", shape), func(t *testing.T) {
			assert2376ManifestAdmission(t, shape, "", false)
		})
	}
	for _, field := range []string{"name", "version", "platform_version"} {
		for _, value := range []string{"", "null", "''", "[]", "[text]", "{}", "{value: text}", "true", "12"} {
			t.Run(field+"_"+fmt.Sprintf("%q", value), func(t *testing.T) {
				lines := strings.Split(manifest2376, "\n")
				for i, line := range lines {
					if strings.HasPrefix(line, field+":") {
						lines[i] = field + ": " + value
					}
				}
				assert2376ManifestAdmission(t, strings.Join(lines, "\n"), field, false)
			})
		}
	}
}

func Test2376ManifestUnknownKeysAndDuplicates(t *testing.T) {
	for _, field := range []string{"description", "requires", "publisher", "unknown"} {
		t.Run(field, func(t *testing.T) {
			assert2376ManifestAdmission(t, manifest2376+field+": retired\n", field, false)
		})
	}
	for _, field := range []string{"name", "version", "platform_version"} {
		t.Run("duplicate_"+field, func(t *testing.T) {
			assert2376ManifestAdmission(t, manifest2376+field+": duplicate\n", field, false)
		})
	}
}

func Test2376ManifestMalformedDocument(t *testing.T) {
	for _, body := range []string{"name: [unterminated\n", manifest2376 + "---\n" + manifest2376} {
		t.Run(fmt.Sprintf("%q", body), func(t *testing.T) {
			assert2376ManifestAdmission(t, body, "", false)
		})
	}
}

func Test2376ManifestStrictVersion(t *testing.T) {
	for _, version := range []string{"0.0.0", "1.2.3", "1.2.3-alpha.1+build.2", "1", "1.2", "v1.2.3", "01.2.3", " 1.2.3 ", "not-a-version"} {
		t.Run(version, func(t *testing.T) {
			body := strings.Replace(manifest2376, "version: 1.2.3", fmt.Sprintf("version: %q", version), 1)
			valid := version == "0.0.0" || version == "1.2.3" || version == "1.2.3-alpha.1+build.2"
			assert2376ManifestAdmission(t, body, "version", valid)
		})
	}
}

func Test2376ManifestRangeGrammar(t *testing.T) {
	for _, constraint := range []string{"*", ">=0.7.0 <1.0.0", "^0.7.0", "not-a-range"} {
		t.Run(constraint, func(t *testing.T) {
			body := strings.Replace(manifest2376, "'>=0.7.0 <1.0.0'", fmt.Sprintf("%q", constraint), 1)
			assert2376ManifestAdmission(t, body, "platform_version", constraint != "not-a-range")
		})
	}
}

func Test2376ManifestAliasMergeProvenance(t *testing.T) {
	assert2376ManifestAdmission(t, "name: &name Reception\nversion: 1.2.3\nplatform_version: '*'\n", "", true)
	assert2376ManifestAdmission(t, "<<: {name: Reception, version: 1.2.3, platform_version: '*'}\n", "", true)
	assert2376ManifestAdmission(t, manifest2376+"<<: {version: 1.2.3}\n", "version", false)
	_, err := newArtifact([]Entry{
		{label: "schema.yaml", body: []byte("description: root\n")},
		{label: "manifest.yaml", body: []byte("name: &range 'not-a-range'\nversion: 1.2.3\nplatform_version: *range\n")},
	})
	if err == nil || !strings.Contains(err.Error(), `$["platform_version"]`) || !strings.Contains(err.Error(), "manifest.yaml:3:") || !strings.Contains(err.Error(), "manifest.yaml:1:") {
		t.Fatalf("alias introduction and target evidence missing: %v", err)
	}
}

func Test2376ManifestDispositionAndSelectedRootCompatibility(t *testing.T) {
	artifact, err := newArtifact([]Entry{
		{label: "schema.yaml", body: []byte("description: root\n")},
		{label: "manifest.yaml", body: []byte(manifest2376)},
		{label: "child/schema.yaml", body: []byte("description: child\n")},
		{label: "child/manifest.yaml", body: []byte("name: Child\nversion: 2.0.0\nplatform_version: '>=9.0.0'\n")},
		{label: "docs/manifest.yaml", body: []byte("[malformed\n")},
		{label: "packs/manifest.yaml", body: []byte("description: pack resource\n")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := artifact.ValidatePlatformVersion("0.7.0"); err != nil {
		t.Fatal(err)
	}
	if err := artifact.ValidatePlatformVersion("2.0.0"); err == nil || !strings.Contains(err.Error(), `$["platform_version"]`) {
		t.Fatalf("root range not enforced: %v", err)
	}
	if _, present := artifact.Manifest("packs/manifest.yaml"); present {
		t.Fatal("resource classified as flow manifest")
	}
	if _, present := artifact.Manifest("docs/manifest.yaml"); present {
		t.Fatal("document classified as flow manifest")
	}
	child, err := newArtifact([]Entry{{label: "schema.yaml", body: []byte("description: child\n")}, {label: "manifest.yaml", body: []byte("name: Child\nversion: 2.0.0\nplatform_version: '>=9.0.0'\n")}})
	if err != nil {
		t.Fatal(err)
	}
	if child.ValidatePlatformVersion("0.7.0") == nil {
		t.Fatal("selected child root ignored its own range")
	}
}

func Test2376StoredSourcePresentationHasNoInventedRoot(t *testing.T) {
	for _, named := range []bool{false, true} {
		t.Run(fmt.Sprint(named), func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "reception")
			if err := os.Mkdir(root, 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, "schema.yaml"), []byte("description: root\n"), 0600); err != nil {
				t.Fatal(err)
			}
			if named {
				if err := os.WriteFile(filepath.Join(root, "manifest.yaml"), []byte(manifest2376), 0600); err != nil {
					t.Fatal(err)
				}
			}
			artifact, err := AdmitDirectory(root)
			if err != nil {
				t.Fatal(err)
			}
			short := strings.TrimPrefix(artifact.BundleHash(), HashPrefix)[:7]
			want := "reception@" + short
			if named {
				want = "Shop Reception@1.2.3"
			}
			if artifact.HumanLabel() != want {
				t.Fatalf("local label %q, want %q", artifact.HumanLabel(), want)
			}
			if err := os.RemoveAll(root); err != nil {
				t.Fatal(err)
			}
			stored, err := DecodeLogical(artifact.LogicalBlob())
			if err != nil {
				t.Fatal(err)
			}
			if !named {
				want = short
			}
			if stored.HumanLabel() != want {
				t.Fatalf("stored label %q, want %q", stored.HumanLabel(), want)
			}
			if artifact.BundleHash() != stored.BundleHash() {
				t.Fatal("presentation changed exact identity")
			}
		})
	}
}
