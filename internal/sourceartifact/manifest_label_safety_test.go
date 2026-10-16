package sourceartifact

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"unicode"
)

func Test2376ManifestUTF8ByteBoundaries(t *testing.T) {
	for _, field := range []string{"version", "platform_version"} {
		for _, size := range []int{255, 256, 257} {
			t.Run(fmt.Sprintf("%s/%d_bytes", field, size), func(t *testing.T) {
				version, constraint := "1.2.3", "*"
				switch field {
				case "version":
					version += "+" + strings.Repeat("a", size-len(version)-1)
				case "platform_version":
					constraint += strings.Repeat(" ", size-1)
				}
				body := fmt.Sprintf("version: %q\nplatform_version: %q\n", version, constraint)
				assert2376ManifestAdmission(t, body, field, size <= 256)
			})
		}
	}
}

func Test2376ManifestOversizeEvidencePrecedesSemanticParsing(t *testing.T) {
	oversize := strings.Repeat("\u00e9", 128) + "x"
	for _, row := range []struct {
		name, body, field string
		alias             bool
	}{
		{"invalid_version", fmt.Sprintf("version: %q\nplatform_version: '*'\n", oversize), "version", false},
		{"invalid_range", fmt.Sprintf("version: 1.2.3\nplatform_version: %q\n", oversize), "platform_version", false},
		{"alias", fmt.Sprintf("capabilities: &large %q\nplatform_version: *large\nversion: 1.2.3\n", oversize), "platform_version", true},
		{"merge", fmt.Sprintf("<<: {version: %q, platform_version: '*'}\n", oversize), "version", false},
	} {
		for _, label := range []string{"manifest.yaml", "child/manifest.yaml"} {
			t.Run(row.name+"/"+label, func(t *testing.T) {
				entries := []Entry{{label: "schema.yaml", disposition: DispositionDeclaration, body: []byte("description: root\n")}, {label: label, disposition: DispositionManifest, body: []byte(row.body)}}
				if strings.Contains(label, "/") {
					entries = append(entries, Entry{label: "child/schema.yaml", disposition: DispositionDeclaration, body: []byte("description: child\n")})
				}
				if row.alias {
					entries[0].label = "trigger.yaml"
					entries[0].body = []byte("event_name: {literal: raw}\n")
					entries[0].disposition = DispositionResource
					if strings.Contains(label, "/") {
						entries[0].label = "schema.yaml"
						entries[0].body = []byte("description: root\n")
						entries[0].disposition = DispositionDeclaration
						entries[2] = Entry{label: "child/trigger.yaml", disposition: DispositionResource, body: []byte("event_name: {literal: raw}\n")}
					}
				}
				sort.Slice(entries, func(i, j int) bool { return entries[i].label < entries[j].label })
				blob, err := encodeLogical(entries)
				if err != nil {
					t.Fatal(err)
				}
				for _, decode := range []bool{false, true} {
					t.Run(fmt.Sprintf("decode=%t", decode), func(t *testing.T) {
						if decode {
							_, err = DecodeLogical(blob)
						} else {
							_, err = newArtifact(entries)
						}
						if err == nil || !strings.Contains(err.Error(), `$["`+row.field+`"]`) || !strings.Contains(err.Error(), label+":") || !strings.Contains(err.Error(), "256 UTF-8 bytes") || strings.Contains(err.Error(), oversize) {
							t.Fatalf("missing bounded exact-path evidence: %v", err)
						}
						if row.alias && (!strings.Contains(err.Error(), label+":2:") || !strings.Contains(err.Error(), "resolved at "+label+":1:")) {
							t.Fatalf("alias introduction/resolution lost: %v", err)
						}
					})
				}
			})
		}
	}
}

func Test2376HumanLabelsRemoveEveryFormatControl(t *testing.T) {
	check := func(r rune) {
		if got := humanSourceLabel("left" + string(r) + "right"); got != "left right" {
			t.Errorf("format control U+%04X survived: %q", r, got)
		}
	}
	for _, interval := range unicode.Cf.R16 {
		for r := uint32(interval.Lo); r <= uint32(interval.Hi); r += uint32(interval.Stride) {
			check(rune(r))
		}
	}
	for _, interval := range unicode.Cf.R32 {
		for r := interval.Lo; r <= interval.Hi; r += interval.Stride {
			check(rune(r))
		}
	}
}

func Test2376FormatControlPresentationPreservesExactArtifact(t *testing.T) {
	document := "Shop\u202eReception\u2066\u200dDesk"
	body := []byte("version: 1.2.3\nplatform_version: '*'\n")
	root := filepath.Join(t.TempDir(), "shop")
	writeTestFile(t, root, "schema.yaml", "description: root\n")
	writeTestFile(t, root, "manifest.yaml", string(body))
	writeTestFile(t, root, "README.md", document)
	artifact, err := AdmitDirectory(root)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeLogical(artifact.LogicalBlob())
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(artifact.LogicalBlob())
	for _, source := range []*AdmittedSourceArtifact{artifact, decoded} {
		metadata, present := source.RootManifest()
		want := "shop@1.2.3"
		if source == decoded {
			want = ShortHashLabel(artifact.BundleHash())
		}
		if !present || metadata.Version != "1.2.3" || source.HumanLabel() != want {
			t.Fatalf("display/source metadata separation lost: metadata=%#v label=%q", metadata, source.HumanLabel())
		}
		if source.BundleHash() != HashPrefix+hex.EncodeToString(digest[:]) || !bytes.Equal(source.LogicalBlob(), artifact.LogicalBlob()) {
			t.Fatal("display sanitization rewrote source identity")
		}
		readme, present := source.Entry("README.md")
		if !present || string(readme.Bytes()) != document {
			t.Fatal("display sanitization rewrote inert document bytes")
		}
		for _, entry := range source.Entries() {
			if entry.Label() == "manifest.yaml" && !bytes.Equal(entry.Bytes(), body) {
				t.Fatal("display sanitization rewrote authored manifest bytes")
			}
		}
	}
}
