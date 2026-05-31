package contracts

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestBuildBundleRegistrationDirectoryUploadPackagesTextAndData(t *testing.T) {
	repo := repoRootForContractsTest(t)
	root := t.TempDir()
	writeBundleRegistrationUploadFixture(t, root)
	writeBundleHashText(t, filepath.Join(root, ".DS_Store"), "ignored")
	writeBundleHashText(t, filepath.Join(root, "prompts", ".#ignored.md"), "ignored")

	upload, err := BuildBundleRegistrationDirectoryUpload(repo, root, DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatalf("BuildBundleRegistrationDirectoryUpload: %v", err)
	}

	var envelope bundleRegistrationEnvelopeUploadV1
	if err := yaml.Unmarshal([]byte(upload.ContentYAML), &envelope); err != nil {
		t.Fatalf("unmarshal content_yaml: %v\n%s", err, upload.ContentYAML)
	}
	if envelope.APIVersion != bundleRegistrationEnvelopeAPIVersion {
		t.Fatalf("api_version = %q, want %q", envelope.APIVersion, bundleRegistrationEnvelopeAPIVersion)
	}
	var paths []string
	for _, file := range envelope.Files {
		paths = append(paths, file.Path)
		if strings.Contains(file.Text, "ignored") {
			t.Fatalf("ignored content leaked through %s", file.Path)
		}
	}
	wantPaths := []string{
		"flows/alpha/schema.yaml",
		"package.yaml",
		"prompts/root.md",
	}
	if !reflect.DeepEqual(paths, wantPaths) {
		t.Fatalf("files = %#v, want %#v\n%s", paths, wantPaths, upload.ContentYAML)
	}
	if upload.DataBlob == nil {
		t.Fatal("DataBlob = nil, want one data entry")
	}
	if upload.DataBlob.APIVersion != bundleRegistrationDataAPIVersion {
		t.Fatalf("data api_version = %q, want %q", upload.DataBlob.APIVersion, bundleRegistrationDataAPIVersion)
	}
	if got, want := len(upload.DataBlob.Entries), 1; got != want {
		t.Fatalf("data entries = %d, want %d", got, want)
	}
	entry := upload.DataBlob.Entries[0]
	if entry.Path != "flows/alpha/data/payload.bin" {
		t.Fatalf("data path = %q", entry.Path)
	}
	if want := base64.StdEncoding.EncodeToString([]byte{0x01, 0x02, 0x03}); entry.DataBase64 != want {
		t.Fatalf("data_base64 = %q, want %q", entry.DataBase64, want)
	}
}

func TestBuildBundleRegistrationDirectoryUploadFailsClosed(t *testing.T) {
	repo := repoRootForContractsTest(t)
	for _, tc := range []struct {
		name       string
		mutate     func(t *testing.T, root string)
		wantErrSub string
	}{
		{
			name: "symlink",
			mutate: func(t *testing.T, root string) {
				target := filepath.Join(root, "prompts", "root.md")
				link := filepath.Join(root, "prompts", "link.md")
				if err := os.Symlink(target, link); err != nil {
					t.Skipf("symlink unsupported: %v", err)
				}
			},
			wantErrSub: "symlink",
		},
		{
			name: "ascii case collision",
			mutate: func(t *testing.T, root string) {
				writeBundleHashText(t, filepath.Join(root, "prompts", "Root.md"), "collision\n")
				lower, errLower := os.Stat(filepath.Join(root, "prompts", "root.md"))
				upper, errUpper := os.Stat(filepath.Join(root, "prompts", "Root.md"))
				if errLower == nil && errUpper == nil && os.SameFile(lower, upper) {
					t.Skip("case-insensitive filesystem cannot represent ASCII case collision fixture")
				}
			},
			wantErrSub: "case-colliding",
		},
		{
			name: "invalid prompt utf8",
			mutate: func(t *testing.T, root string) {
				writeBundleHashBytes(t, filepath.Join(root, "prompts", "bad.md"), []byte{0xff})
			},
			wantErrSub: "not valid UTF-8",
		},
		{
			name: "empty raw data",
			mutate: func(t *testing.T, root string) {
				writeBundleHashBytes(t, filepath.Join(root, "flows", "alpha", "data", "empty.bin"), nil)
			},
			wantErrSub: "cannot be represented in BundleRegisterDataBlobV1",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			writeBundleRegistrationUploadFixture(t, root)
			tc.mutate(t, root)

			_, err := BuildBundleRegistrationDirectoryUpload(repo, root, DefaultPlatformSpecFile(repo))
			if err == nil || !strings.Contains(err.Error(), tc.wantErrSub) {
				t.Fatalf("error = %v, want substring %q", err, tc.wantErrSub)
			}
		})
	}
}

func writeBundleRegistrationUploadFixture(t *testing.T, root string) {
	t.Helper()
	writeBundleHashText(t, filepath.Join(root, "package.yaml"), `
name: upload-fixture
version: "1.0.0"
flows:
  - id: alpha
    flow: alpha
`)
	writeBundleHashText(t, filepath.Join(root, "flows", "alpha", "schema.yaml"), `
initial_state: start
states:
  - start
  - done
`)
	writeBundleHashText(t, filepath.Join(root, "prompts", "root.md"), "root prompt\n")
	writeBundleHashBytes(t, filepath.Join(root, "flows", "alpha", "data", "payload.bin"), []byte{0x01, 0x02, 0x03})
}
