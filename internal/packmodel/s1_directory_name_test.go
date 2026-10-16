package packmodel

import (
	"strings"
	"testing"
)

func TestS1ManifestDirectoryOwnsName(t *testing.T) {
	for _, kind := range []DirectoryKind{DirectoryFlow, DirectoryProvider} {
		t.Run(kind.String(), func(t *testing.T) {
			body := "version: 1.2.3\nplatform_version: '*'\n"
			if kind == DirectoryProvider {
				body += s1ManifestCapabilities
			}
			manifest, err := ParseManifestAt([]byte(body), "manifest.yaml", kind)
			if err != nil || manifest.Kind() != kind {
				t.Fatalf("directory-only manifest refused: %+v, %v", manifest, err)
			}
			for _, declaration := range []string{"name: ignored\n", "name: null\n", "name: {}\n", "<<: {name: ignored}\n"} {
				_, err := ParseManifestAt([]byte(body+declaration), "manifest.yaml", kind)
				if err == nil || !strings.Contains(err.Error(), "name is not a manifest key; the directory is the name") || !strings.Contains(err.Error(), "manifest.yaml:") {
					t.Fatalf("authored name gained authority or lost teaching: %v", err)
				}
			}
		})
	}
}
