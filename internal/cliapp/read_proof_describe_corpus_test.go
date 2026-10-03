package cliapp

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDescribeProofCorpusAdmission(t *testing.T) {
	for _, mutation := range []string{"none", "missing", "extra", "extra_directory", "symlink"} {
		t.Run(mutation, func(t *testing.T) {
			root := t.TempDir()
			var first string
			for _, fixture := range describeProofFixtures {
				if err := os.Mkdir(filepath.Join(root, fixture.name), 0700); err != nil {
					t.Fatal(err)
				}
				for _, surface := range describeProofSurfaces {
					path := filepath.Join(root, fixture.name, surface.name+".golden")
					if first == "" {
						first = path
					}
					if err := os.WriteFile(path, []byte("fixture\n"), 0600); err != nil {
						t.Fatal(err)
					}
				}
			}
			switch mutation {
			case "missing", "symlink":
				if err := os.Remove(first); err != nil {
					t.Fatal(err)
				}
				if mutation == "symlink" {
					if err := os.Symlink("describe-quiet.golden", first); err != nil {
						t.Fatal(err)
					}
				}
			case "extra":
				if err := os.WriteFile(filepath.Join(root, "extra.golden"), nil, 0600); err != nil {
					t.Fatal(err)
				}
			case "extra_directory":
				if err := os.Mkdir(filepath.Join(root, "extra"), 0700); err != nil {
					t.Fatal(err)
				}
			}
			if err := validateDescribeProofCorpus(root); (err == nil) != (mutation == "none") {
				t.Fatalf("%s corpus admission: %v", mutation, err)
			}
		})
	}
}
