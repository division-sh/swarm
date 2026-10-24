package packartifact

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type admittedArtifactRoot struct {
	directory *os.File
}

func (r *admittedArtifactRoot) openCapturedDirectory(relative string) (*admittedArtifactRoot, error) {
	directory, err := r.openDirectory(relative)
	if err != nil {
		return nil, err
	}
	return &admittedArtifactRoot{directory: directory}, nil
}

func artifactPathSegments(relative string) ([]string, error) {
	relative = filepath.Clean(filepath.FromSlash(strings.TrimSpace(relative)))
	if relative == "." {
		return nil, nil
	}
	if relative == "" || !filepath.IsLocal(relative) || filepath.VolumeName(relative) != "" {
		return nil, fmt.Errorf("artifact path %q must be local", relative)
	}
	segments := strings.Split(relative, string(filepath.Separator))
	for _, segment := range segments {
		if segment == "" || segment == "." || segment == ".." {
			return nil, fmt.Errorf("artifact path %q is not canonical", relative)
		}
	}
	return segments, nil
}

func (r *admittedArtifactRoot) info() (os.FileInfo, error) {
	directory, err := r.openDirectory(".")
	if err != nil {
		return nil, err
	}
	defer directory.Close()
	return directory.Stat()
}
