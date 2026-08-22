package packartifact

import (
	"fmt"
	"path/filepath"
	"strings"
)

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
