package contracts

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
)

func ResolvePolicyModulePath(bundle *WorkflowContractBundle, module PolicyModule) (string, error) {
	if bundle == nil {
		return "", fmt.Errorf("workflow contract bundle is required")
	}
	modulePath := strings.TrimSpace(module.Path)
	if modulePath == "" {
		return "", fmt.Errorf("module path is required")
	}
	if modulePath != path.Clean(modulePath) || strings.HasPrefix(modulePath, "/") || strings.Contains(modulePath, `\`) || modulePath == "." || modulePath == ".." || strings.HasPrefix(modulePath, "../") {
		return "", fmt.Errorf("module path %q must be one canonical selected-root-relative source label", modulePath)
	}
	if bundle.SourceArtifact == nil {
		return "", fmt.Errorf("admitted source artifact is required for compute_module bytes")
	}
	if _, ok := bundle.SourceArtifact.Entry(modulePath); !ok {
		return "", fmt.Errorf("module path %q is not an admitted source member", modulePath)
	}
	return modulePath, nil
}

func PolicyModuleBytes(bundle *WorkflowContractBundle, module PolicyModule) ([]byte, string, error) {
	path, err := ResolvePolicyModulePath(bundle, module)
	if err != nil {
		return nil, "", err
	}
	entry, ok := bundle.SourceArtifact.Entry(path)
	if !ok {
		return nil, "", fmt.Errorf("module path %q is not an admitted source member", path)
	}
	return entry.Bytes(), path, nil
}

func lstatNoSymlinkPath(rootAbs, rel, modulePath string) (os.FileInfo, error) {
	current := rootAbs
	var last os.FileInfo
	for _, segment := range strings.Split(rel, string(filepath.Separator)) {
		if segment == "" || segment == "." {
			continue
		}
		current = filepath.Join(current, segment)
		info, err := os.Lstat(current)
		if err != nil {
			return nil, err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return nil, fmt.Errorf("symlinks are not allowed in module path %q", modulePath)
		}
		last = info
	}
	if last == nil {
		return nil, fmt.Errorf("module path is required")
	}
	return last, nil
}
