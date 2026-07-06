package contracts

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func ResolvePolicyModulePath(bundle *WorkflowContractBundle, module PolicyModule) (string, error) {
	if bundle == nil {
		return "", fmt.Errorf("workflow contract bundle is required")
	}
	root := strings.TrimSpace(bundle.Paths.ContractsRoot)
	if root == "" {
		return "", fmt.Errorf("contracts root is required for compute_module bytes")
	}
	modulePath := strings.TrimSpace(module.Path)
	if modulePath == "" {
		return "", fmt.Errorf("module path is required")
	}
	if filepath.IsAbs(modulePath) {
		return "", fmt.Errorf("module path %q must be relative to the contracts root", modulePath)
	}
	clean := filepath.Clean(filepath.FromSlash(modulePath))
	if clean == "." || clean == "" || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("module path %q must remain inside the contracts root", modulePath)
	}
	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	pathAbs, err := filepath.Abs(filepath.Join(rootAbs, clean))
	if err != nil {
		return "", err
	}
	rel, err := filepath.Rel(rootAbs, pathAbs)
	if err != nil {
		return "", err
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("module path %q must remain inside the contracts root", modulePath)
	}
	stat, err := lstatNoSymlinkPath(rootAbs, rel, modulePath)
	if err != nil {
		return "", fmt.Errorf("module path %q: %w", modulePath, err)
	}
	if !stat.Mode().IsRegular() {
		return "", fmt.Errorf("module path %q must be a regular file", modulePath)
	}
	return pathAbs, nil
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

func PolicyModuleBytes(bundle *WorkflowContractBundle, module PolicyModule) ([]byte, string, error) {
	path, err := ResolvePolicyModulePath(bundle, module)
	if err != nil {
		return nil, "", err
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, "", err
	}
	return raw, path, nil
}
