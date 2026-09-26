// Package checkoutsource defines which filesystem entries belong to a checkout's
// live source and proof corpus. Callers retain ownership of their domain filters.
package checkoutsource

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// Member reports whether path belongs to checkoutRoot, including untracked
// files. A nested .git file or directory starts a foreign checkout.
func Member(checkoutRoot, path string) (bool, error) {
	root, _, parts, err := paths(checkoutRoot, path)
	if err != nil {
		return false, err
	}
	current := root
	for _, part := range parts {
		if part == ".git" {
			return false, nil
		}
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if err != nil {
			if os.IsNotExist(err) {
				return false, err
			}
			return false, fmt.Errorf("inspect checkout path %s: %w", current, err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return false, fmt.Errorf("checkout scan path %s is a symlink", current)
		}
		if info.IsDir() {
			foreign, err := hasGitMarker(current)
			if err != nil {
				return false, err
			}
			if foreign {
				return false, nil
			}
		}
	}
	return true, nil
}

// WalkDir walks the selected subroot, excluding foreign nested checkouts before
// the caller can interpret their files. A checkout root's own .git is allowed.
func WalkDir(checkoutRoot, scanRoot string, fn fs.WalkDirFunc) error {
	root, scan, _, err := paths(checkoutRoot, scanRoot)
	if err != nil {
		return err
	}
	member, err := Member(root, scan)
	if err != nil || !member {
		return err
	}
	return filepath.WalkDir(scanRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.Type()&os.ModeSymlink != 0 {
			if err := checkDiscoveredSymlink(path); err != nil {
				return err
			}
			return nil
		}
		if entry.Name() == ".git" {
			if entry.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		if entry.IsDir() && path != scanRoot {
			foreign, err := hasGitMarker(path)
			if err != nil {
				return err
			}
			if foreign {
				return fs.SkipDir
			}
		}
		return fn(path, entry, nil)
	})
}

// Walk is the os.FileInfo callback form for existing source-policy consumers.
func Walk(checkoutRoot, scanRoot string, fn filepath.WalkFunc) error {
	return WalkDir(checkoutRoot, scanRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		return fn(path, info, nil)
	})
}

// ReadDir lists only entries belonging to the current checkout. It supports
// hierarchical discovery without admitting a foreign tier or fixture directory.
func ReadDir(checkoutRoot, dir string) ([]os.DirEntry, error) {
	member, err := Member(checkoutRoot, dir)
	if err != nil || !member {
		return nil, err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	out := make([]os.DirEntry, 0, len(entries))
	for _, entry := range entries {
		if entry.Type()&os.ModeSymlink != 0 {
			if err := checkDiscoveredSymlink(filepath.Join(dir, entry.Name())); err != nil {
				return nil, err
			}
			continue
		}
		member, err := Member(checkoutRoot, filepath.Join(dir, entry.Name()))
		if err != nil {
			return nil, err
		}
		if member {
			out = append(out, entry)
		}
	}
	return out, nil
}

func checkDiscoveredSymlink(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("inspect checkout symlink %s: %w", path, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("checkout source symlink %s is not a directory", path)
	}
	return nil
}

func paths(checkoutRoot, path string) (string, string, []string, error) {
	root, err := filepath.Abs(checkoutRoot)
	if err != nil {
		return "", "", nil, err
	}
	target, err := filepath.Abs(path)
	if err != nil {
		return "", "", nil, err
	}
	info, err := os.Lstat(root)
	if err != nil {
		return "", "", nil, fmt.Errorf("inspect checkout root %s: %w", root, err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", "", nil, fmt.Errorf("checkout root %s is not a directory", root)
	}
	rel, err := filepath.Rel(root, target)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", "", nil, fmt.Errorf("scan path %s is outside checkout %s", target, root)
	}
	if rel == "." {
		return root, target, nil, nil
	}
	return root, target, strings.Split(rel, string(filepath.Separator)), nil
}

func hasGitMarker(dir string) (bool, error) {
	_, err := os.Lstat(filepath.Join(dir, ".git"))
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, fmt.Errorf("inspect checkout boundary %s: %w", dir, err)
}
