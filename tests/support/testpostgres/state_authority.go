package testpostgres

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

func validatePrivateStateRoot(root string) error {
	return filepath.WalkDir(root, func(path string, _ fs.DirEntry, walkErr error) error {
		return validatePrivateStatePath(root, path, walkErr)
	})
}

func validatePrivateStatePath(root, path string, walkErr error) error {
	if walkErr != nil {
		if path != root && os.IsNotExist(walkErr) {
			return nil
		}
		return walkErr
	}
	info, err := os.Lstat(path)
	if err != nil {
		if path != root && os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("Postgres service state authority %q is a symlink; refusing Docker mutation", path)
	}
	if !info.IsDir() && !info.Mode().IsRegular() {
		return fmt.Errorf("Postgres service state authority %q has unsupported type %s", path, info.Mode().Type())
	}
	return validatePrivateStateInfo(path, info)
}

func validatePrivateStateInfo(path string, info os.FileInfo) error {
	return validateStateAccess(path, info)
}

func validateExistingAuthorityFile(path string) error {
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return fmt.Errorf("Postgres service state authority %q is not a regular file", path)
	}
	return validatePrivateStateInfo(path, info)
}
