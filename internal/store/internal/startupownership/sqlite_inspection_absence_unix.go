//go:build darwin || linux

package startupownership

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// ObserveSQLiteInspectionAbsence distinguishes a missing coordinate from an
// unsafe existing path without opening, creating or acquiring any resource.
func ObserveSQLiteInspectionAbsence(ctx context.Context, path string) (bool, error) {
	_, absent, err := inspectSQLiteCoordinate(ctx, path)
	return absent, err
}

func inspectSQLiteCoordinate(ctx context.Context, path string) (string, bool, error) {
	if err := ctx.Err(); err != nil {
		return "", false, err
	}
	if strings.TrimSpace(path) == "" {
		return "", false, errors.New("SQLite selected-store path is required")
	}
	abs, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		return "", false, err
	}
	coordinate := abs
	current := string(filepath.Separator)
	parts := strings.Split(strings.TrimPrefix(abs, current), string(filepath.Separator))
	for index, part := range parts {
		if err := ctx.Err(); err != nil {
			return "", false, err
		}
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if errors.Is(err, os.ErrNotExist) {
			return coordinate, true, nil
		}
		if err != nil {
			return "", false, fmt.Errorf("inspect SQLite selected-store coordinate %q: %w", abs, err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			resolved, err := filepath.EvalSymlinks(current)
			if err != nil {
				return "", false, sqlitePriorOwnerAmbiguous("SQLite selected-store ancestor alias cannot prove a coordinate")
			}
			canonical := filepath.Join(append([]string{resolved}, parts[index+1:]...)...)
			if index == len(parts)-1 || !systemCanonicalPathAlias(abs, canonical) {
				return "", false, sqlitePriorOwnerAmbiguous("SQLite selected-store aliases are not ownership authority; select its canonical path")
			}
			coordinate = canonical
			info, err = os.Stat(current)
			if err != nil {
				return "", false, err
			}
		}
		if index < len(parts)-1 && !info.IsDir() {
			return "", false, fmt.Errorf("SQLite selected-store ancestor %q is not a directory", current)
		}
	}
	return coordinate, false, nil
}
