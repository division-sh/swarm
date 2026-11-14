package startupownership

import (
	"context"
	"errors"
	"os"
	"path/filepath"
)

// SQLiteInspectionInput is a non-authoritative observation. Only the existing
// construction possession can make its later comparison exclude native writers.
type SQLiteInspectionInput struct {
	path string
	file os.FileInfo
}

func CaptureSQLiteInspectionInput(ctx context.Context, path string) (*SQLiteInspectionInput, error) {
	absent, err := ObserveSQLiteInspectionAbsence(ctx, path)
	if err != nil {
		return nil, err
	}
	path, err = filepath.Abs(filepath.Clean(path))
	if err != nil {
		return nil, err
	}
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) && absent {
		return &SQLiteInspectionInput{path: path}, nil
	}
	if err != nil {
		return nil, err
	}
	if absent {
		return nil, errors.New("selected store changed during inspection")
	}
	return &SQLiteInspectionInput{path: path, file: info}, nil
}

func (i *SQLiteInspectionInput) Absent() bool { return i != nil && i.file == nil }

func (i *SQLiteInspectionInput) Revalidate(ctx context.Context, path string) error {
	current, err := CaptureSQLiteInspectionInput(ctx, path)
	if err != nil {
		return err
	}
	if i == nil || current.path != i.path || (current.file == nil) != (i.file == nil) || i.file != nil && !os.SameFile(current.file, i.file) {
		return errors.New("selected store changed after boot checks")
	}
	return nil
}
