package startupownership

import (
	"context"
	"errors"
	"os"
)

// SQLiteInspectionInput is a non-authoritative observation. Only the existing
// construction possession can make its later comparison exclude native writers.
type SQLiteInspectionInput struct {
	path       string
	file       os.FileInfo
	coordinate os.FileInfo
}

func CaptureSQLiteInspectionInput(ctx context.Context, path string) (*SQLiteInspectionInput, error) {
	path, absent, err := inspectSQLiteCoordinate(ctx, path)
	if err != nil {
		return nil, err
	}
	coordinate, err := captureSQLiteInspectionCoordinate(path)
	if err != nil {
		return nil, err
	}
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) && absent {
		return &SQLiteInspectionInput{path: path, coordinate: coordinate}, nil
	}
	if err != nil {
		return nil, err
	}
	if absent {
		return nil, errors.New("selected store changed during inspection")
	}
	return &SQLiteInspectionInput{path: path, file: info, coordinate: coordinate}, nil
}

func (i *SQLiteInspectionInput) Absent() bool { return i != nil && i.file == nil }

func (i *SQLiteInspectionInput) Revalidate(ctx context.Context, path string) error {
	current, err := CaptureSQLiteInspectionInput(ctx, path)
	if err != nil {
		return err
	}
	if i == nil || current.path != i.path || !sameSQLiteInspectionFile(current.file, i.file) || !sameSQLiteInspectionFile(current.coordinate, i.coordinate) {
		return errors.New("selected store changed after boot checks")
	}
	return nil
}

func sameSQLiteInspectionFile(left, right os.FileInfo) bool {
	return left == nil && right == nil || left != nil && right != nil && os.SameFile(left, right)
}
