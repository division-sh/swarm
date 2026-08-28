package cliapp

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// InvocationRoot is the canonical working directory captured at the shipped
// CLI boundary. Commands may project paths from it but may not rediscover it.
type InvocationRoot struct {
	path string
}

func captureInvocationRoot() (InvocationRoot, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return InvocationRoot{}, fmt.Errorf("read process working directory: %w", err)
	}
	return NewInvocationRoot(cwd)
}

// NewInvocationRoot validates and canonicalizes an already-owned absolute
// invocation coordinate. The shipped CLI obtains that coordinate only from
// captureInvocationRoot; direct runtime tests use this constructor without
// mutating process-wide cwd.
func NewInvocationRoot(raw string) (InvocationRoot, error) {
	path, err := requireInvocationRootPath(raw)
	if err != nil {
		return InvocationRoot{}, err
	}
	canonical, err := filepath.EvalSymlinks(path)
	if err != nil {
		return InvocationRoot{}, fmt.Errorf("canonicalize invocation root %s: %w", path, err)
	}
	info, err := os.Stat(canonical)
	if err != nil {
		return InvocationRoot{}, fmt.Errorf("inspect invocation root %s: %w", canonical, err)
	}
	if !info.IsDir() {
		return InvocationRoot{}, fmt.Errorf("invocation root must be a directory: %s", canonical)
	}
	return InvocationRoot{path: filepath.Clean(canonical)}, nil
}

func requireInvocationRootPath(raw string) (string, error) {
	path := strings.TrimSpace(raw)
	if path == "" {
		return "", fmt.Errorf("CLI invocation root is required")
	}
	if !filepath.IsAbs(path) {
		return "", fmt.Errorf("CLI invocation root must be absolute: %s", path)
	}
	return filepath.Clean(path), nil
}

func (r InvocationRoot) Path() string {
	return r.path
}

func (r InvocationRoot) Resolve(raw string) string {
	path := strings.TrimSpace(raw)
	if path == "" || filepath.IsAbs(path) {
		return path
	}
	return filepath.Join(r.path, path)
}

func (r InvocationRoot) validate() error {
	if strings.TrimSpace(r.path) == "" || !filepath.IsAbs(r.path) {
		return fmt.Errorf("canonical invocation root is required")
	}
	return nil
}
