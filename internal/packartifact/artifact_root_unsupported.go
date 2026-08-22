//go:build !linux && !darwin && !windows

package packartifact

import (
	"fmt"
	"io/fs"
	"os"
)

type admittedArtifactRoot struct{}

func openAdmittedArtifactRoot(target string) (*admittedArtifactRoot, error) {
	return nil, fmt.Errorf("artifact root %q cannot be admitted safely on this platform", target)
}

func (r *admittedArtifactRoot) close() error { return nil }

func (r *admittedArtifactRoot) readDir(relative string) ([]fs.DirEntry, error) {
	return nil, fmt.Errorf("artifact directory %q cannot be admitted safely on this platform", relative)
}

func (r *admittedArtifactRoot) openRegularFile(relative string) (*os.File, error) {
	return nil, fmt.Errorf("artifact file %q cannot be admitted safely on this platform", relative)
}

func (r *admittedArtifactRoot) readRegularFile(relative string) ([]byte, error) {
	return nil, fmt.Errorf("artifact file %q cannot be admitted safely on this platform", relative)
}
