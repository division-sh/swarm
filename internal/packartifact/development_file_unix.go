//go:build linux || darwin

package packartifact

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

func readRegularDevelopmentPackFile(dir, name string) ([]byte, error) {
	path := filepath.Join(dir, name)
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, fmt.Errorf("open development pack artifact %q without following links: %w", name, err)
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("open development pack artifact %q returned an invalid file", name)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("inspect opened development pack artifact %q: %w", name, err)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("development pack artifact %q must be a regular non-symlink file", name)
	}
	body, err := io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("read opened development pack artifact %q: %w", name, err)
	}
	return body, nil
}
