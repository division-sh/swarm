//go:build windows

package packartifact

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"golang.org/x/sys/windows"
)

func readRegularDevelopmentPackFile(dir, name string) ([]byte, error) {
	path := filepath.Join(dir, name)
	pathUTF16, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return nil, fmt.Errorf("encode development pack artifact %q path: %w", name, err)
	}
	handle, err := windows.CreateFile(
		pathUTF16,
		windows.GENERIC_READ,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_ATTRIBUTE_NORMAL|windows.FILE_FLAG_OPEN_REPARSE_POINT|windows.FILE_FLAG_SEQUENTIAL_SCAN,
		0,
	)
	if err != nil {
		return nil, fmt.Errorf("open development pack artifact %q without following links: %w", name, err)
	}
	file := os.NewFile(uintptr(handle), path)
	if file == nil {
		_ = windows.CloseHandle(handle)
		return nil, fmt.Errorf("open development pack artifact %q returned an invalid file", name)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("inspect opened development pack artifact %q: %w", name, err)
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("development pack artifact %q must be a regular non-symlink file", name)
	}
	body, err := io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("read opened development pack artifact %q: %w", name, err)
	}
	return body, nil
}
