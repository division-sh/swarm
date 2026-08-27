//go:build darwin

package devscratch

import (
	"fmt"
	"strings"

	"golang.org/x/sys/unix"
)

func requireSupportedFilesystem(path string) error {
	var stat unix.Statfs_t
	if err := unix.Statfs(path, &stat); err != nil {
		return fmt.Errorf("inspect dev scratch filesystem: %w", err)
	}
	name := make([]byte, 0, len(stat.Fstypename))
	for _, value := range stat.Fstypename {
		if value == 0 {
			break
		}
		name = append(name, byte(value))
	}
	switch strings.ToLower(string(name)) {
	case "apfs", "hfs", "ufs", "tmpfs":
		return nil
	default:
		return fmt.Errorf("dev scratch filesystem %q cannot prove retained process ownership", string(name))
	}
}
