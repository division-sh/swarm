//go:build linux

package devscratch

import (
	"fmt"

	"golang.org/x/sys/unix"
)

func requireSupportedFilesystem(path string) error {
	var stat unix.Statfs_t
	if err := unix.Statfs(path, &stat); err != nil {
		return fmt.Errorf("inspect dev scratch filesystem: %w", err)
	}
	switch uint64(stat.Type) {
	case 0xef53, 0x58465342, 0x9123683e, 0x01021994, 0x794c7630, 0x2fc12fc1:
		return nil
	default:
		return fmt.Errorf("dev scratch filesystem type %#x cannot prove retained process ownership", uint64(stat.Type))
	}
}
