//go:build linux

package startupownership

import (
	"fmt"

	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"golang.org/x/sys/unix"
)

func requireSupportedLocalFilesystem(path string) error {
	var stat unix.Statfs_t
	if err := unix.Statfs(path, &stat); err != nil {
		return fmt.Errorf("inspect SQLite selected-store filesystem: %w", err)
	}
	switch uint64(stat.Type) {
	case 0xef53, // ext2/3/4
		0x58465342, // XFS
		0x9123683e, // Btrfs
		0x01021994, // tmpfs
		0x794c7630, // overlayfs
		0x2fc12fc1: // ZFS
		return nil
	default:
		return &runtimestartupownership.AcquisitionError{
			Failure: runtimestartupownership.AcquisitionPriorOwnerAmbiguous,
			Detail:  "SQLite selected-store filesystem cannot prove process ownership",
		}
	}
}

func systemCanonicalPathAlias(_, _ string) bool { return false }
