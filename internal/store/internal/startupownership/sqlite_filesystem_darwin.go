//go:build darwin

package startupownership

import (
	"fmt"
	"strings"

	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"golang.org/x/sys/unix"
)

func requireSupportedLocalFilesystem(path string) error {
	var stat unix.Statfs_t
	if err := unix.Statfs(path, &stat); err != nil {
		return fmt.Errorf("inspect SQLite selected-store filesystem: %w", err)
	}
	nameBytes := make([]byte, 0, len(stat.Fstypename))
	for _, value := range stat.Fstypename {
		if value == 0 {
			break
		}
		nameBytes = append(nameBytes, byte(value))
	}
	switch strings.ToLower(string(nameBytes)) {
	case "apfs", "hfs", "ufs", "tmpfs":
		return nil
	default:
		return &runtimestartupownership.AcquisitionError{
			Failure: runtimestartupownership.AcquisitionPriorOwnerAmbiguous,
			Detail:  "SQLite selected-store filesystem cannot prove process ownership",
		}
	}
}

func systemCanonicalPathAlias(abs, resolved string) bool {
	// macOS exposes /var as the system-owned /private/var mount alias. It is
	// not an operator-selected alternate database identity, and both spellings
	// resolve to the same retained lock coordinate.
	return strings.HasPrefix(abs, "/var/") && resolved == "/private"+abs
}
