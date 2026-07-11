//go:build aix || darwin || dragonfly || freebsd || hurd || illumos || linux || netbsd || openbsd || solaris

package testpostgres

import (
	"fmt"
	"os"
	"syscall"
)

func validateStateAccess(path string, info os.FileInfo) error {
	if info.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("Postgres service state authority %q has unsafe mode %04o; require owner-only access", path, info.Mode().Perm())
	}
	return validateStateOwner(path, info)
}

func validateStateOwner(path string, info os.FileInfo) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("cannot verify owner of Postgres service state authority %q", path)
	}
	if stat.Uid != uint32(os.Geteuid()) {
		return fmt.Errorf("Postgres service state authority %q is owned by uid %d, want %d", path, stat.Uid, os.Geteuid())
	}
	return nil
}
