//go:build linux || darwin || freebsd || openbsd || netbsd || dragonfly

package workspace

import "golang.org/x/sys/unix"

func inspectHostDirectoryAccess(path string) error {
	return unix.Faccessat(unix.AT_FDCWD, path, unix.W_OK|unix.X_OK, unix.AT_EACCESS)
}
