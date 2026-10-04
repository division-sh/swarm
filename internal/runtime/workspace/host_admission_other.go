//go:build !linux && !darwin && !freebsd && !openbsd && !netbsd && !dragonfly

package workspace

import "fmt"

func inspectHostDirectoryAccess(string) error {
	return fmt.Errorf("effective host filesystem access observation is unavailable on this platform")
}
