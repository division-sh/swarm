//go:build !aix && !darwin && !dragonfly && !freebsd && !hurd && !illumos && !linux && !netbsd && !openbsd && !solaris && !windows

package pythonmodule

import "fmt"

func lockArtifactMutation(string) (func(), error) {
	return nil, fmt.Errorf("CPython-WASI artifact locking is unsupported on this platform")
}
