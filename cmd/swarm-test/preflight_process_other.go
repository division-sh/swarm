//go:build !aix && !darwin && !dragonfly && !freebsd && !hurd && !illumos && !linux && !netbsd && !openbsd && !solaris

package main

import (
	"context"
	"fmt"
	"io"
)

func runPreflightProcess(context.Context, []string, io.Writer, io.Writer) (preflightProcessTiming, error) {
	return preflightProcessTiming{}, fmt.Errorf("preflight descendant ownership is unsupported on this platform")
}
