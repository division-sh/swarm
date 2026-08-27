//go:build !darwin && !linux

package devscratch

import (
	"errors"
	"os"
)

type platformLock interface {
	rewrite([]byte) error
	release() error
}

func acquirePlatformLock(string) (platformLock, error) {
	return nil, errors.New("dev scratch retained filesystem ownership is unsupported on this platform")
}

func isSingleLink(info os.FileInfo) bool {
	return info != nil && info.Mode().IsRegular()
}
