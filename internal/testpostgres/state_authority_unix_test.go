//go:build aix || darwin || dragonfly || freebsd || hurd || illumos || linux || netbsd || openbsd || solaris

package testpostgres

import (
	"io/fs"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"
)

type authorityFileInfo struct {
	uid uint32
}

func (i authorityFileInfo) Name() string       { return "authority" }
func (i authorityFileInfo) Size() int64        { return 0 }
func (i authorityFileInfo) Mode() fs.FileMode  { return 0o600 }
func (i authorityFileInfo) ModTime() time.Time { return time.Time{} }
func (i authorityFileInfo) IsDir() bool        { return false }
func (i authorityFileInfo) Sys() any           { return &syscall.Stat_t{Uid: i.uid} }

func TestStateAuthorityRejectsWrongOwner(t *testing.T) {
	wrongUID := uint32(os.Geteuid()) + 1
	err := validateStateOwner("authority", authorityFileInfo{uid: wrongUID})
	if err == nil || !strings.Contains(err.Error(), "owned by uid") {
		t.Fatalf("validateStateOwner() error = %v", err)
	}
}
