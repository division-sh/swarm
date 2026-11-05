//go:build windows

package testpostgres

import (
	"fmt"
	"os"
	"unsafe"

	"golang.org/x/sys/windows"
)

func validateStateAccess(path string, _ os.FileInfo) error {
	descriptor, err := windows.GetNamedSecurityInfo(path, windows.SE_FILE_OBJECT, windows.OWNER_SECURITY_INFORMATION|windows.DACL_SECURITY_INFORMATION)
	if err != nil {
		return fmt.Errorf("read owner of Postgres service state authority %q: %w", path, err)
	}
	owner, _, err := descriptor.Owner()
	if err != nil {
		return fmt.Errorf("read owner SID of Postgres service state authority %q: %w", path, err)
	}
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		return fmt.Errorf("read current user SID for Postgres service state authority %q: %w", path, err)
	}
	if owner == nil || !owner.Equals(user.User.Sid) {
		return fmt.Errorf("Postgres service state authority %q is not owned by the current user", path)
	}
	dacl, _, err := descriptor.DACL()
	if err != nil || dacl == nil {
		return fmt.Errorf("Postgres service state authority %q has no verifiable DACL", path)
	}
	system, _ := windows.StringToSid("S-1-5-18")
	administrators, _ := windows.StringToSid("S-1-5-32-544")
	const fileDeleteChild windows.ACCESS_MASK = 0x00000040
	writeMask := windows.ACCESS_MASK(windows.FILE_WRITE_DATA | windows.FILE_APPEND_DATA | windows.FILE_WRITE_ATTRIBUTES | windows.FILE_WRITE_EA | windows.DELETE | windows.WRITE_DAC | windows.WRITE_OWNER | windows.GENERIC_WRITE | windows.GENERIC_ALL)
	writeMask |= fileDeleteChild
	for index := uint32(0); index < uint32(dacl.AceCount); index++ {
		var ace *windows.ACCESS_ALLOWED_ACE
		if err := windows.GetAce(dacl, index, &ace); err != nil {
			return fmt.Errorf("inspect DACL for Postgres service state authority %q: %w", path, err)
		}
		if ace.Header.AceType == windows.ACCESS_DENIED_ACE_TYPE {
			continue
		}
		if ace.Header.AceType != windows.ACCESS_ALLOWED_ACE_TYPE {
			return fmt.Errorf("Postgres service state authority %q has unsupported DACL entry type %d", path, ace.Header.AceType)
		}
		if ace.Mask&writeMask == 0 {
			continue
		}
		sid := (*windows.SID)(unsafe.Pointer(&ace.SidStart))
		if !sid.Equals(owner) && !sid.Equals(system) && !sid.Equals(administrators) {
			return fmt.Errorf("Postgres service state authority %q grants write authority to untrusted SID %s", path, sid.String())
		}
	}
	return nil
}
