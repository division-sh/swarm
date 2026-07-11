//go:build windows

package testpostgres

import (
	"fmt"
	"os"

	"golang.org/x/sys/windows"
)

func validateStateAccess(path string, _ os.FileInfo) error {
	descriptor, err := windows.GetNamedSecurityInfo(path, windows.SE_FILE_OBJECT, windows.OWNER_SECURITY_INFORMATION)
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
	return nil
}
