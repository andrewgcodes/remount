//go:build windows

package privatefile

import (
	"errors"
	"fmt"
	"os"
	"unsafe"

	"golang.org/x/sys/windows"
)

// SecureFile restricts path to the current Windows account, Local System, and
// local Administrators.
func SecureFile(path string) error {
	return secure(path, false)
}

// SecureDirectory restricts path and its future descendants to the current
// Windows account, Local System, and local Administrators.
func SecureDirectory(path string) error {
	return secure(path, true)
}

func secure(path string, inheritable bool) error {
	token, err := windows.OpenCurrentProcessToken()
	if err != nil {
		return err
	}
	defer token.Close()
	current, err := token.GetTokenUser()
	if err != nil {
		return err
	}
	inheritance := ""
	if inheritable {
		inheritance = "OICI"
	}
	sd, err := windows.SecurityDescriptorFromString(
		fmt.Sprintf(
			"D:P(A;%s;FA;;;%s)(A;%s;FA;;;SY)(A;%s;FA;;;BA)",
			inheritance,
			current.User.Sid.String(),
			inheritance,
			inheritance,
		),
	)
	if err != nil {
		return err
	}
	dacl, _, err := sd.DACL()
	if err != nil {
		return err
	}
	return windows.SetNamedSecurityInfo(
		path,
		windows.SE_FILE_OBJECT,
		windows.DACL_SECURITY_INFORMATION|windows.PROTECTED_DACL_SECURITY_INFORMATION,
		nil,
		nil,
		dacl,
		nil,
	)
}

// ValidateFile rejects a file readable by another Windows principal.
func ValidateFile(path string, _ os.FileInfo) error {
	sd, err := windows.GetNamedSecurityInfo(
		path,
		windows.SE_FILE_OBJECT,
		windows.OWNER_SECURITY_INFORMATION|windows.DACL_SECURITY_INFORMATION,
	)
	if err != nil {
		return err
	}
	owner, _, err := sd.Owner()
	if err != nil {
		return err
	}
	dacl, _, err := sd.DACL()
	if err != nil {
		return err
	}
	if dacl == nil {
		return errors.New("private file has an unrestricted Windows DACL")
	}
	system, err := windows.StringToSid("S-1-5-18")
	if err != nil {
		return err
	}
	administrators, err := windows.StringToSid("S-1-5-32-544")
	if err != nil {
		return err
	}
	token, err := windows.OpenCurrentProcessToken()
	if err != nil {
		return err
	}
	defer token.Close()
	current, err := token.GetTokenUser()
	if err != nil {
		return err
	}
	if !owner.Equals(system) && !owner.Equals(administrators) && !owner.Equals(current.User.Sid) {
		return errors.New("private file is owned by another Windows principal")
	}
	for i := uint32(0); i < uint32(dacl.AceCount); i++ {
		var ace *windows.ACCESS_ALLOWED_ACE
		if err := windows.GetAce(dacl, i, &ace); err != nil {
			return err
		}
		switch ace.Header.AceType {
		case windows.ACCESS_DENIED_ACE_TYPE:
			continue
		case windows.ACCESS_ALLOWED_ACE_TYPE:
		default:
			return errors.New("private file has an unsupported Windows access-control entry")
		}
		if ace.Mask&(windows.GENERIC_READ|windows.GENERIC_ALL|windows.FILE_READ_DATA) == 0 {
			continue
		}
		sid := (*windows.SID)(unsafe.Pointer(&ace.SidStart))
		if !sid.Equals(owner) && !sid.Equals(system) && !sid.Equals(administrators) && !sid.Equals(current.User.Sid) {
			return errors.New("private file is readable by another Windows principal")
		}
	}
	return nil
}
