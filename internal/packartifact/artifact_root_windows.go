//go:build windows

package packartifact

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"unsafe"

	"golang.org/x/sys/windows"
)

type admittedArtifactRoot struct {
	directory *os.File
}

type fileAttributeTagInfo struct {
	attributes uint32
	reparseTag uint32
}

func openAdmittedArtifactRoot(target string) (*admittedArtifactRoot, error) {
	target = filepath.Clean(target)
	if !filepath.IsAbs(target) {
		return nil, fmt.Errorf("artifact root %q must be absolute", target)
	}
	volumeRoot := filepath.VolumeName(target) + string(filepath.Separator)
	relative, err := filepath.Rel(volumeRoot, target)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return nil, fmt.Errorf("artifact root %q is outside its volume", target)
	}
	rootPath, err := windows.UTF16PtrFromString(volumeRoot)
	if err != nil {
		return nil, fmt.Errorf("encode artifact volume root %q: %w", volumeRoot, err)
	}
	handle, err := windows.CreateFile(rootPath, windows.GENERIC_READ,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE, nil, windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_OPEN_REPARSE_POINT, 0)
	if err != nil {
		return nil, fmt.Errorf("open artifact volume root %q: %w", volumeRoot, err)
	}
	directory := os.NewFile(uintptr(handle), volumeRoot)
	if directory == nil {
		_ = windows.CloseHandle(handle)
		return nil, fmt.Errorf("open artifact volume root %q returned an invalid handle", volumeRoot)
	}
	if err := requireWindowsHandleKind(directory, true); err != nil {
		_ = directory.Close()
		return nil, err
	}
	segments, err := artifactPathSegments(relative)
	if err != nil {
		_ = directory.Close()
		return nil, err
	}
	for _, segment := range segments {
		next, openErr := openWindowsRelative(directory, segment, true)
		_ = directory.Close()
		if openErr != nil {
			return nil, fmt.Errorf("open artifact root %q component %q without following links: %w", target, segment, openErr)
		}
		directory = next
	}
	return &admittedArtifactRoot{directory: directory}, nil
}

func openWindowsRelative(parent *os.File, name string, directory bool) (*os.File, error) {
	objectName, err := windows.NewNTUnicodeString(name)
	if err != nil {
		return nil, err
	}
	attributes := &windows.OBJECT_ATTRIBUTES{
		RootDirectory: windows.Handle(parent.Fd()),
		ObjectName:    objectName,
		Attributes:    windows.OBJ_CASE_INSENSITIVE,
	}
	attributes.Length = uint32(unsafe.Sizeof(*attributes))
	options := uint32(windows.FILE_OPEN_REPARSE_POINT | windows.FILE_SYNCHRONOUS_IO_NONALERT)
	if directory {
		options |= windows.FILE_DIRECTORY_FILE
	} else {
		options |= windows.FILE_NON_DIRECTORY_FILE | windows.FILE_SEQUENTIAL_ONLY
	}
	var (
		handle windows.Handle
		status windows.IO_STATUS_BLOCK
	)
	err = windows.NtCreateFile(&handle, windows.FILE_GENERIC_READ, attributes, &status, nil, 0,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		windows.FILE_OPEN, options, 0, 0)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(handle), name)
	if file == nil {
		_ = windows.CloseHandle(handle)
		return nil, fmt.Errorf("opened artifact handle is invalid")
	}
	if err := requireWindowsHandleKind(file, directory); err != nil {
		_ = file.Close()
		return nil, err
	}
	return file, nil
}

func requireWindowsHandleKind(file *os.File, directory bool) error {
	var info fileAttributeTagInfo
	if err := windows.GetFileInformationByHandleEx(windows.Handle(file.Fd()), windows.FileAttributeTagInfo,
		(*byte)(unsafe.Pointer(&info)), uint32(unsafe.Sizeof(info))); err != nil {
		return fmt.Errorf("inspect opened artifact handle: %w", err)
	}
	if info.attributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
		return fmt.Errorf("artifact path must not be a reparse point")
	}
	isDirectory := info.attributes&windows.FILE_ATTRIBUTE_DIRECTORY != 0
	if directory != isDirectory {
		return fmt.Errorf("artifact path has the wrong file type")
	}
	if !directory {
		stat, err := file.Stat()
		if err != nil {
			return fmt.Errorf("inspect opened artifact file: %w", err)
		}
		if !stat.Mode().IsRegular() {
			return fmt.Errorf("artifact path must be a regular file")
		}
	}
	return nil
}

func (r *admittedArtifactRoot) close() error {
	if r == nil || r.directory == nil {
		return nil
	}
	err := r.directory.Close()
	r.directory = nil
	return err
}

func (r *admittedArtifactRoot) openDirectory(relative string) (*os.File, error) {
	if r == nil || r.directory == nil {
		return nil, fmt.Errorf("artifact root is closed")
	}
	process := windows.CurrentProcess()
	var duplicate windows.Handle
	if err := windows.DuplicateHandle(process, windows.Handle(r.directory.Fd()), process, &duplicate, 0, false, windows.DUPLICATE_SAME_ACCESS); err != nil {
		return nil, fmt.Errorf("duplicate artifact root handle: %w", err)
	}
	directory := os.NewFile(uintptr(duplicate), r.directory.Name())
	if directory == nil {
		_ = windows.CloseHandle(duplicate)
		return nil, fmt.Errorf("duplicate artifact root handle is invalid")
	}
	segments, err := artifactPathSegments(relative)
	if err != nil {
		_ = directory.Close()
		return nil, err
	}
	for _, segment := range segments {
		next, openErr := openWindowsRelative(directory, segment, true)
		_ = directory.Close()
		if openErr != nil {
			return nil, fmt.Errorf("open artifact directory %q without following links: %w", relative, openErr)
		}
		directory = next
	}
	return directory, nil
}

func (r *admittedArtifactRoot) readDir(relative string) ([]fs.DirEntry, error) {
	directory, err := r.openDirectory(relative)
	if err != nil {
		return nil, err
	}
	defer directory.Close()
	entries, err := directory.ReadDir(-1)
	if err != nil {
		return nil, fmt.Errorf("read artifact directory %q: %w", relative, err)
	}
	return entries, nil
}

func (r *admittedArtifactRoot) openRegularFile(relative string) (*os.File, error) {
	segments, err := artifactPathSegments(relative)
	if err != nil {
		return nil, err
	}
	if len(segments) == 0 {
		return nil, fmt.Errorf("artifact file path is required")
	}
	directory, err := r.openDirectory(filepath.Join(segments[:len(segments)-1]...))
	if err != nil {
		return nil, err
	}
	defer directory.Close()
	file, err := openWindowsRelative(directory, segments[len(segments)-1], false)
	if err != nil {
		return nil, fmt.Errorf("open artifact file %q without following links: %w", relative, err)
	}
	return file, nil
}

func (r *admittedArtifactRoot) readRegularFile(relative string) ([]byte, error) {
	file, err := r.openRegularFile(relative)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	body, err := io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("read opened artifact file %q: %w", relative, err)
	}
	return body, nil
}
