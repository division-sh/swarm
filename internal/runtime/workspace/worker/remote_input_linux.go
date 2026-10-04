package worker

import (
	"io"
	"os"
	"syscall"
	"time"
)

// InterruptibleInput returns an owned pollable duplicate for native stdin.
// The caller closes it and the original; both refer to the same input stream.
func InterruptibleInput(input io.ReadCloser) (io.ReadCloser, error) {
	file, ok := input.(*os.File)
	if !ok {
		return input, nil
	}
	connection, err := file.SyscallConn()
	if err != nil {
		return nil, err
	}
	var descriptor int
	var duplicateErr error
	if err := connection.Control(func(fd uintptr) { descriptor, duplicateErr = syscall.Dup(int(fd)) }); err != nil {
		return nil, err
	}
	if duplicateErr != nil {
		return nil, duplicateErr
	}
	if err := syscall.SetNonblock(descriptor, true); err != nil {
		_ = syscall.Close(descriptor)
		return nil, err
	}
	// Inherited stdin can be a blocking descriptor that os.File.Close cannot
	// interrupt. Register the owned duplicate with Go's poller before reading.
	owned := os.NewFile(uintptr(descriptor), file.Name())
	if err := owned.SetReadDeadline(time.Time{}); err != nil {
		_ = owned.Close()
		return nil, err
	}
	return owned, nil
}
