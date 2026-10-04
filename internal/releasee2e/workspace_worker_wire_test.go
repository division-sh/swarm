package releasee2e

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"syscall"
	"time"
)

// The release observer checks the compiled child's wire protocol, never calls
// its implementation. These constants are deliberate independent wire oracles.
const (
	releaseWorkerArgument = "--internal-workspace-worker"
	releaseWorkerABI      = "swarm-workspace-worker-v2"
	releaseWorkerMaxBytes = 4 << 20
)

func readReleaseWorkerFrame(reader *bufio.Reader) ([]byte, error) {
	var frame []byte
	for {
		fragment, err := reader.ReadSlice('\n')
		payloadBytes := len(frame) + len(fragment)
		if err == nil {
			payloadBytes--
		}
		if payloadBytes > releaseWorkerMaxBytes {
			return nil, fmt.Errorf("compiled workspace worker frame over budget")
		}
		frame = append(frame, fragment...)
		if err == nil {
			return frame, nil
		}
		if !errors.Is(err, bufio.ErrBufferFull) {
			return nil, err
		}
	}
}

// An observer's own stdin copy must be interruptible so joining the real child
// can also join the forwarding goroutine. It owns only this local descriptor.
func releaseWorkerInput(input *os.File) (io.ReadCloser, error) {
	if runtime.GOOS != "linux" {
		return input, nil
	}
	connection, err := input.SyscallConn()
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
	owned := os.NewFile(uintptr(descriptor), input.Name())
	if err := owned.SetReadDeadline(time.Time{}); err != nil {
		_ = owned.Close()
		return nil, err
	}
	return owned, nil
}
