//go:build linux || darwin

package cliapp

import (
	"context"
	"errors"
	"io"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

type channelConfirmationInput struct {
	ctx      context.Context
	input    syscall.RawConn
	wakeRead *os.File
	wakeSend *os.File
	wakeFD   int32
	stop     func() bool
	joined   chan struct{}
}

func channelConfirmationReader(ctx context.Context, input io.Reader) (io.Reader, func() error, error) {
	file, ok := input.(*os.File)
	if !ok {
		// Injected synchronous readers retain their existing read semantics.
		return input, func() error { return nil }, nil
	}
	raw, err := file.SyscallConn()
	if err != nil {
		return nil, nil, err
	}
	wakeRead, wakeSend, err := os.Pipe()
	if err != nil {
		return nil, nil, err
	}
	r := &channelConfirmationInput{ctx: ctx, input: raw, wakeRead: wakeRead, wakeSend: wakeSend,
		wakeFD: int32(wakeRead.Fd()), joined: make(chan struct{})}
	r.stop = context.AfterFunc(ctx, func() {
		defer close(r.joined)
		_, _ = r.wakeSend.Write([]byte{1})
	})
	return r, r.close, nil
}

func (r *channelConfirmationInput) close() error {
	if !r.stop() {
		<-r.joined
	}
	return errors.Join(r.wakeRead.Close(), r.wakeSend.Close())
}

func (r *channelConfirmationInput) Read(p []byte) (n int, err error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	if len(p) == 0 {
		return 0, nil
	}
	// Control pins the borrowed descriptor throughout Poll and the ready read.
	controlErr := r.input.Control(func(fd uintptr) { n, err = r.readReady(int(fd), p[:1]) })
	if cancelled := r.ctx.Err(); cancelled != nil {
		return 0, cancelled
	}
	return n, errors.Join(err, controlErr)
}

func (r *channelConfirmationInput) readReady(fd int, p []byte) (int, error) {
	fds := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}, {Fd: r.wakeFD, Events: unix.POLLIN}}
	for {
		if err := r.ctx.Err(); err != nil {
			return 0, err
		}
		_, err := unix.Poll(fds, -1)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if cancelled := r.ctx.Err(); cancelled != nil {
			return 0, cancelled
		}
		if err != nil {
			return 0, err
		}
		if fds[0].Revents&unix.POLLNVAL != 0 {
			return 0, unix.EBADF
		}
		if fds[0].Revents&(unix.POLLIN|unix.POLLHUP) == 0 {
			return 0, unix.EIO
		}
		n, err := unix.Read(fd, p)
		if errors.Is(err, unix.EINTR) || errors.Is(err, unix.EAGAIN) {
			continue
		}
		if n == 0 && err == nil {
			err = io.EOF
		}
		return n, err
	}
}
