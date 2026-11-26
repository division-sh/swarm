package cliapp

import (
	"errors"
	"net"
	"syscall"
)

func inspectBorrowedAPIListener(address string, listener net.Listener) error {
	expected, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		return err
	}
	actual, ok := listener.Addr().(*net.TCPAddr)
	if !ok || expected.Port != 0 && expected.Port != actual.Port ||
		!(expected.IP.Equal(actual.IP) || len(expected.IP) == 0 && actual.IP.IsUnspecified()) {
		return errors.New("API check listener differs from the configured address")
	}
	connection, ok := listener.(syscall.Conn)
	if !ok {
		return errors.New("API check listener cannot prove its original handle")
	}
	raw, err := connection.SyscallConn()
	if err != nil {
		return err
	}
	return raw.Control(func(uintptr) {})
}
