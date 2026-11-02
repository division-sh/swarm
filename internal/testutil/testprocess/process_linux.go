package testprocess

import (
	"errors"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
)

var launches = sync.OnceValue(func() chan func() {
	queue := make(chan func())
	go func() {
		// Pdeathsig belongs to the creating OS thread, not merely its process.
		// This one launcher thread remains alive for the test binary's lifetime.
		runtime.LockOSThread()
		for launch := range queue {
			launch()
		}
	}()
	return queue
})

func startNative(command *exec.Cmd) error {
	if command == nil {
		return errors.New("test helper command is required")
	}
	attributes := syscall.SysProcAttr{}
	if command.SysProcAttr != nil {
		attributes = *command.SysProcAttr
	}
	if attributes.Pdeathsig != 0 && attributes.Pdeathsig != syscall.SIGKILL {
		return errors.New("test helper has incompatible parent-death signal")
	}
	attributes.Pdeathsig = syscall.SIGKILL
	command.SysProcAttr = &attributes
	result := make(chan error, 1)
	launches() <- func() { result <- command.Start() }
	return <-result
}

func validateWatched(command *exec.Cmd) error {
	if command.SysProcAttr != nil && command.SysProcAttr.Pdeathsig != 0 {
		return errors.New("test helper explicit lifetime conflicts with an existing parent-death signal")
	}
	return nil
}

func processBirth(pid int) (string, error) {
	raw, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return "", err
	}
	end := strings.LastIndexByte(string(raw), ')')
	if end < 0 {
		return "", errors.New("test helper process identity is malformed")
	}
	fields := strings.Fields(string(raw[end+1:]))
	if len(fields) <= 19 || fields[0] == "Z" || fields[0] == "X" {
		return "", errors.New("test helper process is not live")
	}
	return fields[19], nil
}
