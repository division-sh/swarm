//go:build !linux && !darwin

package testprocess

import (
	"fmt"
	"os/exec"
	"runtime"
)

type Lifetime struct {
	PID   int
	Birth string
}

func Current() (Lifetime, error)                                 { return Lifetime{}, unsupported() }
func Observe(int) (Lifetime, error)                              { return Lifetime{}, unsupported() }
func (Lifetime) Alive() bool                                     { return false }
func Start(*exec.Cmd) error                                      { return unsupported() }
func StartFor(*exec.Cmd, Lifetime) error                         { return unsupported() }
func WriteBoundScript(string, string, string, ...Lifetime) error { return unsupported() }
func unsupported() error {
	return fmt.Errorf("test helper lifetime binding is unsupported on %s", runtime.GOOS)
}
