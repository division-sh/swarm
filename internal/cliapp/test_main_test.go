package cliapp

import (
	"fmt"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	home, err := os.MkdirTemp("", "swarm-cliapp-test-home-")
	if err != nil {
		panic(err)
	}
	if err := os.Setenv("HOME", home); err != nil {
		panic(err)
	}
	if err := os.Setenv("XDG_CONFIG_HOME", home); err != nil {
		panic(err)
	}
	writeCLIAPITestExecutionPosture(testMainTB{})
	code := m.Run()
	_ = os.RemoveAll(home)
	os.Exit(code)
}

type testMainTB struct{}

func (testMainTB) Helper() {}

func (testMainTB) Fatalf(format string, args ...any) {
	panic(fmt.Sprintf(format, args...))
}
