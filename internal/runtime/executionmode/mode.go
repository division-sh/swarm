package executionmode

import "strings"

type Mode string

const (
	Live Mode = "live"
	Mock Mode = "mock"
)

func (m Mode) Valid() bool { return m == Live || m == Mock }

func Parse(raw string) (Mode, bool) {
	mode := Mode(strings.TrimSpace(raw))
	return mode, mode.Valid()
}
