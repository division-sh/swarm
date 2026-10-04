//go:build !linux

package worker

import (
	"fmt"
	"io"
	"os"
)

func InterruptibleInput(input io.ReadCloser) (io.ReadCloser, error) {
	if _, native := input.(*os.File); native {
		return nil, fmt.Errorf("remote workspace worker requires Linux")
	}
	return input, nil
}
