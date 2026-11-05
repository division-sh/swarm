//go:build !linux && !darwin

package cliapp

import (
	"context"
	"io"
)

func channelConfirmationReader(_ context.Context, input io.Reader) (io.Reader, func() error, error) {
	return input, func() error { return nil }, nil
}
