package cliapp

import (
	"context"
	"errors"
)

func (opts *channelConnectOptions) prepareConfirmationInput(ctx context.Context) (func() error, error) {
	if (opts.verb == "connect" || opts.verb == "rebind") && !opts.yes && !controlStdinIsTerminal(opts.apiOptions) {
		return nil, errors.New("channel account confirmation requires a terminal or --yes")
	}
	if opts.yes {
		return func() error { return nil }, nil
	}
	input, closeInput, err := channelConfirmationReader(ctx, opts.apiOptions.input)
	if err != nil {
		return nil, err
	}
	opts.apiOptions.input = input
	return closeInput, nil
}

func finishChannelConfirmationInput(closeInput func() error, resultErr *error) {
	if err := closeInput(); err != nil {
		*resultErr = errors.Join(*resultErr, err)
	}
}
