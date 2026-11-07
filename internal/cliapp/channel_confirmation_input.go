package cliapp

import (
	"context"
	"errors"
)

func (opts *channelConnectOptions) prepareConnectConfirmationInput(ctx context.Context, provider string) (func() error, bool, error) {
	deferred := provider == "telegram" && opts.verb == "connect" && opts.account != "" && opts.apiOptions.channelRegistration != nil
	if deferred {
		return nil, true, nil
	}
	closeInput, err := opts.prepareConfirmationInput(ctx)
	return closeInput, false, err
}

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
	if closeInput == nil {
		return
	}
	if err := closeInput(); err != nil {
		*resultErr = errors.Join(*resultErr, err)
	}
}
