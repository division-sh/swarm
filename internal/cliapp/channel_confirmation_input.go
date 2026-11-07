package cliapp

import "errors"

func (opts channelConnectOptions) validateConnectConfirmationInput(provider string) (bool, error) {
	deferred := provider == "telegram" && opts.verb == "connect" && opts.account != "" && opts.apiOptions.channelRegistration != nil
	if deferred {
		return true, nil
	}
	return false, opts.validateConfirmationInput()
}

func (opts channelConnectOptions) validateConfirmationInput() error {
	if (opts.verb == "connect" || opts.verb == "rebind") && !opts.yes && !controlStdinIsTerminal(opts.apiOptions) {
		return errors.New("channel account confirmation requires a terminal or --yes")
	}
	return nil
}

func finishChannelConfirmationInput(closeInput func() error, resultErr *error) {
	if closeInput == nil {
		return
	}
	if err := closeInput(); err != nil {
		*resultErr = errors.Join(*resultErr, err)
	}
}
