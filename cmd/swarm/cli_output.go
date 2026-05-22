package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"

	"github.com/spf13/cobra"
)

const (
	cliOutputJSONFlag      = "json"
	cliOutputJSONFlagHelp  = "Render successful output as one JSON document"
	cliOutputQuietFlag     = "quiet"
	cliOutputQuietFlagHelp = "Render only declared load-bearing value(s)"
)

type cliOutputOptions struct {
	asJSON bool
	quiet  bool
}

type cliTextRenderer func(io.Writer)
type cliQuietRenderer func() ([]string, error)

func bindCLIOutputFlags(cmd *cobra.Command, opts *cliOutputOptions) {
	cmd.Flags().BoolVar(&opts.asJSON, cliOutputJSONFlag, false, cliOutputJSONFlagHelp)
	cmd.Flags().BoolVar(&opts.quiet, cliOutputQuietFlag, false, cliOutputQuietFlagHelp)
}

func bindCLIOutputFlagSet(fs *flag.FlagSet, opts *cliOutputOptions) {
	fs.BoolVar(&opts.asJSON, cliOutputJSONFlag, false, cliOutputJSONFlagHelp)
	fs.BoolVar(&opts.quiet, cliOutputQuietFlag, false, cliOutputQuietFlagHelp)
}

func (opts cliOutputOptions) validate() error {
	if opts.asJSON && opts.quiet {
		return fmt.Errorf("--json and --quiet are mutually exclusive")
	}
	return nil
}

func renderCLIOutput(out, errOut io.Writer, opts cliOutputOptions, value any, text cliTextRenderer, quiet cliQuietRenderer) error {
	if err := opts.validate(); err != nil {
		return returnCLIValidationError(errOut, err)
	}
	switch {
	case opts.asJSON:
		if out == nil {
			return nil
		}
		if err := json.NewEncoder(out).Encode(value); err != nil {
			return returnCLIValidationError(errOut, fmt.Errorf("render json output: %w", err))
		}
	case opts.quiet:
		if quiet == nil {
			return returnCLIValidationError(errOut, fmt.Errorf("--quiet is not supported for this command"))
		}
		values, err := quiet()
		if err != nil {
			return returnCLIValidationError(errOut, err)
		}
		for _, value := range values {
			if out != nil {
				fmt.Fprintln(out, value)
			}
		}
	default:
		if text != nil {
			text(out)
		}
	}
	return nil
}
