package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"regexp"

	"github.com/spf13/cobra"
)

const (
	cliOutputJSONFlag        = "json"
	cliOutputJSONFlagHelp    = "Render successful output as one JSON document"
	cliOutputQuietFlag       = "quiet"
	cliOutputQuietFlagHelp   = "Render only declared load-bearing value(s)"
	cliOutputNoColorFlag     = "no-color"
	cliOutputNoColorFlagHelp = "Disable ANSI color in human-readable output"
)

type cliOutputOptions struct {
	asJSON  bool
	quiet   bool
	noColor bool
}

type cliTextRenderer func(io.Writer)
type cliQuietRenderer func() ([]string, error)

var cliANSISequencePattern = regexp.MustCompile("\x1b\\[[0-?]*[ -/]*[@-~]")

func bindCLIOutputFlags(cmd *cobra.Command, opts *cliOutputOptions) {
	cmd.Flags().BoolVar(&opts.asJSON, cliOutputJSONFlag, false, cliOutputJSONFlagHelp)
	cmd.Flags().BoolVar(&opts.quiet, cliOutputQuietFlag, false, cliOutputQuietFlagHelp)
	cmd.Flags().BoolVar(&opts.noColor, cliOutputNoColorFlag, false, cliOutputNoColorFlagHelp)
}

func bindCLIOutputFlagSet(fs *flag.FlagSet, opts *cliOutputOptions) {
	fs.BoolVar(&opts.asJSON, cliOutputJSONFlag, false, cliOutputJSONFlagHelp)
	fs.BoolVar(&opts.quiet, cliOutputQuietFlag, false, cliOutputQuietFlagHelp)
	fs.BoolVar(&opts.noColor, cliOutputNoColorFlag, false, cliOutputNoColorFlagHelp)
}

func (opts cliOutputOptions) validate() error {
	if opts.asJSON && opts.quiet {
		return fmt.Errorf("--json and --quiet are mutually exclusive")
	}
	return nil
}

func (opts cliOutputOptions) colorDisabled() bool {
	if opts.noColor {
		return true
	}
	value, ok := os.LookupEnv("NO_COLOR")
	return ok && value != ""
}

func (opts cliOutputOptions) textWriter(out io.Writer) io.Writer {
	if !opts.colorDisabled() {
		return out
	}
	return cliANSITextWriter{out: out}
}

type cliANSITextWriter struct {
	out io.Writer
}

func (w cliANSITextWriter) Write(p []byte) (int, error) {
	if w.out == nil {
		return len(p), nil
	}
	clean := cliANSISequencePattern.ReplaceAll(p, nil)
	if len(clean) == 0 {
		return len(p), nil
	}
	if _, err := w.out.Write(clean); err != nil {
		return 0, err
	}
	return len(p), nil
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
			text(opts.textWriter(out))
		}
	}
	return nil
}
