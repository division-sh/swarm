package cliapp

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/division-sh/swarm/internal/userfacing"
	"github.com/spf13/cobra"
	"golang.org/x/term"
	"gopkg.in/yaml.v3"
)

const (
	cliOutputJSONFlag        = "json"
	cliOutputJSONFlagHelp    = "Render successful output as one JSON document"
	cliOutputYAMLFlag        = "yaml"
	cliOutputYAMLFlagHelp    = "Render successful output as one YAML document"
	cliOutputQuietFlag       = "quiet"
	cliOutputQuietFlagHelp   = "Render only declared load-bearing value(s)"
	cliOutputNoColorFlag     = "no-color"
	cliOutputNoColorFlagHelp = "Disable ANSI color in human-readable output"
	cliOutputVerboseFlag     = "verbose"
	cliOutputVerboseFlagHelp = "Render the full record: absolute timestamps, bookkeeping fields, loops, and accumulated state"
)

type cliOutputOptions struct {
	asJSON  bool
	asYAML  bool
	quiet   bool
	noColor bool
	verbose bool
}

type cliTextRenderer func(io.Writer)
type cliQuietRenderer func() ([]string, error)

var cliANSISequencePattern = regexp.MustCompile("\x1b\\[[0-?]*[ -/]*[@-~]")

type cliDisplayPolicy struct {
	Color bool
	Emoji bool
}

type cliTextOutputWriter struct {
	out    io.Writer
	policy cliDisplayPolicy
}

func (w cliTextOutputWriter) Write(p []byte) (int, error) {
	if w.out == nil {
		return len(p), nil
	}
	return w.out.Write(p)
}

func (w cliTextOutputWriter) displayPolicy() cliDisplayPolicy {
	return w.policy
}

type cliDisplayPolicyProvider interface {
	displayPolicy() cliDisplayPolicy
}

type cliTableColumn struct {
	Header           string
	KeyColumn        bool
	Truncatable      bool
	IdentifierFamily cliIdentifierFamily
}

type cliTable struct {
	Columns      []cliTableColumn
	Rows         [][]string
	EmptyMessage string
	FooterLines  []string
}

type cliDetailField struct {
	Key   string
	Value string
}

type cliHumanCodeFamily = userfacing.HumanCodeFamily

const (
	cliHumanCodeRunStatus                   = userfacing.HumanCodeRunStatus
	cliHumanCodeOperationalState            = userfacing.HumanCodeOperationalState
	cliHumanCodeRunBlockingLayer            = userfacing.HumanCodeRunBlockingLayer
	cliHumanCodeRunBlockingReason           = userfacing.HumanCodeRunBlockingReason
	cliHumanCodeAgentStatus                 = userfacing.HumanCodeAgentStatus
	cliHumanCodeMemorySource                = userfacing.HumanCodeMemorySource
	cliHumanCodeDeliveryStatus              = userfacing.HumanCodeDeliveryStatus
	cliHumanCodeAgentLifecycleState         = userfacing.HumanCodeAgentLifecycleState
	cliHumanCodeAgentLifecycleBlockingLayer = userfacing.HumanCodeAgentLifecycleBlockingLayer
	cliHumanCodeWatchdogState               = userfacing.HumanCodeWatchdogState
	cliHumanCodeWatchdogBlockingLayer       = userfacing.HumanCodeWatchdogBlockingLayer
	cliHumanCodeWatchdogAction              = userfacing.HumanCodeWatchdogAction
	cliHumanCodeWatchdogOutcome             = userfacing.HumanCodeWatchdogOutcome
	cliHumanCodeProviderSubjectKind         = userfacing.HumanCodeProviderSubjectKind
	cliHumanCodeProviderSubjectStatus       = userfacing.HumanCodeProviderSubjectStatus
	cliHumanCodeProviderCapability          = userfacing.HumanCodeProviderCapability
	cliHumanCodeProviderGuarantee           = userfacing.HumanCodeProviderGuarantee
	cliHumanCodeProviderRequirementStatus   = userfacing.HumanCodeProviderRequirementStatus
	cliHumanCodeRoutingTopology             = userfacing.HumanCodeRoutingTopology
)

func formatCLIHumanCode(family cliHumanCodeFamily, raw string) string {
	return userfacing.ProjectHumanCode(family, raw)
}

func formatCLIHumanCount(count int, singular, plural string) string {
	label := plural
	if count == 1 {
		label = singular
	}
	return fmt.Sprintf("%d %s", count, label)
}

type cliLabeledDetailRow struct {
	Label string
	Value string
}

type cliLabeledDetailSection struct {
	Label string
	Items []string
}

type cliLabeledDetail struct {
	Title    string
	Rows     []cliLabeledDetailRow
	Sections []cliLabeledDetailSection
}

func writeCLILabeledDetail(out io.Writer, detail cliLabeledDetail) {
	if out == nil {
		return
	}
	writeCLITitle(out, detail.Title)
	width := 0
	rows := make([]cliLabeledDetailRow, 0, len(detail.Rows))
	for _, row := range detail.Rows {
		row.Label = strings.TrimSpace(row.Label)
		row.Value = strings.TrimSpace(row.Value)
		if row.Label == "" || row.Value == "" {
			continue
		}
		rows = append(rows, row)
		if candidate := cliDisplayWidth(row.Label); candidate > width {
			width = candidate
		}
	}
	for _, row := range rows {
		fmt.Fprintf(out, "  %s%s  %s\n", row.Label, strings.Repeat(" ", width-cliDisplayWidth(row.Label)), row.Value)
	}
	for _, section := range detail.Sections {
		label := strings.TrimSpace(section.Label)
		items := make([]string, 0, len(section.Items))
		for _, item := range section.Items {
			if item = strings.TrimSpace(item); item != "" {
				items = append(items, item)
			}
		}
		if label == "" || len(items) == 0 {
			continue
		}
		fmt.Fprintf(out, "  %s\n", label)
		for _, item := range items {
			fmt.Fprintf(out, "    %s\n", item)
		}
	}
}

func bindCLIOutputFlags(cmd *cobra.Command, opts *cliOutputOptions) {
	cmd.Flags().BoolVar(&opts.asJSON, cliOutputJSONFlag, false, cliOutputJSONFlagHelp)
	cmd.Flags().BoolVar(&opts.quiet, cliOutputQuietFlag, false, cliOutputQuietFlagHelp)
	cmd.Flags().BoolVar(&opts.noColor, cliOutputNoColorFlag, false, cliOutputNoColorFlagHelp)
}

func bindCLIYAMLOutputFlag(cmd *cobra.Command, opts *cliOutputOptions) {
	cmd.Flags().BoolVar(&opts.asYAML, cliOutputYAMLFlag, false, cliOutputYAMLFlagHelp)
}

// bindCLIOutputVerboseFlag binds the shared --verbose flag without sweeping it
// onto every bindCLIOutputFlags consumer; commands opt into the full record
// projection explicitly.
func bindCLIOutputVerboseFlag(cmd *cobra.Command, opts *cliOutputOptions) {
	cmd.Flags().BoolVar(&opts.verbose, cliOutputVerboseFlag, false, cliOutputVerboseFlagHelp)
}

func (opts cliOutputOptions) validate() error {
	if opts.asJSON && opts.quiet && !opts.asYAML {
		return fmt.Errorf("--json and --quiet are mutually exclusive")
	}
	if opts.asYAML && (opts.asJSON || opts.quiet) {
		return fmt.Errorf("--json, --yaml, and --quiet are mutually exclusive")
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
	policy := cliDisplayPolicy{
		Color: !opts.colorDisabled() && cliOutputIsTerminal(out),
		Emoji: !opts.colorDisabled() && cliOutputIsTerminal(out),
	}
	writer := out
	if opts.colorDisabled() {
		writer = cliANSITextWriter{out: out}
	}
	return cliTextOutputWriter{out: writer, policy: policy}
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

func cliOutputIsTerminal(out io.Writer) bool {
	file, ok := out.(*os.File)
	if !ok || file == nil {
		return false
	}
	return term.IsTerminal(int(file.Fd()))
}

func cliWriterDisplayPolicy(out io.Writer) cliDisplayPolicy {
	if provider, ok := out.(cliDisplayPolicyProvider); ok {
		return provider.displayPolicy()
	}
	return cliDisplayPolicy{}
}

func writeCLITable(out io.Writer, table cliTable) {
	if out == nil {
		return
	}
	if len(table.Rows) == 0 {
		writeCLIEmptyState(out, table.EmptyMessage)
		writeCLIFooterLines(out, table.FooterLines)
		return
	}
	columns := table.Columns
	if len(columns) == 0 {
		for _, row := range table.Rows {
			fmt.Fprintln(out, strings.Join(row, "  "))
		}
		writeCLIFooterLines(out, table.FooterLines)
		return
	}
	widths := make([]int, len(columns))
	for i, column := range columns {
		widths[i] = cliDisplayWidth(column.Header)
	}
	normalizedRows := make([][]string, 0, len(table.Rows))
	for _, row := range table.Rows {
		normalized := make([]string, len(columns))
		for i := range columns {
			if i < len(row) {
				normalized[i] = formatCLIIdentifierForDisplay(columns[i].IdentifierFamily, cliDisplayDash(row[i]))
			} else {
				normalized[i] = "-"
			}
			if width := cliDisplayWidth(normalized[i]); width > widths[i] {
				widths[i] = width
			}
		}
		normalizedRows = append(normalizedRows, normalized)
	}
	writeCLITableRow(out, cliTableHeaders(columns), widths)
	for _, row := range normalizedRows {
		writeCLITableRow(out, row, widths)
	}
	writeCLIFooterLines(out, table.FooterLines)
}

func cliTableHeaders(columns []cliTableColumn) []string {
	headers := make([]string, len(columns))
	for i, column := range columns {
		headers[i] = column.Header
	}
	return headers
}

func writeCLITableRow(out io.Writer, row []string, widths []int) {
	for i, value := range row {
		if i > 0 {
			fmt.Fprint(out, "  ")
		}
		fmt.Fprint(out, value)
		if i < len(row)-1 {
			padding := widths[i] - cliDisplayWidth(value)
			if padding > 0 {
				fmt.Fprint(out, strings.Repeat(" ", padding))
			}
		}
	}
	fmt.Fprintln(out)
}

func writeCLIEmptyState(out io.Writer, message string) {
	message = strings.TrimSpace(message)
	if message == "" {
		message = "No rows match the current filters."
	}
	fmt.Fprintln(out, message)
}

func writeCLIFooterLines(out io.Writer, lines []string) {
	for _, line := range lines {
		if strings.TrimSpace(line) != "" {
			fmt.Fprintln(out, line)
		}
	}
}

func writeCLITitle(out io.Writer, title string) {
	if out == nil || strings.TrimSpace(title) == "" {
		return
	}
	fmt.Fprintln(out, strings.TrimSpace(title))
}

func writeCLIFieldLine(out io.Writer, fields ...cliDetailField) {
	if out == nil {
		return
	}
	line := formatCLIFields(fields...)
	if line == "" {
		return
	}
	fmt.Fprintln(out, line)
}

func formatCLIFields(fields ...cliDetailField) string {
	parts := make([]string, 0, len(fields))
	for _, field := range fields {
		key := strings.TrimSpace(field.Key)
		if key == "" {
			continue
		}
		parts = append(parts, key+"="+cliDisplayDash(field.Value))
	}
	return strings.Join(parts, " ")
}

func cliDisplayDash(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "-"
	}
	return value
}

func cliDisplayWidth(value string) int {
	value = string(cliANSISequencePattern.ReplaceAll([]byte(value), nil))
	if value == "" {
		return 0
	}
	return utf8.RuneCountInString(value)
}

// cliRelativeTimeNow is the single owner of "now" for relative-time rendering;
// tests override it for deterministic output.
var cliRelativeTimeNow = time.Now

// formatCLIRelativeTime renders an RFC3339 timestamp relative to now
// ("5m ago"). Timestamps within a minute in the past OR a minute in the
// future render as "just now" (small clock-skew tolerance); a genuinely future
// timestamp beyond the skew window falls back to the raw value. Unparseable
// values also fall back to raw. Machine output must never consume this helper
// (use the raw absolute value).
func formatCLIRelativeTime(now time.Time, raw string) string {
	at, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(raw))
	if err != nil {
		return raw
	}
	elapsed := now.Sub(at)
	switch {
	case elapsed < -time.Minute:
		return raw
	case elapsed < time.Minute:
		return "just now"
	case elapsed < time.Hour:
		return fmt.Sprintf("%dm ago", int(elapsed.Minutes()))
	case elapsed < 24*time.Hour:
		return fmt.Sprintf("%dh ago", int(elapsed.Hours()))
	case elapsed < 30*24*time.Hour:
		return fmt.Sprintf("%dd ago", int(elapsed.Hours()/24))
	case elapsed < 365*24*time.Hour:
		return fmt.Sprintf("%dmo ago", int(elapsed.Hours()/(24*30)))
	default:
		return fmt.Sprintf("%dy ago", int(elapsed.Hours()/(24*365)))
	}
}

const cliOneLineMaxRunes = 200

// cliRenderOneLineValue collapses line-breaking and control characters so a
// value can never break the CLI's line discipline, then truncates long values
// at a fixed rune ceiling with a visible truncation marker. The ceiling is a
// fixed constant, not terminal-width-dependent, so human output stays
// deterministic across TTY and piped rendering.
func cliRenderOneLineValue(value string) string {
	value = cliSanitizeOneLineValue(value)
	runes := []rune(value)
	if len(runes) <= cliOneLineMaxRunes {
		return value
	}
	return string(runes[:cliOneLineMaxRunes]) + "…"
}

// cliSanitizeOneLineValue preserves the complete value while replacing
// characters that would break terminal line discipline. Identifier columns
// consume this helper because truncation belongs to the identifier registry.
func cliSanitizeOneLineValue(value string) string {
	return strings.Map(cliReplaceLineBreakingRune, strings.TrimSpace(value))
}

func cliReplaceLineBreakingRune(r rune) rune {
	if unicode.IsControl(r) || r == '\u2028' || r == '\u2029' {
		return ' '
	}
	return r
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
	case opts.asYAML:
		if out == nil {
			return nil
		}
		encoded, err := json.Marshal(value)
		if err != nil {
			return returnCLIValidationError(errOut, fmt.Errorf("render yaml output: %w", err))
		}
		var document any
		if err := yaml.Unmarshal(encoded, &document); err != nil {
			return returnCLIValidationError(errOut, fmt.Errorf("render yaml output: %w", err))
		}
		if err := yaml.NewEncoder(out).Encode(document); err != nil {
			return returnCLIValidationError(errOut, fmt.Errorf("render yaml output: %w", err))
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
