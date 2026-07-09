package main

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
)

const cliArgDiscoveryHintAnnotation = "swarm.sh/arg-discovery-hint"

type cliArgCountRule struct {
	exact int
	max   int
}

type cliArgCountDiagnostic struct {
	problem string
	usage   string
	hint    string
}

func (d cliArgCountDiagnostic) Error() string {
	lines := []string{"ERROR: " + d.problem}
	if d.usage != "" {
		lines = append(lines, "Usage: "+d.usage)
	}
	if d.hint != "" {
		lines = append(lines, "  "+d.hint)
	}
	return strings.Join(lines, "\n")
}

func setCLIArgDiscoveryHint(cmd *cobra.Command, hint string) {
	if cmd == nil {
		return
	}
	hint = strings.TrimSpace(hint)
	if hint == "" {
		return
	}
	if cmd.Annotations == nil {
		cmd.Annotations = map[string]string{}
	}
	cmd.Annotations[cliArgDiscoveryHintAnnotation] = hint
}

func cliExactArgs(count int) cobra.PositionalArgs {
	return func(cmd *cobra.Command, args []string) error {
		if len(args) == count {
			return nil
		}
		return newCLIArgCountDiagnostic(cmd, args, cliArgCountRule{exact: count})
	}
}

func cliMaximumNArgs(count int) cobra.PositionalArgs {
	return func(cmd *cobra.Command, args []string) error {
		if len(args) <= count {
			return nil
		}
		return newCLIArgCountDiagnostic(cmd, args, cliArgCountRule{max: count})
	}
}

func newCLIArgCountDiagnostic(cmd *cobra.Command, args []string, rule cliArgCountRule) error {
	commandPath := ""
	name := ""
	use := ""
	hint := ""
	if cmd != nil {
		commandPath = cmd.CommandPath()
		name = cmd.Name()
		use = cmd.Use
		hint = strings.TrimSpace(cmd.Annotations[cliArgDiscoveryHintAnnotation])
	}
	return newCLIArgCountDiagnosticFromUse(commandPath, name, use, args, rule, hint)
}

func newCLIArgCountDiagnosticFromUse(commandPath, name, use string, args []string, rule cliArgCountRule, hint string) error {
	commandPath = strings.TrimSpace(commandPath)
	if commandPath == "" {
		commandPath = "swarm"
	}
	usageSuffix := cliUsageSuffixFromUse(use, name)
	usage := strings.TrimSpace(commandPath)
	if usageSuffix != "" {
		usage += " " + usageSuffix
	}
	placeholders := cliArgPlaceholders(usageSuffix)
	problem := cliArgCountProblem(commandPath, placeholders, args, rule)
	return cliArgCountDiagnostic{
		problem: problem,
		usage:   usage,
		hint:    strings.TrimSpace(hint),
	}
}

func cliUsageSuffixFromUse(use, name string) string {
	use = strings.TrimSpace(use)
	name = strings.TrimSpace(name)
	if use == "" {
		return ""
	}
	segments := strings.Split(use, " | ")
	for i, segment := range segments {
		segment = strings.TrimSpace(segment)
		if name != "" {
			switch {
			case segment == name:
				segment = ""
			case strings.HasPrefix(segment, name+" "):
				segment = strings.TrimSpace(strings.TrimPrefix(segment, name))
			}
		}
		segments[i] = segment
	}
	return strings.Join(segments, " | ")
}

func cliArgPlaceholders(usageSuffix string) []string {
	fields := strings.Fields(usageSuffix)
	placeholders := make([]string, 0, len(fields))
	for _, field := range fields {
		field = strings.TrimSpace(field)
		if field == "" || field == "|" || strings.HasPrefix(field, "--") || strings.HasPrefix(field, "[--") {
			continue
		}
		if placeholder := cliArgPlaceholder(field); placeholder != "" {
			placeholders = append(placeholders, placeholder)
		}
	}
	return placeholders
}

func cliArgPlaceholder(field string) string {
	field = strings.Trim(field, ",.;")
	if strings.HasPrefix(field, "<") {
		if end := strings.Index(field, ">"); end >= 0 {
			return field[:end+1]
		}
		return field
	}
	if strings.HasPrefix(field, "[<") {
		if end := strings.Index(field, ">]"); end >= 0 {
			return field[:end+2]
		}
		return field
	}
	if strings.HasPrefix(field, "[") && strings.HasSuffix(field, "]") && !strings.HasPrefix(field, "[--") {
		return field
	}
	return ""
}

func cliArgCountProblem(commandPath string, placeholders []string, args []string, rule cliArgCountRule) string {
	if rule.exact > 0 {
		if len(args) < rule.exact {
			return fmt.Sprintf("'%s' requires %s%s.", commandPath, cliMissingArgLabel(placeholders, len(args), rule.exact), cliGotPlaceholderSuffix(placeholders, len(args)))
		}
		return fmt.Sprintf("'%s' accepts %s (%s); got %d: %s.", commandPath, cliArgumentCountPhrase(rule.exact), cliExpectedArgList(placeholders, rule.exact), len(args), cliQuotedArgs(args))
	}
	if rule.max >= 0 && len(args) > rule.max {
		expected := cliExpectedArgList(placeholders, rule.max)
		if expected == "" {
			return fmt.Sprintf("'%s' accepts no positional arguments; got %d: %s.", commandPath, len(args), cliQuotedArgs(args))
		}
		return fmt.Sprintf("'%s' accepts at most %s (%s); got %d: %s.", commandPath, cliArgumentCountPhrase(rule.max), expected, len(args), cliQuotedArgs(args))
	}
	return fmt.Sprintf("'%s' received invalid positional arguments: %s.", commandPath, cliQuotedArgs(args))
}

func cliMissingArgLabel(placeholders []string, got, required int) string {
	if got >= 0 && got < len(placeholders) {
		return cliRequiredPlaceholder(placeholders[got])
	}
	if got == 0 && required == 1 {
		return "one argument"
	}
	return fmt.Sprintf("%d positional arguments", required)
}

func cliRequiredPlaceholder(placeholder string) string {
	placeholder = strings.TrimSpace(placeholder)
	if strings.HasPrefix(placeholder, "[<") && strings.HasSuffix(placeholder, ">]") {
		return placeholder[1 : len(placeholder)-1]
	}
	if strings.HasPrefix(placeholder, "[") && strings.HasSuffix(placeholder, "]") {
		inner := strings.TrimSpace(placeholder[1 : len(placeholder)-1])
		if inner != "" && !strings.HasPrefix(inner, "<") {
			return "<" + inner + ">"
		}
		return inner
	}
	return placeholder
}

func cliGotPlaceholderSuffix(placeholders []string, got int) string {
	if got <= 0 {
		return ""
	}
	if got > len(placeholders) {
		return fmt.Sprintf(" (got %d)", got)
	}
	return " (got " + strings.Join(placeholders[:got], " ") + ")"
}

func cliExpectedArgList(placeholders []string, count int) string {
	if count <= 0 {
		return ""
	}
	if len(placeholders) >= count {
		return strings.Join(placeholders[:count], " ")
	}
	if len(placeholders) > 0 {
		return strings.Join(placeholders, " ")
	}
	return cliArgumentCountPhrase(count)
}

func cliQuotedArgs(args []string) string {
	quoted := make([]string, 0, len(args))
	for _, arg := range args {
		quoted = append(quoted, strconv.Quote(arg))
	}
	return strings.Join(quoted, " ")
}

func cliArgumentCountPhrase(count int) string {
	switch count {
	case 0:
		return "no positional arguments"
	case 1:
		return "one argument"
	case 2:
		return "two arguments"
	case 3:
		return "three arguments"
	default:
		return fmt.Sprintf("%d arguments", count)
	}
}
