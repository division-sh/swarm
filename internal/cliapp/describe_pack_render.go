package cliapp

import (
	"fmt"
	"io"
	"strings"

	"github.com/division-sh/swarm/internal/packs"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

func writeDescribePackList(out io.Writer, inventory packInventoryReadback) {
	for _, entry := range inventory.Packs {
		fmt.Fprintf(out, "%s  %s  %s:%s\n", entry.ID, entry.Version, entry.Source, entry.Directory)
	}
	if len(inventory.Packs) == 0 {
		fmt.Fprintln(out, "No packs selected.")
	}
}

func writePackDescription(out io.Writer, description PackDescription, opts describePackOptions) {
	if opts.consume {
		for _, fragment := range description.Fragments {
			fmt.Fprintf(out, "# %s (%s; scope=%s)\n%s\n", fragment.File, fragment.Purpose, packDescriptionScope(fragment.Scope), fragment.YAML)
		}
		return
	}
	fmt.Fprintf(out, "%s -- provider pack\n", description.ID)
	for _, component := range description.Components {
		fmt.Fprintf(out, "loaded from %s:%s (scope=%s, version=%s)\n", component.Source, component.Directory, packDescriptionScope(component.Scope), component.Version)
		if opts.tool == "" {
			writePackReceive(out, component.Receive)
		}
		writePackSend(out, component.Tools, opts.tool != "")
	}
	writePackProjectRequirements(out, description.Project)
	if opts.tool == "" {
		fmt.Fprintf(out, "\nNext: swarm describe pack %s --consume\n", description.ID)
		for _, component := range description.Components {
			if len(component.Tools) > 0 {
				fmt.Fprintf(out, "      swarm describe pack %s --tool %s\n", description.ID, component.Tools[0].ID)
				break
			}
		}
	}
}

func packDescriptionScope(scope string) string {
	if scope == "" {
		return "installed"
	}
	return scope
}

func writePackReceive(out io.Writer, receive *PackReceiveDescription) {
	if receive == nil {
		fmt.Fprintln(out, "\nRECEIVE -- none (connector-only pack)")
		return
	}
	fmt.Fprintf(out, "\nRECEIVE -- %s trigger\n", receive.Transport)
	if receive.Transport == packs.ChannelTransportWebhook {
		fmt.Fprintf(out, "  route      POST /webhooks/{ingress-alias}/%s\n", receive.Identity.ID)
	}
	fmt.Fprintf(out, "  source     %s\n  signature  %s", receive.SourcePath, receive.Signature.Authentication)
	if receive.Signature.Header != "" {
		fmt.Fprintf(out, " (%s header)", receive.Signature.Header)
	}
	fmt.Fprintln(out)
	if len(receive.Secrets) > 0 {
		fmt.Fprintf(out, "  secret     %s (evaluated per target only)\n", strings.Join(receive.Secrets, ", "))
	}
	hasNormalized := false
	for _, event := range receive.Events {
		fmt.Fprintf(out, "  emits %s -- %s", event.Event, event.Kind)
		if event.Kind == "normalized" {
			hasNormalized = true
			var fields []string
			for _, field := range event.Fields {
				label := field.Name + ":" + field.Type
				if !field.Required {
					label += "?"
				}
				fields = append(fields, label)
			}
			fmt.Fprintf(out, " (%s)", strings.Join(fields, ", "))
		}
		fmt.Fprintln(out)
	}
	if hasNormalized {
		fmt.Fprintln(out, "  raw -- full provider payload; advanced use. The normalized events above are the supported consumption path.")
	} else {
		fmt.Fprintln(out, "  raw -- full provider payload; advanced use. This component declares no normalized outputs.")
	}
}

func writePackSend(out io.Writer, tools []PackToolDescription, detail bool) {
	if len(tools) == 0 {
		fmt.Fprintln(out, "\nSEND -- none (no connector tools in this component)")
		return
	}
	fmt.Fprintf(out, "\nSEND -- connector tools (%d)\n", len(tools))
	for index, tool := range tools {
		if !detail && index == 8 {
			fmt.Fprintf(out, "  ... %d more; use --json for the complete surface or --tool <canonical-id>\n", len(tools)-index)
			break
		}
		summary, _, _ := strings.Cut(tool.Description, "\n")
		fmt.Fprintf(out, "  %s  %s\n", tool.ID, summary)
		if detail {
			writePackToolDetail(out, tool)
		}
	}
}

func writePackToolDetail(out io.Writer, tool PackToolDescription) {
	fmt.Fprintf(out, "  source: %s (%s)\n", tool.SourcePath, tool.Identity.Provenance)
	writePackSchemaFields(out, "INPUT", tool.Input)
	writePackSchemaFields(out, "OUTPUT", tool.Output)
	fmt.Fprintf(out, "  EXECUTION\n    effect: %s (%s handler)\n", tool.Category, tool.Handler)
	if tool.Permission != "" {
		fmt.Fprintf(out, "    permission: %s\n", tool.Permission)
	}
	for _, capability := range tool.Capabilities {
		if capability.Code == packs.CapabilityJournalAttempts {
			fmt.Fprintln(out, "    journal: runtime activity attempts")
		}
	}
	for _, guarantee := range tool.Guarantees {
		if guarantee.Code == packs.GuaranteeNoAutomaticWriteRetry {
			fmt.Fprintf(out, "    retry: no automatic write retry (owner %s)\n", guarantee.EnforcedBy)
		}
	}
	fmt.Fprintf(out, "    credentials: %s\n", strings.Join(tool.Credentials, ", "))
	if tool.Managed != "" {
		fmt.Fprintf(out, "    managed credential: %s\n", tool.Managed)
	}
}

func writePackSchemaFields(out io.Writer, label string, schema runtimecontracts.ToolInputSchema) {
	fmt.Fprintf(out, "  %s (%s)\n", label, schema.Kind())
	for _, name := range schema.PropertyNames() {
		field, _ := schema.Property(name)
		required := "optional"
		if schema.IsRequired(name) {
			required = "required"
		}
		fmt.Fprintf(out, "    %s  %s  %s\n", name, field.Kind(), required)
	}
}

func writePackProjectRequirements(out io.Writer, project PackProjectDescription) {
	label := "REQUIREMENTS"
	if project.Evaluated {
		label = "THIS PROJECT"
	}
	fmt.Fprintf(out, "\n%s\n", label)
	if len(project.Rows) == 0 {
		fmt.Fprintln(out, "  none declared")
	}
	for _, row := range project.Rows {
		target := row.Target
		if target == "" {
			target = packDescriptionScope(row.Scope)
		}
		fmt.Fprintf(out, "  %s [%s]: %s", row.Name, target, row.Status)
		if row.Remediation != "" {
			fmt.Fprintf(out, " -- %s", row.Remediation)
		}
		fmt.Fprintln(out)
	}
}
