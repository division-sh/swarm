package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"swarm/internal/apispec"
)

func main() {
	platformSpecPath := flag.String("platform-spec", filepath.Join("docs", "specs", "swarm-platform", "platform", "contracts", "platform-spec.yaml"), "Path to authoritative platform-spec.yaml")
	outPath := flag.String("out", filepath.Join("docs", "specs", "swarm-platform", "platform", "contracts", "openrpc.json"), "Path to write generated OpenRPC JSON")
	check := flag.Bool("check", false, "Validate that the generated OpenRPC JSON matches the existing output file")
	flag.Parse()

	api, err := apispec.LoadPlatformSpec(*platformSpecPath)
	if err != nil {
		fatal(err)
	}
	if report, err := apispec.Validate(api); err != nil {
		fatal(err)
	} else {
		fmt.Fprintf(os.Stderr, "api spec valid: methods=%d schemas=%d errors=%d mutating=%d subscriptions=%d\n",
			report.MethodCount,
			report.SchemaCount,
			report.ErrorCodeCount,
			report.MutatingMethodCount,
			report.SubscriptionMethodCnt,
		)
	}
	generated, err := apispec.GenerateOpenRPC(api)
	if err != nil {
		fatal(err)
	}
	if *check {
		existing, err := os.ReadFile(*outPath)
		if err != nil {
			fatal(err)
		}
		if !apispec.EqualJSON(existing, generated) {
			fatal(fmt.Errorf("generated OpenRPC differs from %s; rerun without --check", *outPath))
		}
		return
	}
	if err := os.MkdirAll(filepath.Dir(*outPath), 0o755); err != nil {
		fatal(err)
	}
	if err := os.WriteFile(*outPath, generated, 0o644); err != nil {
		fatal(err)
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
