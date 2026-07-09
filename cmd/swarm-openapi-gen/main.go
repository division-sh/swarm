package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/division-sh/swarm/internal/providerconnectors"
)

func main() {
	root := flag.String("catalog-root", "internal/providerconnectors", "Path to the provider connector catalog root")
	check := flag.Bool("check", false, "Validate checked-in connector packs and conformance evidence against pinned sources and profiles")
	flag.Parse()

	if *check {
		artifacts, err := providerconnectors.CheckGeneratedCatalog(os.DirFS(*root))
		if err != nil {
			fatal(err)
		}
		fmt.Fprintf(os.Stderr, "generated connector catalog is current: packs=%d\n", len(artifacts))
		return
	}
	if err := providerconnectors.WriteGeneratedCatalog(*root); err != nil {
		fatal(err)
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
