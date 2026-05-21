package main

import "github.com/spf13/cobra"

func bindCLIAPIConnectionFlags(cmd *cobra.Command, opts *rootCommandOptions) {
	cmd.Flags().StringVar(&opts.apiServer, "api-server", "", "Swarm API server base URL")
	cmd.Flags().StringVar(&opts.apiTokenFile, "api-token-file", "", "Path to file containing the Swarm API bearer token")
}

func cliAPIConnectionFlagsChanged(cmd *cobra.Command) bool {
	return cmd.Flags().Changed("api-server") || cmd.Flags().Changed("api-token-file")
}
