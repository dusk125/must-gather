package main

import (
	"github.com/openshift/must-gather/cmd/gather"
	"github.com/spf13/cobra"
)

func main() {
	root := cobra.Command{}

	root.AddCommand(gather.NewGatherCommand())

	_ = root.Execute()
}
