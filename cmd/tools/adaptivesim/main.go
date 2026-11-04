package main

import (
	"fmt"
	"os"

	"go.temporal.io/server/tools/adaptivesim"
)

func main() {
	if err := adaptivesim.RunTool(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
