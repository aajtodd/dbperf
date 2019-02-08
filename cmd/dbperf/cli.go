package main

import (
	"flag"
	"runtime"
)

// CliArgs holds the command line interface arguments that were given
type CliArgs struct {
	nworkers int
	filename string
}

// Register the flags with the given flagset
func (cli *CliArgs) Register(fs *flag.FlagSet) {
	fs.IntVar(&cli.nworkers, "n", runtime.NumCPU(), "number of concurrent workers")
	fs.StringVar(&cli.filename, "f", "", "path to input file containing queries to execute")
}
