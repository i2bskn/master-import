package main

import (
	"flag"
	"fmt"
	"io"
)

const (
	AppName = "mysql-yaml-loader"
	Version = "0.0.1"
)

const (
	ExitCodeOK = iota
	ExitCodeFlagParseError
)

type CLI struct {
	outStream, errStream io.Writer
}

func (cli *CLI) Run(args []string) int {
	var version, help bool

	flags := flag.NewFlagSet(AppName, flag.ContinueOnError)
	flags.SetOutput(cli.errStream)
	flags.Usage = func() {
		fmt.Fprint(cli.outStream, usage)
	}

	flags.BoolVar(&version, "version", false, "")
	flags.BoolVar(&help, "help", false, "")

	if err := flags.Parse(args[1:]); err != nil {
		fmt.Fprintln(cli.errStream, err)
		return ExitCodeFlagParseError
	}

	if version {
		fmt.Fprintf(cli.errStream, "%s version %s\n", AppName, Version)
		return ExitCodeOK
	}

	if help {
		flags.Usage()
		return ExitCodeOK
	}

	fmt.Fprintln(cli.outStream, "mysql-yaml-loader")
	return ExitCodeOK
}

const usage = `
Usage: mysql-yaml-loader [OPTIONS] file

Load YAML data to MySQL.

Options:

  -version Show version number and quit
  -help    This help text
`
