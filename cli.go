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
	database := NewDatabase()

	flags := flag.NewFlagSet(AppName, flag.ContinueOnError)
	flags.SetOutput(cli.errStream)
	flags.Usage = func() {
		fmt.Fprint(cli.outStream, usage)
	}

	// Database
	flags.StringVar(&database.Host, "host", "", "")
	flags.StringVar(&database.Port, "port", "", "")
	flags.StringVar(&database.Socket, "socket", "", "")
	flags.StringVar(&database.Name, "db", "", "")
	flags.StringVar(&database.User, "user", "", "")
	flags.StringVar(&database.Password, "password", "", "")

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

  -host=<host name>          Connect to host
  -port=<port number>        Port number to use for connection
  -socket=<socket file path> The socket file to use for connection
  -db=<database name>        Database to use

  -user=<user name>          User name for login
  -password=<password>       Password to use when connecting to server

  -version                   Show version number and quit
  -help                      This help text
`
