package main

import (
	"flag"
	"fmt"
	"io"
	"path/filepath"
	"sync"
)

const (
	AppName = "mysql-yaml-loader"
	Version = "0.0.1"
)

const (
	ExitCodeOK = iota
	ExitCodeFlagParseError
	ExitCodeFileNotSpecified
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

	targets := flags.Args()
	if len(targets) == 0 {
		fmt.Fprintln(cli.errStream, "Target YAML file must be specified\n")
		return ExitCodeFileNotSpecified
	}

	ch, fin := Load(targets, database)

Loop:
	for {
		select {
		case result := <-ch:
			baseName := filepath.Base(result.Arg)
			if result.Error != nil {
				fmt.Fprintf(cli.outStream, "Failed: %s %s\n", baseName, result.Error)
			} else {
				fmt.Fprintf(cli.outStream, "Done: %s\n", baseName)
			}
		case <-fin:
			break Loop
		}
	}

	fmt.Fprintln(cli.outStream, "mysql-yaml-loader")
	return ExitCodeOK
}

type Result struct {
	Arg        string
	Error      error
	DataSource *DataSource
}

func Load(args []string, database *Database) (chan Result, chan bool) {
	ch := make(chan Result)
	fin := make(chan bool)

	go func() {
		var wg sync.WaitGroup
		wg.Add(len(args))

		for _, arg := range args {
			go func(a string) {
				result := Result{Arg: a}
				dataSource, err := NewDataSource(result.Arg)
				if err != nil {
					result.Error = err
					ch <- result
					wg.Done()
					return
				}

				result.DataSource = dataSource
				if err := database.LoadWithTransaction(dataSource); err != nil {
					result.Error = err
					ch <- result
					wg.Done()
					return
				}

				ch <- result
				wg.Done()
			}(arg)
		}

		wg.Wait()
		fin <- true
	}()
	return ch, fin
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
