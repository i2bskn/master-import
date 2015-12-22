package main

import (
	"database/sql"
	"errors"
	"fmt"
	"os/user"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

const (
	driverName    = "mysql"
	defaultHost   = "localhost"
	defaultPort   = "3306"
	defaultSocket = "/tmp/mysql.sock"
)

type Database struct {
	Host           string
	Port           string
	Socket         string
	Name           string
	User           string
	Password       string
	Params         map[string]string
	dataSourceName string
}

func NewDatabase() *Database {
	return &Database{}
}

func (db *Database) Open() (*sql.DB, error) {
	dsn, err := db.DataSourceName()
	if err != nil {
		return nil, err
	}

	return sql.Open(driverName, dsn)
}

func (db *Database) DataSourceName() (string, error) {
	// returns data source name from cache
	if len(db.dataSourceName) > 0 {
		return db.dataSourceName, nil
	}

	if db.ValidOptions() {
		user, err := db.selectUser()
		if err != nil {
			return "", err
		}

		db.dataSourceName = fmt.Sprintf("%s:%s@%s/%s%s", user,
			db.Password, db.address(), db.Name, db.dsnOptions())
		return db.dataSourceName, nil
	} else {
		return "", errors.New("Invalid options. Can't create DSN.")
	}
}

func (db *Database) ValidOptions() bool {
	for _, item := range []string{db.User, db.Name} {
		if len(item) < 1 {
			return false
		}
	}

	return true
}

func (db *Database) selectUser() (string, error) {
	if len(db.User) > 0 {
		return db.User, nil
	}

	user, err := user.Current()
	if err != nil {
		return "", err
	}
	return user.Username, nil
}

func (db *Database) address() string {
	sockSize := len(db.Socket)
	hostSize := len(db.Host)
	portSize := len(db.Port)

	if sockSize > 0 {
		return fmt.Sprintf("unix(%s)", db.Socket)
	}

	if (hostSize + portSize) > 0 {
		if hostSize == 0 {
			db.Host = defaultHost
		}

		if portSize == 0 {
			db.Port = defaultPort
		}

		return fmt.Sprintf("tcp(%s:%s)", db.Host, db.Port)
	}

	db.Socket = defaultSocket
	return fmt.Sprintf("unix(%s)", db.Socket)
}

func (db *Database) dsnOptions() string {
	if len(db.Params) < 1 {
		return ""
	}

	params := make([]string, 0, len(db.Params))
	for key, value := range db.Params {
		params = append(params, fmt.Sprintf("%s=%s", key, value))
	}
	return fmt.Sprintf("?%s", strings.Join(params, "&"))
}
