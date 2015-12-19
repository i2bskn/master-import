package main

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

const driverName = "mysql"

type Database struct {
	Host           string
	Port           string
	Socket         string
	User           string
	Password       string
	Database       string
	Params         map[string]string
	dataSourceName string
}

func NewDatabase() *Database {
	return &Database{}
}

func (db *Database) Open() (*sql.DB, error) {
	return sql.Open(driverName, db.DataSourceName())
}

func (db *Database) DataSourceName() (string, error) {
	// returns data source name from cache
	if len(db.dataSourceName) > 0 {
		return db.dataSourceName
	}

	if db.ValidOptions() {
		db.dataSourceName = fmt.Sprintf("%s:%s@%s/%s%s", db.User,
			db.Password, db.address(), db.Database, db.dsnOptions())
		return db.DataSourceName, nil
	} else {
		return "", errors.New("Invalid options. Can't create DSN.")
	}
}

func (db *Database) ValidOptions() bool {
	for _, item := range []string{db.User, db.Database} {
		if len(item) < 1 {
			return false
		}
	}

	if len(db.Socket) < 1 {
		for _, item := range []string{db.Host, db.Port} {
			if len(item) < 1 {
				return false
			}
		}
	}

	return true
}

func (db *Database) address() string {
	if len(db.Socket) > 0 {
		return fmt.Sprintf("unix(%s)", db.Socket)
	} else {
		return fmt.Sprintf("tcp(%s:%s)", db.Host, db.Port)
	}
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
