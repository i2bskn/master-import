package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"os/user"
	"path/filepath"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

const (
	AppName            = "master-import"
	defaultBaseDirName = "master"
	tableNameDelimiter = ","
	extDelimiter       = "."
)

const (
	driverName    = "mysql"
	defaultHost   = "localhost"
	defaultPort   = "3306"
	defaultSocket = "/tmp/mysql.sock"
)

const (
	ExitCodeOK = iota
	ExitCodeError
)

var (
	queryValueSize int = 3
)

type StringValue struct {
	Values map[int]string
}

func NewStringValue() StringValue {
	return StringValue{
		Values: make(map[int]string),
	}
}

func (sv StringValue) SetValue(index int, arg interface{}) error {
	var value string
	switch arg.(type) {
	case string:
		value = strings.Join([]string{"\"", arg.(string), "\""}, "")
	case int, float64:
		value = fmt.Sprint(arg)
	case nil:
		value = "NULL"
	default:
		return fmt.Errorf("Unexpected value: %v", arg)
	}

	sv.Values[index] = value
	return nil
}

type DataSource struct {
	Source       string
	TableName    string
	sourceFiles  []string
	columnNames  map[int]string
	stringValues []StringValue
}

func NewDataSource(source string) (*DataSource, error) {
	abs, err := filepath.Abs(source)
	if err != nil {
		return nil, err
	}

	baseName := filepath.Base(abs)
	elements := strings.Split(baseName, extDelimiter)
	tableName := elements[0]

	return &DataSource{
		Source:      abs,
		TableName:   tableName,
		columnNames: make(map[int]string),
	}, nil
}

func (ds *DataSource) SourceFiles() ([]string, error) {
	if len(ds.sourceFiles) == 0 {
		src, err := os.Stat(ds.Source)
		if err != nil {
			return []string{}, fmt.Errorf("Source not found: %s", ds.Source)
		}

		if src.IsDir() {
			pattern := filepath.Join(ds.Source, "*.json")
			matches, err := filepath.Glob(pattern)
			if err != nil {
				return matches, err
			}
			if len(matches) == 0 {
				return matches, fmt.Errorf("Empty source: %s", ds.Source)
			}
			ds.sourceFiles = matches
			return matches, nil
		}

		ds.sourceFiles = []string{ds.Source}
	}

	return ds.sourceFiles, nil
}

func (ds *DataSource) ColumnNames() (map[int]string, error) {
	if len(ds.columnNames) == 0 {
		sources, err := ds.SourceFiles()
		if err != nil {
			return ds.columnNames, err
		}

		for _, source := range sources {
			bytes, err := ioutil.ReadFile(source)
			if err != nil {
				return ds.columnNames, err
			}

			data := make(map[string]interface{})
			err = json.Unmarshal(bytes, &data)
			if err != nil {
				return ds.columnNames, err
			}

			idx := 0
			for name, _ := range data {
				ds.columnNames[idx] = name
				idx++
			}
			break
		}
	}

	return ds.columnNames, nil
}

func (ds *DataSource) StringValues() ([]StringValue, error) {
	if len(ds.stringValues) == 0 {
		sources, err := ds.SourceFiles()
		if err != nil {
			return ds.stringValues, err
		}

		names, err := ds.ColumnNames()
		if err != nil {
			return ds.stringValues, err
		}

		for _, source := range sources {
			bytes, err := ioutil.ReadFile(source)
			if err != nil {
				return ds.stringValues, err
			}

			data := make(map[string]interface{})
			err = json.Unmarshal(bytes, &data)
			if err != nil {
				return ds.stringValues, err
			}

			stringValue := NewStringValue()
			for i, name := range names {
				stringValue.SetValue(i, data[name])
			}

			ds.stringValues = append(ds.stringValues, stringValue)
		}
	}

	return ds.stringValues, nil
}

type QueryBuilder struct {
	dataSource *DataSource
}

func NewQueryBuilder(dataSource *DataSource) QueryBuilder {
	return QueryBuilder{
		dataSource: dataSource,
	}
}

func (builder *QueryBuilder) sqlColumns() (string, error) {
	var sqlElement string

	columnNames, err := builder.dataSource.ColumnNames()
	if err != nil {
		return sqlElement, err
	}

	if len(columnNames) > 0 {
		sqlElement = columnNames[0]
		for i := 1; i < len(columnNames); i++ {
			sqlElement += ", " + columnNames[i]
		}
		return fmt.Sprintf("(%s)", sqlElement), nil
	}

	return sqlElement, fmt.Errorf("Column names can not acquired: %s", builder.dataSource.TableName)
}

func (builder *QueryBuilder) sqlValues() ([]string, error) {
	sqlElements := make([]string, 0, 0)

	stringValues, err := builder.dataSource.StringValues()
	if err != nil {
		return sqlElements, err
	}

	for _, stringValue := range stringValues {
		var sqlElement string

		for i := 0; i < len(stringValue.Values); i++ {
			if i == 0 {
				sqlElement = stringValue.Values[i]
			} else {
				sqlElement += ", " + stringValue.Values[i]
			}
		}

		sqlElements = append(sqlElements, fmt.Sprintf("(%s)", sqlElement))
	}

	return sqlElements, nil
}

func (builder QueryBuilder) TruncateQuery() string {
	return fmt.Sprintf("TRUNCATE TABLE %s", builder.dataSource.TableName)
}

func (builder QueryBuilder) InsertQueries() (map[int]string, error) {
	queries := make(map[int]string)
	table := builder.dataSource.TableName
	sqlColumns, err := builder.sqlColumns()
	if err != nil {
		return queries, err
	}

	sqlValues, err := builder.sqlValues()
	if err != nil {
		return queries, err
	}

	insertQuerySize := int(math.Ceil(float64(len(sqlValues)) / float64(queryValueSize)))
	for i := 0; i < insertQuerySize; i++ {
		f := queryValueSize * i
		l := queryValueSize * (i + 1)
		if l > len(sqlValues) {
			l = len(sqlValues)
		}
		values := strings.Join(sqlValues[f:l], ",")
		queries[i] = fmt.Sprintf("INSERT INTO %s %s VALUES %s", table, sqlColumns, values)
	}
	return queries, err
}

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

func (db *Database) LoadWithTransaction(dataSource *DataSource) error {
	var sqlDB *sql.DB
	var tx *sql.Tx

	queryBuilder := NewQueryBuilder(dataSource)
	insertQueries, err := queryBuilder.InsertQueries()
	if err != nil {
		return err
	}

	sqlDB, err = db.Open()
	if err != nil {
		return err
	}
	defer sqlDB.Close()

	tx, err = sqlDB.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err := recover(); err != nil {
			tx.Rollback()
		}
	}()

	_, err = tx.Exec(queryBuilder.TruncateQuery())
	if err != nil {
		panic(err)
	}

	for i := 0; i < len(insertQueries); i++ {
		_, err = tx.Exec(insertQueries[i])
		if err != nil {
			panic(err)
		}
	}

	err = tx.Commit()
	if err != nil {
		panic(err)
	}

	return nil
}

func (db *Database) Open() (*sql.DB, error) {
	dsn, err := db.DataSourceName()
	if err != nil {
		return nil, err
	}

	return sql.Open(driverName, dsn)
}

func (db *Database) DataSourceName() (string, error) {
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
	for _, item := range []string{db.Name} {
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

func getBaseDir(path string) string {
	if len(path) > 0 {
		abs, err := filepath.Abs(path)
		if err != nil {
			panic(err)
		}

		return abs
	}

	current, err := os.Getwd()
	if err != nil {
		panic(err)
	}

	return filepath.Join(current, defaultBaseDirName)
}

func targetDataSources(path string, tableNames []string) []*DataSource {
	sources, err := ioutil.ReadDir(path)
	if err != nil {
		panic(err)
	}

	if len(sources) == 0 {
		fmt.Printf("source directories not found: %s\n", path)
		os.Exit(ExitCodeError)
	}

	var dataSources []*DataSource
	for _, source := range sources {
		if source.IsDir() {
			dataSource, err := NewDataSource(filepath.Join(path, source.Name()))
			if err != nil {
				panic(err)
			}

			if len(tableNames) == 0 {
				dataSources = append(dataSources, dataSource)
				continue
			}

			for _, name := range tableNames {
				if dataSource.TableName == name {
					dataSources = append(dataSources, dataSource)
				}
			}
		}
	}

	if len(tableNames) > 0 && len(dataSources) != len(tableNames) {
		for _, name := range tableNames {
			notFound := true
			for _, dataSource := range dataSources {
				if name == dataSource.TableName {
					notFound = false
				}
			}

			if notFound {
				fmt.Printf("source directory not found: %s\n", filepath.Join(path, name))
			}
		}
		os.Exit(ExitCodeError)
	}

	return dataSources
}

func LoadSources(database *Database, dataSources []*DataSource) {
	for _, dataSource := range dataSources {
		if err := database.LoadWithTransaction(dataSource); err != nil {
			panic(err)
		}
	}
}

func main() {
	var basedir, tableStr string
	database := NewDatabase()

	flags := flag.NewFlagSet(AppName, flag.ContinueOnError)
	flags.SetOutput(os.Stderr)

	flags.StringVar(&database.Host, "host", "", "database hostname")
	flags.StringVar(&database.Port, "port", "", "database port")
	flags.StringVar(&database.Socket, "socket", "", "database socket")
	flags.StringVar(&database.Name, "db", "", "database name")
	flags.StringVar(&database.User, "user", "", "database user")
	flags.StringVar(&database.Password, "password", "", "database password")

	flags.StringVar(&basedir, "basedir", "", "base directory")
	flags.StringVar(&tableStr, "tables", "", "target tables")

	if err := flags.Parse(os.Args[1:]); err != nil {
		panic(err)
	}

	baseDir := getBaseDir(basedir)
	if _, err := os.Stat(baseDir); err != nil {
		fmt.Printf("basedir not found: %s\n", baseDir)
		os.Exit(ExitCodeError)
	}

	names := make([]string, 0, 0)
	if len(tableStr) > 0 {
		names = strings.Split(tableStr, tableNameDelimiter)
	}

	dataSources := targetDataSources(baseDir, names)
	LoadSources(database, dataSources)
}
