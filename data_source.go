package main

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
)

const extDelimiter string = "."

type DataSource struct {
	Source       string
	paths        []string
	tableName    string
	columnNames  map[int]string
	wrappers     []*YAMLWrapper
	stringValues map[int]StringValue
}

func NewDataSource(source string) (*DataSource, error) {
	absPath, err := filepath.Abs(source)
	if err != nil {
		return nil, err
	}

	return &DataSource{
		Source: absPath,
	}, nil
}

func (ds *DataSource) Paths() ([]string, error) {
	if len(ds.paths) == 0 {
		src, err := os.Stat(ds.Source)
		if err != nil {
			return []string{}, fmt.Errorf("Source not found: %s", ds.Source)
		}

		if src.IsDir() {
			pattern := path.Join(ds.Source, "**", "*.yml")
			matches, err := filepath.Glob(pattern)
			ds.paths = matches
			return matches, err
		}

		ds.paths = []string{ds.Source}
	}

	return ds.paths, nil
}

func (ds *DataSource) TableName() string {
	if len(ds.tableName) == 0 {
		base := filepath.Base(ds.Source)
		elements := strings.Split(base, extDelimiter)
		ds.tableName = elements[0]
	}

	return ds.tableName
}

func (ds *DataSource) Wrappers() ([]*YAMLWrapper, error) {
	if len(ds.wrappers) == 0 {
		wrappers := []*YAMLWrapper{}

		paths, err := ds.Paths()
		if err != nil {
			return wrappers, err
		}

		for _, path := range paths {
			wrappers = append(wrappers, NewYAMLWrapper(path))
		}

		ds.wrappers = wrappers
	}

	return ds.wrappers, nil
}

func (ds *DataSource) ColumnNames() (map[int]string, error) {
	if len(ds.columnNames) == 0 {
		emptyNames := make(map[int]string)

		wrappers, err := ds.Wrappers()
		if err != nil {
			return emptyNames, err
		}

		for _, wrapper := range wrappers {
			names, _ := wrapper.ColumnNames()
			if len(names) > 1 {
				ds.columnNames = names
				break
			}
		}
	}

	return ds.columnNames, nil
}

func (ds *DataSource) StringValues() (map[int]StringValue, error) {
	if len(ds.stringValues) == 0 {
		values := make(map[int]StringValue)

		wrappers, err := ds.Wrappers()
		if err != nil {
			return values, err
		}

		index := 0
		for _, wrapper := range wrappers {
			individualValues, err := wrapper.StringValues()
			if err != nil {
				return values, err
			}

			for i, v := range individualValues {
				values[index+i] = v
			}

			index += len(values)
		}

		ds.stringValues = values
	}

	return ds.stringValues, nil
}
