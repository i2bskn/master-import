package main

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
)

type DataSource struct {
	Source string
	paths  []string
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
	if len(ds.paths) > 0 {
		return ds.paths, nil
	}

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
	return ds.paths, nil
}
