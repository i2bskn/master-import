package main

import (
	"errors"
	"os"
	"path"
	"path/filepath"
)

type DataSource struct {
	Source string
	paths  []string
}

func NewDataSource(source string) *DataSource {
	return &DataSource{
		Source: filepath.Abs(source),
	}
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
		pattern := path.Join(db.Source, "**", "*.yml")
		matches, err := filepath.Glob(pattern)
		db.paths = matches
		return matches, err
	}
}
