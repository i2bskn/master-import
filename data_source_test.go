package main

import (
	"path/filepath"
	"testing"
)

const exampleYaml string = "example/users.yml"

func TestNewDataSource__ok(t *testing.T) {
	ds, err := NewDataSource(exampleYaml)
	if err != nil {
		t.Errorf("Expected %v, but %v", nil, err)
	}

	expected, _ := filepath.Abs(exampleYaml)
	if ds.Source != expected {
		t.Errorf("Expected %v, but %v", expected, ds.Source)
	}
}

func TestPaths__ok(t *testing.T) {
	ds, err := NewDataSource(exampleYaml)
	if err != nil {
		t.Errorf("Expected %v, but %v", nil, err)
	}

	paths, err := ds.Paths()
	if err != nil {
		t.Errorf("Expected %v, but %v", nil, err)
	}

	if paths[0] != ds.Source {
		t.Errorf("Expected %v, but %v", ds.Source, paths[0])
	}
}
