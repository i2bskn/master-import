package main

import (
	"fmt"
	"io/ioutil"
	"strings"

	"gopkg.in/yaml.v2"
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

type YAMLWrapper struct {
	FilePath    string
	contents    map[interface{}]interface{}
	columnNames map[int]string
	values      map[int]StringValue
}

func NewYAMLWrapper(filePath string) *YAMLWrapper {
	return &YAMLWrapper{
		FilePath:    filePath,
		contents:    make(map[interface{}]interface{}),
		columnNames: make(map[int]string),
		values:      make(map[int]StringValue),
	}
}

func (w *YAMLWrapper) Contents() (map[interface{}]interface{}, error) {
	if len(w.contents) == 0 {
		contents := make(map[interface{}]interface{})

		buf, err := ioutil.ReadFile(w.FilePath)
		if err != nil {
			return contents, err
		}

		if err := yaml.Unmarshal(buf, &contents); err != nil {
			return contents, err
		}

		w.contents = contents
	}

	return w.contents, nil
}

func (w *YAMLWrapper) ColumnNames() (map[int]string, error) {
	if len(w.columnNames) == 0 {
		keys := make(map[int]string)

		contents, err := w.Contents()
		if err != nil {
			return keys, err
		}

		for _, content := range contents {
			i := 0
			for key, _ := range content.(map[interface{}]interface{}) {
				keys[i] = key.(string)
				i++
			}
			break
		}

		w.columnNames = keys
	}

	return w.columnNames, nil
}

func (w *YAMLWrapper) StringValues() (map[int]StringValue, error) {
	if len(w.values) == 0 {
		var contents map[interface{}]interface{}

		columnNames, err := w.ColumnNames()
		if err != nil {
			return w.values, err
		}

		contents, err = w.Contents()
		if err != nil {
			return w.values, err
		}

		values := make(map[int]StringValue)
		contentIndex := 0
		for _, data := range contents {
			stringValue := NewStringValue()
			for i, name := range columnNames {
				err := stringValue.SetValue(i, data.(map[interface{}]interface{})[name])
				if err != nil {
					return w.values, err
				}
			}
			values[contentIndex] = stringValue
			contentIndex++
		}
		w.values = values
	}

	return w.values, nil
}
