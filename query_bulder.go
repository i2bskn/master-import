package main

import (
	"fmt"
)

type QueryBuilder struct {
	dataSource *DataSource
}

func NewQueryBuilder(dataSource *DataSource) *QueryBuilder {
	return &QueryBuilder{
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

	return sqlElement, fmt.Errorf("Column names can not acquired: %s", builder.dataSource.TableName())
}

func (builder *QueryBuilder) sqlValues() (string, error) {
	var sqlElement string

	stringValues, err := builder.dataSource.StringValues()
	if err != nil {
		return sqlElement, err
	}

	if len(stringValues) > 0 {
		for i := 0; i < len(stringValues); i++ {
			values := stringValues[i].Values
			var sqlValue string
			for j := 0; j < len(values); j++ {
				if j == 0 {
					sqlValue = values[j]
				} else {
					sqlValue += ", " + values[j]
				}
			}

			if i == 0 {
				sqlElement = fmt.Sprintf("(%s)", sqlValue)
			} else {
				sqlElement += ", " + fmt.Sprintf("(%s)", sqlValue)
			}
		}
	}

	return sqlElement, nil
}

func (builder *QueryBuilder) TruncateQuery() string {
	return fmt.Sprintf("DELETE FROM %s", builder.dataSource.TableName())
}

func (builder *QueryBuilder) InsertQueries() (map[int]string, error) {
	var sqlColumns string
	var sqlValues string
	var err error
	queries := make(map[int]string)

	sqlColumns, err = builder.sqlColumns()
	if err != nil {
		return queries, err
	}

	table := builder.dataSource.TableName()
	sqlValues, err = builder.sqlValues()
	if err != nil {
		return queries, err
	}

	queries[0] = fmt.Sprintf("INSERT INTO %s %s VALUES %s", table, sqlColumns, sqlValues)

	return queries, nil
}

func (builder *QueryBuilder) CountQuery() string {
	return fmt.Sprintf("SELECT COUNT(1) FROM %s", builder.dataSource.TableName())
}

func (builder *QueryBuilder) ResetAutoIncrementQuery(count int) string {
	return fmt.Sprintf("ALTER TABLE %s AUTO_INCREMENT = %d", builder.dataSource.TableName(), count)
}
