package postgres

import (
	"fmt"
	"reflect"
	"strings"
)

// InsertQuery takes an sqlQueryTemplate of the form 'INSERT INTO table_name %s VALUES %s',
// an object to insert as well as the dbColumns which should map 1-to-1 with the object's db tags.
// It will return a query and an array of params that can be used directly with db.Exec(query, params)
// or tx.Exec(query, params). This method will panic if one of the dbColumns is not a valid tag of an object to insert.
func InsertQuery(sqlQueryTemplate string, objectToInsert any, dbColumns ...string) (string, []any) {
	t := reflect.TypeOf(objectToInsert)
	slice := reflect.Append(reflect.MakeSlice(reflect.SliceOf(t), 0, 1), reflect.ValueOf(objectToInsert))
	tags, params := getParams(slice, dbColumns)
	if len(dbColumns) == 0 {
		dbColumns = tags
	}
	query := generateInsertQuery(sqlQueryTemplate, dbColumns, 1)
	return query, params
}

// BatchInsertQuery takes an sqlQueryTemplate of the form 'INSERT INTO table_name %s VALUES %s',
// a slice of objects to insert as well as the dbColumns which should map 1-to-1 with the object's db tags.
// It will return a query and an array of params that can be used directly with db.Exec(query, params)
// or tx.Exec(query, params). This method will panic if one of the dbColumns is not a valid tag of an object to insert.
func BatchInsertQuery(sqlQueryTemplate string, objectsToInsertSlice any, dbColumns ...string) (string, []any) {
	objectsToInsertSliceValue := reflect.ValueOf(objectsToInsertSlice)
	tags, params := getParams(objectsToInsertSliceValue, dbColumns)
	if len(dbColumns) == 0 {
		dbColumns = tags
	}
	query := generateInsertQuery(sqlQueryTemplate, dbColumns, objectsToInsertSliceValue.Len())
	return query, params
}

func generateInsertQuery(template string, columns []string, numObjects int) string {
	columnNames := "(" + strings.Join(columns, ",") + ")"
	paramPlaceholders := strings.Builder{}
	for i := 0; i < numObjects; i++ {
		paramPlaceholders.WriteString(fmt.Sprintf("($%d", i*len(columns)+1))
		for j := 1; j < len(columns); j++ {
			paramPlaceholders.WriteString(fmt.Sprintf(",$%d", i*len(columns)+j+1))
		}
		paramPlaceholders.WriteByte(')')
		if i < numObjects-1 {
			paramPlaceholders.WriteByte(',')
		}
	}
	return fmt.Sprintf(template, columnNames, paramPlaceholders.String())
}

// GetParams returns the params for the given db columns.
func GetParams(object any, dbColumns ...string) []any {
	t := reflect.TypeOf(object)
	slice := reflect.Append(reflect.MakeSlice(reflect.SliceOf(t), 0, 1), reflect.ValueOf(object))
	_, params := getParams(slice, dbColumns)
	return params
}

func getParams(objects reflect.Value, dbColumns []string) ([]string, []any) {
	// Initialize parameters container and column names list
	params := make([]any, 0)
	var columns []string

	if len(dbColumns) == 0 {
		// We will collect column names dynamically if not provided
		columns = collectColumnNames(objects.Index(0).Elem())
	} else {
		columns = dbColumns
	}

	for i := 0; i < objects.Len(); i++ {
		object := objects.Index(i).Elem()
		objectParams := extractParams(object, columns)
		params = append(params, objectParams...)
	}
	if len(dbColumns) > 0 {
		return []string{}, params
	}
	return columns, params
}

func collectColumnNames(object reflect.Value) []string {
	var columns []string
	collectColumnNamesRecursive(object, &columns)
	return columns
}

func collectColumnNamesRecursive(object reflect.Value, columns *[]string) {
	t := object.Type()
	for i := 0; i < object.NumField(); i++ {
		field := object.Field(i)
		fieldInfo := t.Field(i)

		if fieldInfo.PkgPath != "" || !field.CanInterface() {
			continue
		}

		if fieldInfo.Anonymous && field.Kind() == reflect.Struct {
			collectColumnNamesRecursive(field, columns)
		} else {
			tag, exists := fieldInfo.Tag.Lookup("db")
			if exists {
				*columns = append(*columns, tag)
			}
		}
	}
}

func extractParams(object reflect.Value, columns []string) []any {
	objParams := make([]any, len(columns))
	for i, column := range columns {
		value, found := findFieldByTag(object, column)
		if !found {
			panic(fmt.Errorf("No field with the tag %s", column))
		}
		objParams[i] = value
	}
	return objParams
}

func findFieldByTag(object reflect.Value, tagToFind string) (interface{}, bool) {
	return findFieldByTagRecursive(object, tagToFind)
}

func findFieldByTagRecursive(object reflect.Value, tagToFind string) (interface{}, bool) {
	t := object.Type()
	for i := 0; i < object.NumField(); i++ {
		field := object.Field(i)
		fieldInfo := t.Field(i)

		if fieldInfo.PkgPath != "" || !field.CanInterface() {
			continue
		}

		if fieldInfo.Anonymous && field.Kind() == reflect.Struct {
			if value, found := findFieldByTagRecursive(field, tagToFind); found {
				return value, found
			}
		} else {
			tag, exists := fieldInfo.Tag.Lookup("db")
			if exists && tag == tagToFind {
				return field.Interface(), true
			}
		}
	}
	return nil, false
}
