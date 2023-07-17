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

func getParams(objects reflect.Value, dbColumns []string) ([]string, []any) {
	params := make([]any, 0, len(dbColumns)*objects.Len())
	tags := make([]string, 0, objects.Index(0).Elem().NumField())
	for i := 0; i < objects.Len(); i++ {
		object := objects.Index(i).Elem()
		t := reflect.TypeOf(objects.Index(i).Interface())

		dbTags := make(map[string]any, object.NumField())
		for j := 0; j < object.NumField(); j++ {
			if t.Elem().Field(j).PkgPath != "" {
				// skip unexported field.
				continue
			}
			tag, ok := t.Elem().Field(j).Tag.Lookup("db")
			if !ok {
				continue
			}
			if len(dbColumns) == 0 {
				if i == 0 {
					tags = append(tags, tag)
				}
				params = append(params, object.Field(j).Interface())
				continue
			}
			dbTags[tag] = object.Field(j).Interface()
		}
		for _, dbColumn := range dbColumns {
			param, ok := dbTags[dbColumn]
			if !ok {
				log.Panicf("%s has no field with the tag %s", t, dbColumn)
			}
			params = append(params, param)
		}
	}
	return tags, params
}
