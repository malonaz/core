package postgres

import (
	"reflect"
)

// GetDBColumns is used to get all the db tags of a struct. This can be used to
// construct a Retrieve Query and avoid maintaining a list of column names in parallel
// to the struct's db tags. This could have been bundled into a postgres.RetrieveQuery
// helper function, but we don't want to recompute this for every query so users should
// call this once at the top-level of their datastore package.
func GetDBColumns(object any) []string {
	t := reflect.TypeOf(object)
	tags := make([]string, 0, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		if t.Field(i).PkgPath != "" {
			// skip unimported field
			continue
		}
		tag, ok := t.Field(i).Tag.Lookup("db")
		if !ok {
			continue
		}
		tags = append(tags, tag)
	}
	return tags
}
