package postgres

import (
	"reflect"
	"strings"
)

type GetDBColumnsOpts struct {
	except      map[string]struct{}
	unqualified bool
}

type GetDBColumnsOption func(*GetDBColumnsOpts)

func ExceptColumns(columns ...string) GetDBColumnsOption {
	return func(o *GetDBColumnsOpts) {
		for _, c := range columns {
			o.except[c] = struct{}{}
		}
	}
}

// WithUnqualifiedColumns strips the schema.table. prefix, returning bare column names.
func WithUnqualifiedColumns() GetDBColumnsOption {
	return func(o *GetDBColumnsOpts) {
		o.unqualified = true
	}
}

// GetDBColumns returns all db tags of a struct passed by value.
func GetDBColumns(object any, opts ...GetDBColumnsOption) []string {
	o := &GetDBColumnsOpts{except: map[string]struct{}{}}
	for _, opt := range opts {
		opt(o)
	}

	t := reflect.TypeOf(object)
	if t.Kind() != reflect.Struct {
		panic("object must be a struct passed by value")
	}

	sliceType := reflect.SliceOf(reflect.PointerTo(t))
	slice := reflect.New(sliceType).Elem()
	instancePtr := reflect.New(t)
	instancePtr.Elem().Set(reflect.ValueOf(object))
	slice = reflect.Append(slice, instancePtr)

	allTags, _ := getParams(slice, nil)

	var dbTagToUnqualified map[string]string
	if o.unqualified {
		dbTagToUnqualified = buildDBTagToUnqualified(t)
	}

	tags := make([]string, 0, len(allTags))
	for _, tag := range allTags {
		if _, ok := o.except[tag]; ok {
			continue
		}
		if o.unqualified {
			if unqualified, ok := dbTagToUnqualified[tag]; ok {
				tags = append(tags, unqualified)
			} else {
				tags = append(tags, tag)
			}
		} else {
			tags = append(tags, tag)
		}
	}
	return tags
}

func AddToWhereClause(whereClause, newClause string) string {
	if whereClause == "" {
		whereClause = "WHERE (" + newClause + ")"
	} else {
		whereClause += " AND (" + newClause + ")"
	}
	return whereClause
}

// UnqualifyColumn strips the schema.table. prefix from a fully qualified column name.
func UnqualifyColumn(column string) string {
	parts := strings.Split(column, ".")
	return parts[len(parts)-1]
}
