package postgres

import (
	"reflect"
)

type GetDBColumnsOpts struct {
	except    map[string]struct{}
	namespace string
}

type GetDBColumnsOption func(*GetDBColumnsOpts)

func ExceptColumns(columns ...string) GetDBColumnsOption {
	return func(o *GetDBColumnsOpts) {
		for _, c := range columns {
			o.except[c] = struct{}{}
		}
	}
}

func WithTablePrefix(table string) GetDBColumnsOption {
	return func(o *GetDBColumnsOpts) {
		o.namespace = table
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

	tags := make([]string, 0, len(allTags))
	for _, tag := range allTags {
		if _, ok := o.except[tag]; ok {
			continue
		}
		if o.namespace != "" {
			tags = append(tags, o.namespace+"."+tag)
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
