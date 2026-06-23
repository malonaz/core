package postgres

import (
	"reflect"
)

type exceptTagMatch struct {
	key   string
	value string
}

type GetDBColumnsOpts struct {
	except           map[string]struct{}
	exceptTagMatches []exceptTagMatch
}

type GetDBColumnsOption func(*GetDBColumnsOpts)

func ExceptColumns(columns ...string) GetDBColumnsOption {
	return func(o *GetDBColumnsOpts) {
		for _, c := range columns {
			o.except[c] = struct{}{}
		}
	}
}

func ExceptTagMatch(key, value string) GetDBColumnsOption {
	return func(o *GetDBColumnsOpts) {
		o.exceptTagMatches = append(o.exceptTagMatches, exceptTagMatch{key: key, value: value})
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

	for _, match := range o.exceptTagMatches {
		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)
			if tagValue, ok := field.Tag.Lookup(match.key); ok && tagValue == match.value {
				if dbTag, ok := field.Tag.Lookup("db"); ok {
					o.except[dbTag] = struct{}{}
				}
			}
		}
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
		tags = append(tags, tag)
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
