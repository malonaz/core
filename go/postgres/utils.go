package postgres

import (
	"reflect"
)

// GetDBColumns is used to get all the db tags of a struct by creating a slice of pointers to the items of the struct.
// It ensures that the object is a struct passed by value and panics otherwise.
func GetDBColumns(object any, except ...string) []string {
	// Building exception set
	exceptionSet := make(map[string]struct{}, len(except))
	for _, v := range except {
		exceptionSet[v] = struct{}{}
	}

	t := reflect.TypeOf(object)

	// Panic if the object is not a struct or a pointer to a struct (being strict about requiring struct by value)
	if t.Kind() != reflect.Struct {
		panic("object must be a struct passed by value")
	}

	// Make a slice of pointers to the type of object
	sliceType := reflect.SliceOf(reflect.PtrTo(t))
	slice := reflect.New(sliceType).Elem() // Create a new slice to hold pointers to the struct type

	// Append a new pointer to a copy of the object to the slice
	instancePtr := reflect.New(t)
	instancePtr.Elem().Set(reflect.ValueOf(object))
	slice = reflect.Append(slice, instancePtr)

	// Get tags from the adjusted slice (pointer-based)
	allTags, _ := getParams(slice, nil)

	// Filter tags based on exceptions
	tags := make([]string, 0, len(allTags))
	for _, tag := range allTags {
		if _, ok := exceptionSet[tag]; !ok {
			tags = append(tags, tag)
		}
	}
	return tags
}
