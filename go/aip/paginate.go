package aip

import (
	"context"
	"fmt"
	"iter"
	"reflect"

	"google.golang.org/grpc"
)

// apiCallReflection calls the apiCall function using reflection.
func apiCallReflection(ctx context.Context, apiCallFunc any, req any, opts ...grpc.CallOption) (response any, err error) {
	funcValue := reflect.ValueOf(apiCallFunc)

	if funcValue.Kind() != reflect.Func || funcValue.Type().NumIn() < 2 {
		return nil, fmt.Errorf("apiCall must be a function with at least 2 parameters")
	}

	if funcValue.Type().In(0) != reflect.TypeFor[context.Context]() {
		return nil, fmt.Errorf("first argument of apiCall must be context.Context")
	}

	in := []reflect.Value{
		reflect.ValueOf(ctx),
		reflect.ValueOf(req),
	}

	for _, opt := range opts {
		in = append(in, reflect.ValueOf(opt))
	}

	responseValues := funcValue.Call(in)

	if len(responseValues) != 2 {
		return nil, fmt.Errorf("expected apiCall to return two values, got %d", len(responseValues))
	}

	response = responseValues[0].Interface()
	errInterface := responseValues[1].Interface()
	if errInterface != nil {
		err = errInterface.(error)
	}

	return response, err
}

// Paginate abstracts the pagination logic for a given API and assumes that the request object
// has 'PageToken' and 'PageSize' fields, and the response object has a 'GetNextPageToken' method and an
// items field with a slice of items.
func Paginate[T any](ctx context.Context, request, apiCallFunction any, opts ...grpc.CallOption) ([]T, error) {
	var allItems []T
	for items, err := range PageIterator[T](ctx, request, apiCallFunction, opts...) {
		if err != nil {
			return nil, err
		}
		allItems = append(allItems, items...)
	}
	return allItems, nil
}

func Iterator[T any](ctx context.Context, request, apiCallFunction any, opts ...grpc.CallOption) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		for items, err := range PageIterator[T](ctx, request, apiCallFunction, opts...) {
			if err != nil {
				var zero T
				yield(zero, err)
				return
			}
			for _, item := range items {
				if !yield(item, nil) {
					return
				}
			}
		}
	}
}

// PageIterator returns an iterator that yields slices of T, one per page.
func PageIterator[T any](ctx context.Context, request, apiCallFunction any, opts ...grpc.CallOption) iter.Seq2[[]T, error] {
	return func(yield func([]T, error) bool) {
		reqValue := reflect.ValueOf(request)
		if reqValue.Kind() != reflect.Pointer || reqValue.Elem().Kind() != reflect.Struct {
			yield(nil, fmt.Errorf("request must be a pointer to a struct"))
			return
		}
		reqValue = reqValue.Elem()

		if !reqValue.FieldByName("PageToken").IsValid() {
			yield(nil, fmt.Errorf("request does not contain required 'PageToken' field"))
			return
		}

		nextPageToken := ""
		for {
			reqValue.FieldByName("PageToken").SetString(nextPageToken)

			resp, err := apiCallReflection(ctx, apiCallFunction, request, opts...)
			if err != nil {
				yield(nil, fmt.Errorf("API call error: %w", err))
				return
			}

			responseValuePointer := reflect.ValueOf(resp)
			if !responseValuePointer.MethodByName("GetNextPageToken").IsValid() {
				yield(nil, fmt.Errorf("response type does not have required 'GetNextPageToken' method"))
				return
			}
			responseValue := reflect.Indirect(responseValuePointer)

			items, err := getItems[T](responseValue)
			if err != nil {
				yield(nil, err)
				return
			}

			if !yield(items, nil) {
				return
			}

			nextPageToken = responseValuePointer.MethodByName("GetNextPageToken").Call(nil)[0].String()
			if nextPageToken == "" {
				return
			}
		}
	}
}

// getItems tries to find a field within the provided reflection value that
// matches a slice of type []T.
func getItems[T any](responseValue reflect.Value) ([]T, error) {
	for _, field := range responseValue.Fields() {
		if field.Kind() == reflect.Slice {
			if field.Type().Elem() == reflect.TypeFor[T]() {
				items := field.Interface().([]T)
				return items, nil
			}
		}
	}
	return nil, fmt.Errorf("response type does not have a field with a slice of type []T")
}
