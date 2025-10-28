package aip

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/huandu/xstrings"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// apiCallReflection calls the apiCall function using reflection.
func apiCallReflection(ctx context.Context, apiCallFunc any, req any, opts ...grpc.CallOption) (response any, err error) {
	funcValue := reflect.ValueOf(apiCallFunc)

	// Check if it's a function and it has the expected number of inputs (2 plus variadic grpc.CallOption)
	if funcValue.Kind() != reflect.Func || funcValue.Type().NumIn() < 2 {
		return nil, fmt.Errorf("apiCall must be a function with at least 2 parameters")
	}

	// Check if the second parameter is context.Context
	if funcValue.Type().In(0) != reflect.TypeOf((*context.Context)(nil)).Elem() {
		return nil, fmt.Errorf("first argument of apiCall must be context.Context")
	}

	// Prepare the arguments for the call
	in := []reflect.Value{
		reflect.ValueOf(ctx),
		reflect.ValueOf(req),
	}

	for _, opt := range opts {
		in = append(in, reflect.ValueOf(opt))
	}

	// Perform the call using reflection
	responseValues := funcValue.Call(in)

	// Expecting at least two return values: the response and an error
	if len(responseValues) != 2 {
		return nil, fmt.Errorf("expected apiCall to return two values, got %d", len(responseValues))
	}

	// Extract the response and error from the responseValues
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

	// This function uses reflection to manipulate the 'PageToken' and 'PageSize' fields

	// Verify that request has the necessary fields
	reqValue := reflect.ValueOf(request)
	if reqValue.Kind() != reflect.Ptr || reqValue.Elem().Kind() != reflect.Struct {
		return nil, fmt.Errorf("request must be a pointer to a struct")
	}

	reqValue = reqValue.Elem()

	hasPageTokenField := reqValue.FieldByName("PageToken").IsValid()
	if !hasPageTokenField {
		return nil, fmt.Errorf("request does not contain required 'PageToken' field")
	}

	var allItems []T
	nextPageToken := ""

	for {
		reqValue.FieldByName("PageToken").SetString(nextPageToken)
		resp, err := apiCallReflection(ctx, apiCallFunction, request, opts...)
		if err != nil {
			return nil, fmt.Errorf("API call error: %w", err)
		}

		// Check for the expected methods on the response type
		responseValuePointer := reflect.ValueOf(resp)
		if !responseValuePointer.MethodByName("GetNextPageToken").IsValid() {
			return nil, fmt.Errorf("response type does not have required 'GetNextPageToken' method")
		}
		responseValue := reflect.Indirect(responseValuePointer)

		// Loops through the fields of the response to find a fjield with a type of []*T
		items, err := getItems[T](responseValue)
		if err != nil {
			return nil, err
		}
		allItems = append(allItems, items...)

		nextPageToken = responseValuePointer.MethodByName("GetNextPageToken").Call(nil)[0].String()
		if nextPageToken == "" {
			break
		}
	}

	return allItems, nil
}

// getItems tries to find a field within the provided reflection value that
// matches a slice of type []T. It returns the slice and a boolean flag indicating success.
func getItems[T any](responseValue reflect.Value) ([]T, error) {
	for i := 0; i < responseValue.NumField(); i++ {
		field := responseValue.Field(i)
		if field.Kind() == reflect.Slice {
			// Check if the elements of the slice are of the generic type T
			if field.Type().Elem() == reflect.TypeOf((*T)(nil)).Elem() {
				items := field.Interface().([]T)
				return items, nil
			}
		}
	}
	return nil, fmt.Errorf("response type does not have a field with a slice of type []T")
}

// PaginateFunc iterates over pages of type T, invoking a user provided function `processPage`.
// The `processPage` function is expected to take a slice of type []T and return (bool, error).
// If there is an error, pagination will stop and the error will be returned.
// If the bool is false, pagination will stop and return with no error.
func PaginateFunc[T any](ctx context.Context, request, apiCallFunction any, processPage func([]T) (bool, error), opts ...grpc.CallOption) error {
	reqValue := reflect.ValueOf(request)
	if reqValue.Kind() != reflect.Ptr || reqValue.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("request must be a pointer to a struct")
	}
	reqValue = reqValue.Elem()

	hasPageTokenField := reqValue.FieldByName("PageToken").IsValid()
	if !hasPageTokenField {
		return fmt.Errorf("request does not contain required 'PageToken' field")
	}

	nextPageToken := ""
	for {
		reqValue.FieldByName("PageToken").SetString(nextPageToken)

		resp, err := apiCallReflection(ctx, apiCallFunction, request, opts...)
		if err != nil {
			return fmt.Errorf("API call error: %w", err)
		}

		responseValuePointer := reflect.ValueOf(resp)
		if !responseValuePointer.MethodByName("GetNextPageToken").IsValid() {
			return fmt.Errorf("response type does not have required 'GetNextPageToken' method")
		}
		responseValue := reflect.Indirect(responseValuePointer)

		items, err := getItems[T](responseValue)
		if err != nil {
			return err
		}

		if len(items) > 0 {
			// Call the processPage function
			continuePaginating, procErr := processPage(items)
			if procErr != nil {
				return fmt.Errorf("error processing page: %w", procErr)
			}
			if !continuePaginating {
				return nil
			}
		}

		nextPageToken = responseValuePointer.MethodByName("GetNextPageToken").Call(nil)[0].String()
		if nextPageToken == "" {
			break
		}
	}
	return nil
}

// PaginateParallel abstracts the pagination logic for a given API and assumes that the request object
// has 'PageToken' and 'PageSize' fields, and the response object has a 'GetNextPageToken' method and an
// items field with a slice of items.
func PaginateParallel[T any](
	ctx context.Context, request, apiCallFunction any,
	field string, n int64, opts ...grpc.CallOption,
) ([]T, error) {
	var startTimestamp, endTimestamp int64
	// Find starting timestamp.
	{
		// Prepare request.
		request := proto.Clone(request.(proto.Message))
		reqValue := reflect.ValueOf(request).Elem()
		reqValue.FieldByName("PageSize").SetInt(1)
		newOrderBy := fmt.Sprintf("%s asc", field)
		reqValue.FieldByName("OrderBy").SetString(newOrderBy)

		resp, err := apiCallReflection(ctx, apiCallFunction, request, opts...)
		if err != nil {
			return nil, fmt.Errorf("API call error: %w", err)
		}
		responseValuePointer := reflect.ValueOf(resp)
		responseValue := reflect.Indirect(responseValuePointer)
		items, err := getItems[T](responseValue)
		if err != nil {
			return nil, err
		}
		if len(items) != 1 {
			return nil, fmt.Errorf("[asc query]: expected 1 item, got 0")
		}
		// Extract the first item and check for timestamp
		firstItem := reflect.ValueOf(items[0]).Elem()
		goFieldName := xstrings.ToCamelCase(field)
		startTimestampField := firstItem.FieldByName(goFieldName)
		if !startTimestampField.IsValid() {
			return nil, fmt.Errorf("field %s not found in item type", goFieldName)
		}
		startTimestamp = startTimestampField.Int()
	}

	// Find ending timestamp.
	{
		request := proto.Clone(request.(proto.Message))
		reqValue := reflect.ValueOf(request).Elem()
		reqValue.FieldByName("PageSize").SetInt(1)
		newOrderBy := fmt.Sprintf("%s desc", field)
		reqValue.FieldByName("OrderBy").SetString(newOrderBy)

		resp, err := apiCallReflection(ctx, apiCallFunction, request, opts...)
		if err != nil {
			return nil, fmt.Errorf("API call error: %w", err)
		}
		responseValuePointer := reflect.ValueOf(resp)
		responseValue := reflect.Indirect(responseValuePointer)
		items, err := getItems[T](responseValue)
		if err != nil {
			return nil, err
		}
		if len(items) != 1 {
			return nil, fmt.Errorf("[desc query]: expected 1 item, got 0")
		}
		// Extract the first item and check for timestamp
		firstItem := reflect.ValueOf(items[0]).Elem()
		goFieldName := xstrings.ToCamelCase(field)
		endTimestampField := firstItem.FieldByName(goFieldName)
		if !endTimestampField.IsValid() {
			return nil, fmt.Errorf("field %s not found in item type", goFieldName)
		}
		endTimestamp = endTimestampField.Int()
	}

	mutex := sync.Mutex{}
	allItems := []T{}
	blockSize := (endTimestamp - startTimestamp) / n
	eg, ctx := errgroup.WithContext(ctx)
	for i := int64(0); i < n; i++ {
		request := proto.Clone(request.(proto.Message))
		reqValue := reflect.ValueOf(request).Elem()
		initialFilter := reqValue.FieldByName("Filter").String()
		if initialFilter != "" {
			initialFilter = fmt.Sprintf("(%s) AND ", initialFilter)
		}
		start := startTimestamp + i*blockSize
		end := start + blockSize
		if i == n-1 {
			end = endTimestamp + 1 // Because we go [start, end) + 'block size' might not divide perfectly into the range.
		}
		newFilter := initialFilter + fmt.Sprintf("%s >= %d AND %s < %d", field, start, field, end)
		reqValue.FieldByName("Filter").SetString(newFilter)
		eg.Go(func() error {
			items, err := Paginate[T](ctx, request, apiCallFunction, opts...)
			if err != nil {
				return err
			}
			mutex.Lock()
			allItems = append(allItems, items...)
			mutex.Unlock()
			return nil
		})
	}
	return allItems, eg.Wait()
}
