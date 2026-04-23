package aip

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	annotationspb "google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/pbutil"
	"google.golang.org/grpc/codes"
)

// IdempotentResource is a resource that supports both labels and etag-based optimistic concurrency.
type IdempotentResource interface {
	proto.Message
	Labellable
	ETagResource
	GetName() string
}

// LabelOpt configures the behavior of Labeller.Label.
type LabelOpt[R IdempotentResource] func(*labelOpts[R])

type labelOpts[R IdempotentResource] struct {
	precondition   func(R) bool
	retryOnAborted bool
}

// WithPrecondition evaluates a precondition against the current resource state before labelling.
// If the function returns false, Label aborts and returns that error.
func WithPrecondition[R IdempotentResource](fn func(R) bool) LabelOpt[R] {
	return func(o *labelOpts[R]) {
		o.precondition = fn
	}
}

// WithRetryOnAborted causes Label to re-fetch the resource and retry if the update fails with Aborted.
func WithRetryOnAborted[R IdempotentResource]() LabelOpt[R] {
	return func(o *labelOpts[R]) {
		o.retryOnAborted = true
	}
}

// Labeller sets labels on resources idempotently via reflected Update and Get RPCs.
type Labeller[R IdempotentResource] struct {
	updateMethod       reflect.Value
	updateReqType      reflect.Type
	resourceFieldIndex int
	maskFieldIndex     int

	getMethod       reflect.Value
	getReqType      reflect.Type
	getNameFieldIdx int
}

// NewLabeller constructs a Labeller by reflecting on the service client to find
// Update{Resource} and Get{Resource} methods derived from the resource descriptor's singular name.
func NewLabeller[R IdempotentResource](serviceClient any) (*Labeller[R], error) {
	var zeroResource R
	resourceType := reflect.TypeOf(zeroResource)
	maskType := reflect.TypeOf((*fieldmaskpb.FieldMask)(nil))

	resourceDescriptor, err := pbutil.GetMessageOption[*annotationspb.ResourceDescriptor](zeroResource, annotationspb.E_Resource)
	if err != nil {
		return nil, fmt.Errorf("getting resource descriptor: %w", err)
	}

	typeName := strings.ToUpper(resourceDescriptor.GetSingular()[:1]) + resourceDescriptor.GetSingular()[1:]
	clientValue := reflect.ValueOf(serviceClient)
	labeller := &Labeller[R]{}

	// Find Update method.
	updateMethodName := "Update" + typeName
	updateMethod := clientValue.MethodByName(updateMethodName)
	if !updateMethod.IsValid() {
		return nil, fmt.Errorf("service client %T has no %s method", serviceClient, updateMethodName)
	}
	mt := updateMethod.Type()
	if mt.NumIn() < 2 || mt.NumOut() != 2 {
		return nil, fmt.Errorf("%s has unexpected signature", updateMethodName)
	}
	updateReqPtrType := mt.In(1)
	if updateReqPtrType.Kind() != reflect.Ptr || updateReqPtrType.Elem().Kind() != reflect.Struct {
		return nil, fmt.Errorf("%s request parameter must be a pointer to a struct", updateMethodName)
	}
	updateReqType := updateReqPtrType.Elem()

	resIdx := -1
	maskIdx := -1
	for i := range updateReqType.NumField() {
		if updateReqType.Field(i).Type == resourceType {
			resIdx = i
		}
		if updateReqType.Field(i).Type == maskType {
			maskIdx = i
		}
	}
	if resIdx == -1 {
		return nil, fmt.Errorf("%s request has no field of type %T", updateMethodName, zeroResource)
	}
	if maskIdx == -1 {
		return nil, fmt.Errorf("%s request has no *fieldmaskpb.FieldMask field", updateMethodName)
	}

	labeller.updateMethod = updateMethod
	labeller.updateReqType = updateReqType
	labeller.resourceFieldIndex = resIdx
	labeller.maskFieldIndex = maskIdx

	// Find Get method.
	getMethodName := "Get" + typeName
	getMethod := clientValue.MethodByName(getMethodName)
	if !getMethod.IsValid() {
		return nil, fmt.Errorf("service client %T has no %s method", serviceClient, getMethodName)
	}
	gt := getMethod.Type()
	if gt.NumIn() < 2 || gt.NumOut() != 2 {
		return nil, fmt.Errorf("%s has unexpected signature", getMethodName)
	}
	getReqPtrType := gt.In(1)
	if getReqPtrType.Kind() != reflect.Ptr || getReqPtrType.Elem().Kind() != reflect.Struct {
		return nil, fmt.Errorf("%s request parameter must be a pointer to a struct", getMethodName)
	}
	getReqType := getReqPtrType.Elem()

	nameIdx := -1
	for i := range getReqType.NumField() {
		if getReqType.Field(i).Name == "Name" && getReqType.Field(i).Type.Kind() == reflect.String {
			nameIdx = i
			break
		}
	}
	if nameIdx == -1 {
		return nil, fmt.Errorf("%s request has no string Name field", getMethodName)
	}

	labeller.getMethod = getMethod
	labeller.getReqType = getReqType
	labeller.getNameFieldIdx = nameIdx

	return labeller, nil
}

// Label sets the given label key=value on the resource.
func (l *Labeller[R]) Label(ctx context.Context, resource R, labelKey, labelValue string, opts ...LabelOpt[R]) (R, error) {
	var zero R
	var options labelOpts[R]
	for _, opt := range opts {
		opt(&options)
	}

	for {
		if options.precondition != nil {
			if ok := options.precondition(resource); !ok {
				return zero, status.Errorf(codes.FailedPrecondition, "label precondition evaluated to false").Err()
			}
		}

		// Build a minimal update resource with only the fields needed for a label update,
		// avoiding mutation of the caller's resource.
		updateResource := resource.ProtoReflect().New().Interface().(R)
		srcReflect := resource.ProtoReflect()
		dstReflect := updateResource.ProtoReflect()
		fields := srcReflect.Descriptor().Fields()
		for _, name := range []protoreflect.Name{"name", "etag"} {
			field := fields.ByName(name)
			if field != nil && srcReflect.Has(field) {
				dstReflect.Set(field, srcReflect.Get(field))
			}
		}

		// Deep-copy labels via maps.Clone to avoid mutating the caller's map.
		for k, v := range resource.GetLabels() {
			SetLabel(updateResource, k, v)
		}
		SetLabel(updateResource, labelKey, labelValue)

		updatedResource, err := l.callUpdate(ctx, updateResource)
		if err != nil {
			if options.retryOnAborted && status.HasCode(err, codes.Aborted) {
				refreshedResource, err := l.callGet(ctx, resource.GetName())
				if err != nil {
					return zero, err
				}
				resource = refreshedResource
				continue
			}
			return zero, err
		}

		return updatedResource, nil
	}
}

// callUpdate builds and invokes the reflected Update RPC.
func (l *Labeller[R]) callUpdate(ctx context.Context, resource R) (R, error) {
	var zero R
	reqValue := reflect.New(l.updateReqType)
	reqElem := reqValue.Elem()
	reqElem.Field(l.resourceFieldIndex).Set(reflect.ValueOf(resource))
	reqElem.Field(l.maskFieldIndex).Set(reflect.ValueOf(&fieldmaskpb.FieldMask{Paths: []string{"labels"}}))

	results := l.updateMethod.Call([]reflect.Value{
		reflect.ValueOf(ctx),
		reqValue,
	})

	if errVal := results[1].Interface(); errVal != nil {
		return zero, errVal.(error)
	}
	return results[0].Interface().(R), nil
}

// callGet builds and invokes the reflected Get RPC.
func (l *Labeller[R]) callGet(ctx context.Context, name string) (R, error) {
	var zero R
	reqValue := reflect.New(l.getReqType)
	reqElem := reqValue.Elem()
	reqElem.Field(l.getNameFieldIdx).SetString(name)

	results := l.getMethod.Call([]reflect.Value{
		reflect.ValueOf(ctx),
		reqValue,
	})

	if errVal := results[1].Interface(); errVal != nil {
		return zero, errVal.(error)
	}
	return results[0].Interface().(R), nil
}
