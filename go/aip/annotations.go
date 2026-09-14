package aip

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/malonaz/core/go/pbutil"
)

type Annotatable interface {
	proto.Message
	GetAnnotations() map[string]string
	SetAnnotations(map[string]string)
}

func SetAnnotation(resource Annotatable, key, value string) {
	annotations := resource.GetAnnotations()
	if annotations == nil {
		annotations = map[string]string{}
		resource.SetAnnotations(annotations)
	}
	annotations[key] = value
}

func GetAnnotation(resource Annotatable, key string) (string, bool) {
	value, ok := resource.GetAnnotations()[key]
	return value, ok
}

func HasAnnotation(resource Annotatable, key string) bool {
	_, ok := GetAnnotation(resource, key)
	return ok
}

func DeleteAnnotation(resource Annotatable, key string) {
	delete(resource.GetAnnotations(), key)
}

// StringAnnotation is a declared annotation whose value is an opaque string.
type StringAnnotation struct {
	Key string
}

func (a StringAnnotation) Get(resource Annotatable) (string, bool) {
	return GetAnnotation(resource, a.Key)
}

func (a StringAnnotation) Has(resource Annotatable) bool { return HasAnnotation(resource, a.Key) }

func (a StringAnnotation) Set(resource Annotatable, value string) {
	SetAnnotation(resource, a.Key, value)
}

func (a StringAnnotation) Delete(resource Annotatable) { DeleteAnnotation(resource, a.Key) }

// TypedAnnotation is a declared annotation whose value is a T encoded as protojson,
// so it stays readable wherever the raw map surfaces (database, logs, UIs).
type TypedAnnotation[T proto.Message] struct {
	Key string
}

// Get returns the decoded value; ok is false when the annotation is absent.
func (a TypedAnnotation[T]) Get(resource Annotatable) (value T, ok bool, err error) {
	raw, ok := GetAnnotation(resource, a.Key)
	if !ok {
		return value, false, nil
	}
	value = value.ProtoReflect().New().Interface().(T)
	if err := pbutil.JSONUnmarshal([]byte(raw), value); err != nil {
		return value, true, fmt.Errorf("annotation %q: %w", a.Key, err)
	}
	return value, true, nil
}

func (a TypedAnnotation[T]) Has(resource Annotatable) bool { return HasAnnotation(resource, a.Key) }

func (a TypedAnnotation[T]) Set(resource Annotatable, value T) error {
	raw, err := pbutil.JSONMarshal(value)
	if err != nil {
		return fmt.Errorf("annotation %q: %w", a.Key, err)
	}
	SetAnnotation(resource, a.Key, string(raw))
	return nil
}

func (a TypedAnnotation[T]) Delete(resource Annotatable) { DeleteAnnotation(resource, a.Key) }
