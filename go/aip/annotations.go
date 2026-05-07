package aip

import (
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type Annotatable interface {
	proto.Message
	GetAnnotations() map[string]string
}

func SetAnnotation(resource Annotatable, key, value string) {
	annotations := resource.GetAnnotations()
	if annotations == nil {
		annotations = initializeAnnotations(resource)
	}
	annotations[key] = value
}

func GetAnnotation(resource Annotatable, key string) (string, bool) {
	annotations := resource.GetAnnotations()
	if annotations == nil {
		return "", false
	}
	value, ok := annotations[key]
	return value, ok
}

func HasAnnotation(resource Annotatable, key string) bool {
	_, ok := GetAnnotation(resource, key)
	return ok
}

func DeleteAnnotation(resource Annotatable, key string) {
	annotations := resource.GetAnnotations()
	if annotations == nil {
		return
	}
	delete(annotations, key)
}

func initializeAnnotations(resource Annotatable) map[string]string {
	reflectMessage := resource.ProtoReflect()
	descriptor := reflectMessage.Descriptor()
	annotationsField := descriptor.Fields().ByName("annotations")
	if annotationsField == nil {
		panic("annotation: resource does not have an 'annotations' field")
	}
	if annotationsField.Kind() != protoreflect.MessageKind || !annotationsField.IsMap() {
		panic("annotation: 'annotations' field is not a map type")
	}
	reflectMessage.Mutable(annotationsField)
	annotations := resource.GetAnnotations()
	if annotations == nil {
		panic("annotation: failed to initialize annotations map")
	}
	return annotations
}
