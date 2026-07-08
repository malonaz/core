package aip

import (
	"google.golang.org/protobuf/proto"
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
