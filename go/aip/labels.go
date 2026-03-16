package aip

import (
	"iter"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const (
	LabelValueTrue  = "true"
	LabelValueFalse = "false"
)

type Labellable interface {
	proto.Message
	GetLabels() map[string]string
}

func SetLabel(resource Labellable, key, value string) {
	labels := resource.GetLabels()
	if labels == nil {
		labels = initializeLabels(resource)
	}
	labels[key] = value
}

func GetLabel(resource Labellable, key string) (string, bool) {
	labels := resource.GetLabels()
	if labels == nil {
		return "", false
	}
	value, ok := labels[key]
	return value, ok
}

func HasLabel(resource Labellable, key string) bool {
	_, ok := GetLabel(resource, key)
	return ok
}

func DeleteLabel(resource Labellable, key string) {
	labels := resource.GetLabels()
	if labels == nil {
		return
	}
	delete(labels, key)
}

func initializeLabels(resource Labellable) map[string]string {
	msg, ok := resource.(proto.Message)
	if !ok {
		panic("label: resource does not implement proto.Message")
	}

	reflectMsg := msg.ProtoReflect()
	descriptor := reflectMsg.Descriptor()
	fields := descriptor.Fields()

	labelsField := fields.ByName("labels")
	if labelsField == nil {
		panic("label: resource does not have a 'labels' field")
	}

	if labelsField.Kind() != protoreflect.MessageKind || !labelsField.IsMap() {
		panic("label: 'labels' field is not a map type")
	}

	reflectMsg.Mutable(labelsField)

	labels := resource.GetLabels()
	if labels == nil {
		panic("label: failed to initialize labels map")
	}

	return labels
}

func LabelBool(b bool) string {
	if b {
		return LabelValueTrue
	}
	return LabelValueFalse
}

func NamespacedLabels(resource Labellable) iter.Seq2[string, string] {
	return func(yield func(string, string) bool) {
		for key, value := range resource.GetLabels() {
			if strings.Contains(key, "/") {
				if !yield(key, value) {
					return
				}
			}
		}
	}
}
