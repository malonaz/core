package aip

import (
	"iter"
	"strings"

	"google.golang.org/protobuf/proto"
)

const (
	LabelValueTrue  = "true"
	LabelValueFalse = "false"
)

type Labellable interface {
	proto.Message
	GetLabels() map[string]string
	SetLabels(map[string]string)
}

func SetLabel(resource Labellable, key, value string) {
	labels := resource.GetLabels()
	if labels == nil {
		labels = map[string]string{}
		resource.SetLabels(labels)
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
