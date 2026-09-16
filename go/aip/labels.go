package aip

import (
	"iter"
	"strings"

	"google.golang.org/protobuf/proto"
)

const (
	LabelValueTrue  = "true"
	LabelValueFalse = "false"

	// LabelKeyImportSource marks a resource created by an Import{Plural} method
	// (AIP-153) with the source it came from: the request's source variant minus
	// its `_source` suffix, in kebab-case (`inline`, `google-connection`). Set by
	// the import unless the resource already carries one.
	LabelKeyImportSource = "aip.malonaz.com/import-source"
	// LabelKeyImportTime marks a resource created by an Import{Plural} method
	// with the UTC date the import started, `YYYY-MM-DD` (label values allow no
	// colons). Set by the import unless the resource already carries one.
	LabelKeyImportTime = "aip.malonaz.com/import-time"
	// LabelDateFormat is the layout of a date label value.
	LabelDateFormat = "2006-01-02"
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
