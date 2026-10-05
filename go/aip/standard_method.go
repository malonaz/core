package aip

import (
	"google.golang.org/protobuf/internal/strs"
)

// StandardMethodType is the kind of a standard method, e.g. Create or Export.
type StandardMethodType string

const (
	StandardMethodTypeUnspecified StandardMethodType = ""
	StandardMethodTypeCreate      StandardMethodType = "Create"
	StandardMethodTypeBatchCreate StandardMethodType = "BatchCreate"
	StandardMethodTypeGet         StandardMethodType = "Get"
	StandardMethodTypeBatchGet    StandardMethodType = "BatchGet"
	StandardMethodTypeUpdate      StandardMethodType = "Update"
	StandardMethodTypeDelete      StandardMethodType = "Delete"
	StandardMethodTypeUndelete    StandardMethodType = "Undelete"
	StandardMethodTypeList        StandardMethodType = "List"
	StandardMethodTypeSearch      StandardMethodType = "Search"
	StandardMethodTypeImport      StandardMethodType = "Import"
	StandardMethodTypeExport      StandardMethodType = "Export"
)

// standardMethodTypeToPlural maps every standard method type to whether its name uses the plural.
var standardMethodTypeToPlural = map[StandardMethodType]bool{
	StandardMethodTypeCreate:      false,
	StandardMethodTypeBatchCreate: true,
	StandardMethodTypeGet:         false,
	StandardMethodTypeBatchGet:    true,
	StandardMethodTypeUpdate:      false,
	StandardMethodTypeDelete:      false,
	StandardMethodTypeUndelete:    false,
	StandardMethodTypeList:        true,
	StandardMethodTypeSearch:      true,
	StandardMethodTypeImport:      true,
	StandardMethodTypeExport:      true,
}

// ParseStandardMethodType returns the type of a standard method from its proto name and its
// resource's singular and plural, or StandardMethodTypeUnspecified if it matches none.
// Names are compared in Go casing, as protoc-gen-go generates them.
func ParseStandardMethodType(methodName, singular, plural string) StandardMethodType {
	goMethodName := strs.GoCamelCase(methodName)
	goSingular := strs.GoCamelCase(singular)
	goPlural := strs.GoCamelCase(plural)
	for standardMethodType, isPlural := range standardMethodTypeToPlural {
		resourceName := goSingular
		if isPlural {
			resourceName = goPlural
		}
		if goMethodName == string(standardMethodType)+resourceName {
			return standardMethodType
		}
	}
	return StandardMethodTypeUnspecified
}
