package pbutil

import (
	"strings"

	"github.com/iancoleman/strcase"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func TrimEnumPrefix(enum protoreflect.Enum) string {
	descriptor := enum.Descriptor()
	value := descriptor.Values().ByNumber(enum.Number())
	name := string(value.Name())
	prefix := strcase.ToScreamingSnake(string(descriptor.Name())) + "_"
	return strings.TrimPrefix(name, prefix)
}

func FormatEnumKebab(enum protoreflect.Enum) string {
	return strcase.ToKebab(TrimEnumPrefix(enum))
}

func FormatEnumSnake(enum protoreflect.Enum) string {
	return strcase.ToSnake(TrimEnumPrefix(enum))
}

func FormatEnumTitle(enum protoreflect.Enum) string {
	return cases.Title(language.English).String(strcase.ToDelimited(TrimEnumPrefix(enum), ' '))
}
