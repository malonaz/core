package pbutil

import (
  "strings"

  "github.com/iancoleman/strcase"
  "google.golang.org/protobuf/reflect/protoreflect"
)

func TrimEnumPrefix(enum protoreflect.Enum) string {
  descriptor := enum.Descriptor()
  value := descriptor.Values().ByNumber(enum.Number())
  name := string(value.Name())
  prefix := strcase.ToScreamingSnake(string(descriptor.Name())) + "_"
  return strings.TrimPrefix(name, prefix)
}
