package main

import (
	"fmt"
	"reflect"
	"strings"
	"text/template"

	"github.com/Masterminds/sprig/v3"
	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
)

type scopedExecution struct {
	funcMap                                   template.FuncMap
	generatedFile                             *protogen.GeneratedFile
	originalImportPathToReplacementImportPath map[string]string
}

func newScopedExecution(generatedFile *protogen.GeneratedFile) *scopedExecution {
	return &scopedExecution{
		funcMap:       sprig.TxtFuncMap(),
		generatedFile: generatedFile,
		originalImportPathToReplacementImportPath: map[string]string{},
	}
}

func (se *scopedExecution) FuncMap() template.FuncMap {
	additional := template.FuncMap{
		"debug": func(message string, v ...any) error {
			if *opts.Debug {
				fmt.Printf(message, v...)
			}
			return nil
		},
		"protoreflectName": func(s string) protoreflect.Name { return protoreflect.Name(s) },
		"goIdent": func(goName string, goImportPath protogen.GoImportPath) protogen.GoIdent {
			return protogen.GoIdent{
				GoName:       goName,
				GoImportPath: goImportPath,
			}
		},

		"emptyPb": func(message proto.Message) bool {
			// Create a new instance of the same type
			empty := proto.Clone(message)
			// Reset it to its zero state
			proto.Reset(empty)
			// Compare with original
			return proto.Equal(message, empty)
		},
		"replaceImportPath": se.replaceImportPath,
		"fqn":               se.fqn,
		"qualifiedGoIdent":  se.qualifiedGoIdent,

		"getExt":      getExt,
		"fieldName":   fieldName,
		"fieldGoType": fieldGoType,
		"fieldType":   fieldType,
		"zeroValue":   zeroValue,
		"unquote":     unquote,
	}
	for k, v := range additional {
		se.funcMap[k] = v
	}
	return se.funcMap
}

func (se *scopedExecution) replaceImportPath(original protogen.GoImportPath, replacement string) error {
	cleanedOriginal := unquote(string(original))
	if *opts.Debug {
		fmt.Printf("%s => %s", cleanedOriginal, replacement)
	}
	se.originalImportPathToReplacementImportPath[cleanedOriginal] = replacement
	return nil
}

func (se *scopedExecution) qualifiedGoIdent(ident protogen.GoIdent) string {
	if replacement, ok := se.originalImportPathToReplacementImportPath[unquote(string(ident.GoImportPath))]; ok {
		ident.GoImportPath = protogen.GoImportPath(replacement)
	}
	if *opts.Debug {
		fmt.Printf("qualified go ident: (%s, %s)", ident.GoName, ident.GoImportPath)
	}
	return se.generatedFile.QualifiedGoIdent(ident)
}

func (se *scopedExecution) fqn(importPath, name string) string {
	return se.generatedFile.QualifiedGoIdent(protogen.GoIdent{
		GoName:       name,
		GoImportPath: protogen.GoImportPath(importPath),
	})
}

func unquote(str string) string {
	out := strings.TrimSuffix(strings.TrimPrefix(str, `\"`), `\"`)
	return strings.TrimSuffix(strings.TrimPrefix(out, `"`), `"`)
}

func fieldName(field *protogen.Field) string {
	return field.Desc.TextName()
}

func getExt(desc protoreflect.Descriptor, fullName string) (any, error) {
	options := desc.Options()
	if !options.ProtoReflect().IsValid() {
		return nil, nil
	}
	// Parse the full name to get the extension type
	extType, err := protoregistry.GlobalTypes.FindExtensionByName(protoreflect.FullName(fullName))
	if err != nil {
		return nil, fmt.Errorf("failed to find extension: %w", err)
	}
	ext := proto.GetExtension(proto.Message(options), extType)
	if pbMsg, ok := ext.(proto.Message); ok {
		if reflect.ValueOf(pbMsg).IsNil() {
			// Get the concrete type of the message
			msgType := reflect.TypeOf(pbMsg).Elem()
			// Create a new instance of the concrete type
			newMsg := reflect.New(msgType).Interface().(proto.Message)
			return newMsg, nil
		}
	}
	return ext, nil
}

func registerAllExtensions(extTypes *protoregistry.Types, descs interface {
	Messages() protoreflect.MessageDescriptors
	Extensions() protoreflect.ExtensionDescriptors
}) error {
	mds := descs.Messages()
	for i := 0; i < mds.Len(); i++ {
		m := mds.Get(i)
		if err := extTypes.RegisterMessage(dynamicpb.NewMessageType(m)); err != nil {
			return err
		}
		registerAllExtensions(extTypes, m)
	}
	xds := descs.Extensions()
	for i := 0; i < xds.Len(); i++ {
		if err := extTypes.RegisterExtension(dynamicpb.NewExtensionType(xds.Get(i))); err != nil {
			return err
		}
	}
	return nil
}
