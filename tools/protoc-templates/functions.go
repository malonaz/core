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

	modelpb "github.com/malonaz/core/genproto/codegen/model/v1"
)

var (
	doOnceCache = map[string]bool{}
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
		"skipGeneration": func() bool {
			se.generatedFile.Skip()
			return true // dummy return
		},
		"debug": func(message string, v ...any) error {
			if *opts.Debug {
				fmt.Printf(message, v...)
			}
			return nil
		},
		"protoreflectName": func(s string) protoreflect.Name { return protoreflect.Name(s) },
		"goIdent": func(goImportPath protogen.GoImportPath, goName string) string {
			goIdent := protogen.GoIdent{
				GoName:       goName,
				GoImportPath: goImportPath,
			}
			return se.qualifiedGoIdent(goIdent)
		},

		"doOnce": func(key string) bool {
			if _, ok := doOnceCache[key]; ok {
				return false
			}
			doOnceCache[key] = true
			return true
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
		"qgi":               se.qualifiedGoIdent,

		"parseRPC":                 parseRPC,
		"parseResource":            parseResource,
		"parseResourceFromMessage": parseResourceFromMessage,
		"getMessageUsingResourceType": func(resourceType string) *protogen.Message {
			return resourceTypeToMessage[resourceType]
		},

		"getExt":      getExt,
		"columnName":  columnName,
		"fieldGoType": fieldGoType,
		"fieldType":   fieldType,
		"zeroValue":   zeroValue,
		"unquote":     unquote,

		// Opts functions
		"getModelOpts": getModelOpts,
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

func columnName(field *protogen.Field) (string, error) {
	name := field.Desc.TextName()
	// Check if we are using a standard field.
	options := field.Desc.Options()
	if options == nil {
		return name, nil
	}
	if !proto.HasExtension(options, modelpb.E_FieldOpts) {
		return name, nil
	}

	// 1. Get the message_type annotation
	fieldOptsExt := proto.GetExtension(options, modelpb.E_FieldOpts)
	fieldOpts, ok := fieldOptsExt.(*modelpb.FieldOpts)
	if !ok || fieldOpts == nil {
		return "", fmt.Errorf("field %s has invalid field_opts annotation", field.Desc.Name())
	}

	if fieldOpts.ColumnName != "" {
		return fieldOpts.ColumnName, nil
	}
	return name, nil
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

func getModelOpts(message *protogen.Message) (*modelpb.ModelOpts, error) {
	options := message.Desc.Options()
	if options == nil {
		return nil, nil
	}

	if !proto.HasExtension(options, modelpb.E_ModelOpts) {
		return nil, nil
	}

	modelOptsExt := proto.GetExtension(options, modelpb.E_ModelOpts)
	modelOpts, ok := modelOptsExt.(*modelpb.ModelOpts)
	if !ok || modelOpts == nil {
		return nil, fmt.Errorf("message %s has invalid model_opts annotation", message.Desc.FullName())
	}

	return modelOpts, nil
}
