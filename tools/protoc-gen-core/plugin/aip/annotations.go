package aip

import (
	"errors"
	"fmt"
	"strings"

	"github.com/huandu/xstrings"
	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/reflect/protoreflect"

	aippb "github.com/malonaz/core/genproto/aip/v1"
	codegenaippb "github.com/malonaz/core/genproto/codegen/aip/v1"
	"github.com/malonaz/core/go/pbutil"
	"github.com/malonaz/core/tools/protoc-gen-core/plugin"
)

// generateAnnotations emits the `Annotations` set: one accessor per declared
// annotation, typed by its value_type so callers never decode by hand.
func generateAnnotations(file *protogen.File, g *protogen.GeneratedFile, opts *plugin.Opts) (bool, error) {
	annotations, err := pbutil.GetExtension[[]*aippb.Annotation](file.Desc.Options(), codegenaippb.E_Annotation)
	if err != nil && !errors.Is(err, pbutil.ErrExtensionNotFound) {
		return false, fmt.Errorf("getting annotation extension: %w", err)
	}
	if len(annotations) == 0 {
		return false, nil
	}
	g.P()
	g.P("type AnnotationSet struct {")
	types := make([]string, len(annotations))
	for i, annotation := range annotations {
		typ, err := annotationGoType(g, file, opts, annotation)
		if err != nil {
			return false, err
		}
		types[i] = typ
		g.P("// ", annotation.GetDescription())
		if dynamicKey := annotation.GetDynamicKey(); dynamicKey != "" {
			vars := varPattern.FindAllString(dynamicKey, -1)
			g.P(annotationGoName(dynamicKey), " func(", funcParams(vars), ") ", typ)
			continue
		}
		g.P(annotationGoName(annotation.GetKey()), " ", typ)
	}
	g.P("}")
	g.P()
	g.P("var Annotations = AnnotationSet{")
	for i, annotation := range annotations {
		if dynamicKey := annotation.GetDynamicKey(); dynamicKey != "" {
			// A dynamic annotation resolves its key per call, like a dynamic label.
			vars := varPattern.FindAllString(dynamicKey, -1)
			g.P(annotationGoName(dynamicKey), ": func(", funcParams(vars), ") ", types[i], " {")
			g.P("return ", types[i], "{Key: ", dynamicKeyExpr(dynamicKey, vars), "}")
			g.P("},")
			continue
		}
		g.P(annotationGoName(annotation.GetKey()), ": ", types[i], "{Key: ", fmt.Sprintf("%q", annotation.GetKey()), "},")
	}
	g.P("}")
	return true, nil
}

func annotationGoType(g *protogen.GeneratedFile, file *protogen.File, opts *plugin.Opts, annotation *aippb.Annotation) (string, error) {
	key := annotation.GetKey() + annotation.GetDynamicKey()
	if key == "" || (annotation.GetKey() != "" && annotation.GetDynamicKey() != "") {
		return "", fmt.Errorf("annotation: exactly one of key or dynamic_key must be set, got key=%q dynamic_key=%q", annotation.GetKey(), annotation.GetDynamicKey())
	}
	if dynamicKey := annotation.GetDynamicKey(); dynamicKey != "" && !varPattern.MatchString(dynamicKey) {
		return "", fmt.Errorf("annotation %q: dynamic_key must contain at least one {variable} placeholder", dynamicKey)
	}
	if annotation.GetValueType() == "" {
		return g.QualifiedGoIdent(aipPkg.Ident("StringAnnotation")), nil
	}
	fullName := protoreflect.FullName(annotation.GetValueType())
	if !fullName.IsValid() {
		return "", fmt.Errorf("annotation %q: invalid value_type %q", key, annotation.GetValueType())
	}
	// AIP-151 style: a bare name is relative to the declaring package.
	if fullName.Parent() == "" {
		fullName = file.Desc.Package().Append(fullName.Name())
	}
	for _, f := range opts.Files {
		for _, message := range f.Messages {
			if message.Desc.FullName() == fullName {
				return g.QualifiedGoIdent(aipPkg.Ident("TypedAnnotation")) + "[*" + g.QualifiedGoIdent(message.GoIdent) + "]", nil
			}
		}
	}
	return "", fmt.Errorf("annotation %q: value_type %s is not a top-level message of the compilation unit; import its proto", key, fullName)
}

// annotationGoName names an annotation after its key's last segment, placeholders unwrapped.
func annotationGoName(key string) string {
	if idx := strings.LastIndex(key, "/"); idx >= 0 {
		key = key[idx+1:]
	}
	return xstrings.ToPascalCase(strings.NewReplacer("{", "", "}", "", "-", "_", ".", "_").Replace(key))
}
