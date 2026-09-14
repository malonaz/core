package aip

import (
	"strconv"
	"strings"

	"github.com/huandu/xstrings"
	"google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	"github.com/malonaz/core/go/aip/resourcename"
)

const (
	fmtPkg      = protogen.GoImportPath("fmt")
	stringsPkg  = protogen.GoImportPath("strings")
	encodingPkg = protogen.GoImportPath("encoding")

	// rnSuffix names the generated types: `UserRn`, with `n.OrganizationRn()` for parents.
	rnSuffix = "Rn"
)

// generateResourceNames emits one `{Type}Rn` struct per pattern of every resource the
// file declares (file-level definitions included), with constructors from and accessors
// to the parents declared in the same package.
func generateResourceNames(file *protogen.File, g *protogen.GeneratedFile, files *protoregistry.Files) (bool, error) {
	var generated bool
	for _, resource := range resourcesInFile(file.Desc) {
		if len(resource.GetPattern()) == 0 {
			continue
		}
		generated = true
		gen := resourceNameGenerator{resource: resource, pkg: file.Desc.Package(), files: files}
		gen.generate(g)
	}
	return generated, nil
}

func resourcesInFile(file protoreflect.FileDescriptor) []*annotations.ResourceDescriptor {
	resources := proto.GetExtension(file.Options(), annotations.E_ResourceDefinition).([]*annotations.ResourceDescriptor)
	for i := 0; i < file.Messages().Len(); i++ {
		if resource := proto.GetExtension(file.Messages().Get(i).Options(), annotations.E_Resource).(*annotations.ResourceDescriptor); resource != nil {
			resources = append(resources, resource)
		}
	}
	return resources
}

type resourceNameGenerator struct {
	resource *annotations.ResourceDescriptor
	pkg      protoreflect.FullName
	files    *protoregistry.Files
}

func (r resourceNameGenerator) generate(g *protogen.GeneratedFile) {
	patterns := r.resource.GetPattern()
	multi := len(patterns) > 1 || r.resource.GetHistory() == annotations.ResourceDescriptor_FUTURE_MULTI_PATTERN
	if multi {
		r.generateMultiPatternInterface(g)
		r.generateMultiPatternParse(g)
	}
	// A FUTURE_MULTI_PATTERN resource only gets per-pattern structs; otherwise the first
	// pattern is the resource's canonical `{Type}Rn`.
	single := r.resource.GetHistory() != annotations.ResourceDescriptor_FUTURE_MULTI_PATTERN
	if single {
		r.generatePatternStruct(g, patterns[0], r.singleStructName())
	}
	if multi && !(single && r.multiStructName(patterns[0]) == r.singleStructName()) {
		r.generatePatternStruct(g, patterns[0], r.multiStructName(patterns[0]))
	}
	for _, pattern := range patterns[1:] {
		r.generatePatternStruct(g, pattern, r.multiStructName(pattern))
	}
}

func (r resourceNameGenerator) generatePatternStruct(g *protogen.GeneratedFile, pattern, typeName string) {
	g.P()
	g.P("type ", typeName, " struct {")
	for _, v := range variables(pattern) {
		g.P(fieldName(v), " string")
	}
	g.P("}")
	r.rangeParents(pattern, func(parent *annotations.ResourceDescriptor, parentPattern string) {
		r.generateChildConstructor(g, pattern, typeName, parent, parentPattern)
	})
	r.generateValidate(g, pattern, typeName)
	r.generateContainsWildcard(g, pattern, typeName)
	r.generateString(g, pattern, typeName)
	r.generateMarshalString(g, typeName)
	r.generateMarshalText(g, typeName)
	r.generateUnmarshalString(g, pattern, typeName)
	r.generateUnmarshalText(g, typeName)
	r.generateType(g, typeName)
	r.generatePattern(g, pattern, typeName)
	r.rangeParents(pattern, func(parent *annotations.ResourceDescriptor, parentPattern string) {
		r.generateParentAccessor(g, typeName, parent, parentPattern)
	})
}

// rangeParents calls fn for every resource in the package whose pattern is an ancestor
// of pattern, root first, along with the matching ancestor pattern.
func (r resourceNameGenerator) rangeParents(pattern string, fn func(parent *annotations.ResourceDescriptor, parentPattern string)) {
	resourcename.RangeParents(pattern, func(ancestor string) bool {
		if ancestor == pattern {
			return true
		}
		r.files.RangeFilesByPackage(r.pkg, func(file protoreflect.FileDescriptor) bool {
			for _, resource := range resourcesInFile(file) {
				for _, candidate := range resource.GetPattern() {
					if candidate == ancestor {
						fn(resource, candidate)
						break
					}
				}
			}
			return true
		})
		return true
	})
}

func (r resourceNameGenerator) generateChildConstructor(g *protogen.GeneratedFile, pattern, typeName string, parent *annotations.ResourceDescriptor, parentPattern string) {
	parentGen := resourceNameGenerator{resource: parent, pkg: r.pkg, files: r.files}
	parentType := parentGen.structName(parentPattern)
	ownVariables := variables(strings.TrimPrefix(pattern, parentPattern))
	g.P()
	g.P("func (n ", parentType, ") ", typeName, "(")
	for _, v := range ownVariables {
		g.P(paramName(v), " string,")
	}
	g.P(") ", typeName, " {")
	g.P("return ", typeName, "{")
	for _, v := range variables(parentPattern) {
		g.P(fieldName(v), ": n.", fieldName(v), ",")
	}
	for _, v := range ownVariables {
		g.P(fieldName(v), ": ", paramName(v), ",")
	}
	g.P("}")
	g.P("}")
}

func (r resourceNameGenerator) generateParentAccessor(g *protogen.GeneratedFile, typeName string, parent *annotations.ResourceDescriptor, parentPattern string) {
	parentGen := resourceNameGenerator{resource: parent, pkg: r.pkg, files: r.files}
	parentType := parentGen.structName(parentPattern)
	g.P()
	g.P("func (n ", typeName, ") ", parentType, "() ", parentType, " {")
	g.P("return ", parentType, "{")
	for _, v := range variables(parentPattern) {
		g.P(fieldName(v), ": n.", fieldName(v), ",")
	}
	g.P("}")
	g.P("}")
}

func (r resourceNameGenerator) generateValidate(g *protogen.GeneratedFile, pattern, typeName string) {
	g.P()
	g.P("func (n ", typeName, ") Validate() error {")
	for _, v := range variables(pattern) {
		g.P("if n.", fieldName(v), ` == "" {`)
		g.P("return ", fmtPkg.Ident("Errorf"), `("`, v, `: empty")`)
		g.P("}")
		g.P("if ", stringsPkg.Ident("IndexByte"), "(n.", fieldName(v), ", '/') != -1 {")
		g.P("return ", fmtPkg.Ident("Errorf"), `("`, v, `: contains illegal character '/'")`)
		g.P("}")
	}
	g.P("return nil")
	g.P("}")
}

func (r resourceNameGenerator) generateContainsWildcard(g *protogen.GeneratedFile, pattern, typeName string) {
	g.P()
	g.P("func (n ", typeName, ") ContainsWildcard() bool {")
	var b strings.Builder
	b.WriteString("return false")
	for _, v := range variables(pattern) {
		b.WriteString(" || n." + fieldName(v) + ` == "-"`)
	}
	g.P(b.String())
	g.P("}")
}

func (r resourceNameGenerator) generateString(g *protogen.GeneratedFile, pattern, typeName string) {
	g.P()
	g.P("func (n ", typeName, ") String() string {")
	g.P("return ", resourcenamePkg.Ident("Sprint"), "(")
	g.P(strconv.Quote(pattern), ",")
	for _, v := range variables(pattern) {
		g.P("n.", fieldName(v), ",")
	}
	g.P(")")
	g.P("}")
}

func (r resourceNameGenerator) generateMarshalString(g *protogen.GeneratedFile, typeName string) {
	g.P()
	g.P("func (n ", typeName, ") MarshalString() (string, error) {")
	g.P("if err := n.Validate(); err != nil {")
	g.P(`return "", err`)
	g.P("}")
	g.P("return n.String(), nil")
	g.P("}")
}

func (r resourceNameGenerator) generateMarshalText(g *protogen.GeneratedFile, typeName string) {
	g.P()
	g.P("// MarshalText implements the encoding.TextMarshaler interface.")
	g.P("func (n ", typeName, ") MarshalText() ([]byte, error) {")
	g.P("if err := n.Validate(); err != nil {")
	g.P("return nil, err")
	g.P("}")
	g.P("return []byte(n.String()), nil")
	g.P("}")
}

func (r resourceNameGenerator) generateUnmarshalString(g *protogen.GeneratedFile, pattern, typeName string) {
	g.P()
	g.P("func (n *", typeName, ") UnmarshalString(name string) error {")
	g.P("err := ", resourcenamePkg.Ident("Sscan"), "(")
	g.P("name,")
	g.P(strconv.Quote(pattern), ",")
	for _, v := range variables(pattern) {
		g.P("&n.", fieldName(v), ",")
	}
	g.P(")")
	g.P("if err != nil {")
	g.P("return err")
	g.P("}")
	g.P("return n.Validate()")
	g.P("}")
}

func (r resourceNameGenerator) generateUnmarshalText(g *protogen.GeneratedFile, typeName string) {
	g.P()
	g.P("// UnmarshalText implements the encoding.TextUnmarshaler interface.")
	g.P("func (n *", typeName, ") UnmarshalText(text []byte) error {")
	g.P("return n.UnmarshalString(string(text))")
	g.P("}")
}

func (r resourceNameGenerator) generateType(g *protogen.GeneratedFile, typeName string) {
	g.P()
	g.P("func (n ", typeName, ") Type() string {")
	g.P("return ", strconv.Quote(r.resource.GetType()))
	g.P("}")
}

func (r resourceNameGenerator) generatePattern(g *protogen.GeneratedFile, pattern, typeName string) {
	g.P()
	g.P("// Pattern returns the resource name pattern for ", typeName, " as a string.")
	g.P("func (n ", typeName, ") Pattern() string {")
	g.P("return ", strconv.Quote(pattern))
	g.P("}")
}

func (r resourceNameGenerator) generateMultiPatternInterface(g *protogen.GeneratedFile) {
	g.P()
	g.P("type ", r.multiInterfaceName(), " interface {")
	g.P(fmtPkg.Ident("Stringer"))
	g.P(encodingPkg.Ident("TextMarshaler"))
	g.P("MarshalString() (string, error)")
	g.P("ContainsWildcard() bool")
	g.P("}")
}

func (r resourceNameGenerator) generateMultiPatternParse(g *protogen.GeneratedFile) {
	g.P()
	g.P("func Parse", r.multiInterfaceName(), "(name string) (", r.multiInterfaceName(), ", error) {")
	g.P("switch {")
	for _, pattern := range r.resource.GetPattern() {
		g.P("case ", resourcenamePkg.Ident("Match"), "(", strconv.Quote(pattern), ", name):")
		g.P("var result ", r.multiStructName(pattern))
		g.P("return &result, result.UnmarshalString(name)")
	}
	g.P("default:")
	g.P("return nil, ", fmtPkg.Ident("Errorf"), `("no matching pattern")`)
	g.P("}")
	g.P("}")
}

// typeName is the resource type without its service, e.g. "Author" of "library.com/Author".
func (r resourceNameGenerator) typeName() string {
	t := r.resource.GetType()
	return t[strings.LastIndexByte(t, '/')+1:]
}

func (r resourceNameGenerator) singleStructName() string { return r.typeName() + rnSuffix }

func (r resourceNameGenerator) multiInterfaceName() string {
	return r.typeName() + "MultiPattern" + rnSuffix
}

// structName is the type generated for pattern: the canonical `{Type}Rn` for a
// single-pattern resource's pattern, the literal-prefixed variant otherwise.
func (r resourceNameGenerator) structName(pattern string) string {
	if r.resource.GetHistory() == annotations.ResourceDescriptor_FUTURE_MULTI_PATTERN || len(r.resource.GetPattern()) > 1 {
		return r.multiStructName(pattern)
	}
	if r.resource.GetPattern()[0] == pattern {
		return r.singleStructName()
	}
	return r.multiStructName(pattern)
}

// multiStructName prefixes the type with the pattern's literal segments other than the
// resource's own collection, e.g. "organizations/{organization}/authors/{author}/notes/{note}"
// yields OrganizationsAuthorsNoteRn.
func (r resourceNameGenerator) multiStructName(pattern string) string {
	var b strings.Builder
	var sc resourcename.Scanner
	sc.Init(pattern)
	for sc.Scan() {
		if segment := sc.Segment(); !segment.IsVariable() && string(segment.Literal()) != r.resource.GetPlural() {
			b.WriteString(xstrings.ToPascalCase(string(segment.Literal())))
		}
	}
	b.WriteString(r.singleStructName())
	return b.String()
}

// variables returns the pattern's variable names in order, e.g. ["organization", "author"].
func variables(pattern string) []string {
	var vars []string
	var sc resourcename.Scanner
	sc.Init(pattern)
	for sc.Scan() {
		if segment := sc.Segment(); segment.IsVariable() {
			vars = append(vars, string(segment.Literal()))
		}
	}
	return vars
}

func fieldName(variable string) string { return xstrings.ToPascalCase(variable) }
func paramName(variable string) string { return xstrings.ToCamelCase(variable) }
