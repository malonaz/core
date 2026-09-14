package aip

import (
	"fmt"
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
	fmtPkg     = protogen.GoImportPath("fmt")
	stringsPkg = protogen.GoImportPath("strings")

	rnSuffix = "Rn"
)

// generateResourceNames emits, for every resource the file declares (file-level
// definitions included), a `{Type}Rn` with Parse/Match/New constructors and typed
// parent/child navigation. A multi-pattern resource gets a `{Type}Rn` interface
// implemented by one `{Parent}{Type}Rn` struct per pattern.
func generateResourceNames(file *protogen.File, g *protogen.GeneratedFile, files *protoregistry.Files) (bool, error) {
	var generated bool
	for _, resource := range resourcesInFile(file.Desc) {
		if len(resource.GetPattern()) == 0 {
			continue
		}
		generated = true
		r, err := newResource(resource, file.Desc.Package(), files)
		if err != nil {
			return false, err
		}
		r.generate(g)
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

// pattern is one resource name pattern with its structure resolved.
type pattern struct {
	value     string
	typeName  string   // generated struct, e.g. UserJobRn
	variables []string // every variable in order
	// parent is the pattern minus the resource's own segments ("" at the root), and
	// parentResource the package resource declaring it, if any.
	parent         string
	parentResource *annotations.ResourceDescriptor
	singleton      bool
}

// id is the pattern's own variable; "" for singletons.
func (p *pattern) id() string {
	if p.singleton {
		return ""
	}
	return p.variables[len(p.variables)-1]
}

func (p *pattern) parentVariables() []string {
	if p.singleton {
		return p.variables
	}
	return p.variables[:len(p.variables)-1]
}

type resource struct {
	desc     *annotations.ResourceDescriptor
	pkg      protoreflect.FullName
	files    *protoregistry.Files
	patterns []*pattern
}

func newResource(desc *annotations.ResourceDescriptor, pkg protoreflect.FullName, files *protoregistry.Files) (*resource, error) {
	r := &resource{desc: desc, pkg: pkg, files: files}
	for _, value := range desc.GetPattern() {
		r.patterns = append(r.patterns, r.parsePattern(value))
	}
	if r.multi() {
		seen := map[string]string{}
		for _, p := range r.patterns {
			p.typeName = r.multiTypeName(p)
			if other, ok := seen[p.typeName]; ok {
				return nil, fmt.Errorf("resource %s: patterns %q and %q both generate %s; their parents must differ in type", desc.GetType(), other, p.value, p.typeName)
			}
			seen[p.typeName] = p.value
		}
	} else {
		r.patterns[0].typeName = r.typeName()
	}
	return r, nil
}

func (r *resource) parsePattern(value string) *pattern {
	p := &pattern{value: value}
	var segments []resourcename.Segment
	var sc resourcename.Scanner
	sc.Init(value)
	for sc.Scan() {
		segments = append(segments, sc.Segment())
		if sc.Segment().IsVariable() {
			p.variables = append(p.variables, string(sc.Segment().Literal()))
		}
	}
	// A pattern ending on a literal is a singleton: its parent is everything before that
	// literal; otherwise the parent is everything before the own "{collection}/{id}".
	p.singleton = !segments[len(segments)-1].IsVariable()
	own := 2
	if p.singleton {
		own = 1
	}
	if len(segments) > own {
		parts := make([]string, 0, len(segments)-own)
		for _, s := range segments[:len(segments)-own] {
			parts = append(parts, string(s))
		}
		p.parent = strings.Join(parts, "/")
		p.parentResource = r.resourceByPattern(p.parent)
	}
	return p
}

// resourceByPattern finds the package resource declaring pattern, if any.
func (r *resource) resourceByPattern(pattern string) *annotations.ResourceDescriptor {
	var found *annotations.ResourceDescriptor
	r.files.RangeFilesByPackage(r.pkg, func(file protoreflect.FileDescriptor) bool {
		for _, resource := range resourcesInFile(file) {
			for _, candidate := range resource.GetPattern() {
				if candidate == pattern {
					found = resource
					return false
				}
			}
		}
		return true
	})
	return found
}

func (r *resource) multi() bool {
	return len(r.patterns) > 1 || r.desc.GetHistory() == annotations.ResourceDescriptor_FUTURE_MULTI_PATTERN
}

// shortType is the resource type without its service, e.g. "Author" of "library.com/Author".
func shortType(desc *annotations.ResourceDescriptor) string {
	t := desc.GetType()
	return t[strings.LastIndexByte(t, '/')+1:]
}

func (r *resource) typeName() string { return shortType(r.desc) + rnSuffix }

// multiTypeName names a multi-pattern struct after its parent: RootJobRn, OrganizationJobRn,
// UserJobRn. A parent not declared as a resource falls back to its literal segments.
func (r *resource) multiTypeName(p *pattern) string {
	switch {
	case p.parent == "":
		return "Root" + r.typeName()
	case p.parentResource != nil:
		return shortType(p.parentResource) + r.typeName()
	}
	var b strings.Builder
	var sc resourcename.Scanner
	sc.Init(p.parent)
	for sc.Scan() {
		if s := sc.Segment(); !s.IsVariable() {
			b.WriteString(xstrings.ToPascalCase(string(s.Literal())))
		}
	}
	return b.String() + r.typeName()
}

// typeNameOf is the struct generated for a parent pattern by the resource declaring it.
func (r *resource) typeNameOf(parent *annotations.ResourceDescriptor, parentPattern string) (string, error) {
	pr, err := newResource(parent, r.pkg, r.files)
	if err != nil {
		return "", err
	}
	for _, p := range pr.patterns {
		if p.value == parentPattern {
			return p.typeName, nil
		}
	}
	return "", fmt.Errorf("resource %s declares no pattern %q", parent.GetType(), parentPattern)
}

func (r *resource) generate(g *protogen.GeneratedFile) {
	g.P()
	g.P("// ", r.typeName(), "Type is the resource type of ", r.typeName(), ".")
	g.P("const ", r.typeName(), "Type = ", strconv.Quote(r.desc.GetType()))
	if r.multi() {
		r.generateInterface(g)
	}
	r.generateNew(g)
	for _, p := range r.patterns {
		r.generatePattern(g, p)
	}
}

// generateInterface emits the multi-pattern interface, sealed by a marker method.
func (r *resource) generateInterface(g *protogen.GeneratedFile) {
	name := r.typeName()
	g.P()
	g.P("// ", name, " is any of the resource's patterns: ", r.patternList(), ".")
	g.P("type ", name, " interface {")
	g.P(aipPkg.Ident("Rn"))
	g.P("is", name, "()")
	g.P("}")
	g.P()
	g.P("// Parse", name, " parses name under whichever pattern it matches.")
	g.P("func Parse", name, "(name string) (", name, ", error) {")
	g.P("switch {")
	for _, p := range r.patterns {
		g.P("case ", resourcenamePkg.Ident("Match"), "(", strconv.Quote(p.value), ", name):")
		g.P("return Parse", p.typeName, "(name)")
	}
	g.P("}")
	g.P("return nil, ", fmtPkg.Ident("Errorf"), `("resource name %q matches no pattern of `, r.desc.GetType(), `", name)`)
	g.P("}")
	g.P()
	g.P("// Match", name, " reports whether name follows any pattern of ", r.desc.GetType(), ".")
	g.P("func Match", name, "(name string) bool {")
	var b strings.Builder
	for i, p := range r.patterns {
		if i > 0 {
			b.WriteString(" || ")
		}
		b.WriteString(g.QualifiedGoIdent(resourcenamePkg.Ident("Match")) + "(" + strconv.Quote(p.value) + ", name)")
	}
	g.P("return ", b.String())
	g.P("}")
}

func (r *resource) patternList() string {
	values := make([]string, len(r.patterns))
	for i, p := range r.patterns {
		values[i] = strconv.Quote(p.value)
	}
	return strings.Join(values, ", ")
}

// generateNew emits New{Type}Rn(parent, id): the child of whichever parent pattern
// parent matches, so callers never switch on parent shapes themselves.
func (r *resource) generateNew(g *protogen.GeneratedFile) {
	name := r.typeName()
	id := r.patterns[0].id()
	for _, p := range r.patterns[1:] {
		if p.id() != id {
			// Patterns disagree on the own identifier: no uniform constructor exists.
			return
		}
	}
	returnType := "*" + name
	if r.multi() {
		returnType = name
	}
	g.P()
	if id == "" {
		g.P("// New", name, " is the singleton under parent, which must follow one of: ", r.parentList(), ".")
		g.P("func New", name, "(parent string) (", returnType, ", error) {")
	} else {
		g.P("// New", name, " is the resource named ", paramName(id), " under parent, which must follow one of: ", r.parentList(), ".")
		g.P("func New", name, "(parent string, ", paramName(id), " string) (", returnType, ", error) {")
	}
	g.P("switch {")
	for _, p := range r.patterns {
		if p.parent == "" {
			g.P(`case parent == "":`)
		} else {
			g.P("case ", resourcenamePkg.Ident("Match"), "(", strconv.Quote(p.parent), ", parent):")
		}
		g.P("n := &", p.typeName, "{}")
		if p.parent != "" {
			g.P("if err := ", resourcenamePkg.Ident("Sscan"), "(parent, ", strconv.Quote(p.parent), ", ", addrList(p.parentVariables()), "); err != nil {")
			g.P("return nil, err")
			g.P("}")
		}
		if id != "" {
			g.P("n.", fieldName(id), " = ", paramName(id))
		}
		g.P("return n, nil")
	}
	g.P("}")
	g.P("return nil, ", fmtPkg.Ident("Errorf"), `("parent %q matches no parent pattern of `, r.desc.GetType(), `", parent)`)
	g.P("}")
}

func (r *resource) parentList() string {
	values := make([]string, len(r.patterns))
	for i, p := range r.patterns {
		values[i] = strconv.Quote(p.parent)
	}
	return strings.Join(values, ", ")
}

func (r *resource) generatePattern(g *protogen.GeneratedFile, p *pattern) {
	name := p.typeName
	g.P()
	g.P("// ", name, "Pattern is the pattern ", name, " follows.")
	g.P("const ", name, "Pattern = ", strconv.Quote(p.value))
	g.P()
	g.P("// ", name, " is the resource name ", strconv.Quote(p.value), ".")
	g.P("type ", name, " struct {")
	for _, v := range p.variables {
		g.P(fieldName(v), " string")
	}
	g.P("}")
	if r.multi() {
		g.P()
		g.P("func (*", name, ") is", r.typeName(), "() {}")
	}

	g.P()
	g.P("// Parse", name, " parses and validates name against ", name, "Pattern.")
	g.P("func Parse", name, "(name string) (*", name, ", error) {")
	g.P("n := &", name, "{}")
	g.P("if err := n.UnmarshalString(name); err != nil {")
	g.P("return nil, err")
	g.P("}")
	g.P("return n, nil")
	g.P("}")

	g.P()
	g.P("// Match", name, " reports whether name follows ", name, "Pattern.")
	g.P("func Match", name, "(name string) bool {")
	g.P("return ", resourcenamePkg.Ident("Match"), "(", name, "Pattern, name)")
	g.P("}")

	if p.parentResource != nil {
		parentType, err := r.typeNameOf(p.parentResource, p.parent)
		if err == nil {
			r.generateChildConstructor(g, p, parentType)
			defer r.generateParentAccessor(g, p, parentType)
		}
	}

	g.P()
	g.P("func (n *", name, ") Validate() error {")
	for _, v := range p.variables {
		g.P("if n.", fieldName(v), ` == "" {`)
		g.P("return ", fmtPkg.Ident("Errorf"), `("`, v, `: empty")`)
		g.P("}")
		g.P("if ", stringsPkg.Ident("IndexByte"), "(n.", fieldName(v), ", '/') != -1 {")
		g.P("return ", fmtPkg.Ident("Errorf"), `("`, v, `: contains illegal character '/'")`)
		g.P("}")
	}
	g.P("return nil")
	g.P("}")

	g.P()
	g.P("func (n *", name, ") ContainsWildcard() bool {")
	terms := make([]string, len(p.variables))
	for i, v := range p.variables {
		terms[i] = "n." + fieldName(v) + " == " + g.QualifiedGoIdent(aipPkg.Ident("Wildcard"))
	}
	if len(terms) == 0 {
		terms = []string{"false"}
	}
	g.P("return ", strings.Join(terms, " || "))
	g.P("}")

	g.P()
	g.P("func (n *", name, ") String() string {")
	g.P("return ", resourcenamePkg.Ident("Sprint"), "(", name, "Pattern, ", varList(p.variables), ")")
	g.P("}")

	g.P()
	g.P("// MarshalText implements encoding.TextMarshaler.")
	g.P("func (n *", name, ") MarshalText() ([]byte, error) {")
	g.P("if err := n.Validate(); err != nil {")
	g.P("return nil, err")
	g.P("}")
	g.P("return []byte(n.String()), nil")
	g.P("}")

	g.P()
	g.P("// UnmarshalString parses and validates name against ", name, "Pattern.")
	g.P("func (n *", name, ") UnmarshalString(name string) error {")
	g.P("if err := ", resourcenamePkg.Ident("Sscan"), "(name, ", name, "Pattern, ", addrList(p.variables), "); err != nil {")
	g.P("return err")
	g.P("}")
	g.P("return n.Validate()")
	g.P("}")

	g.P()
	g.P("// UnmarshalText implements encoding.TextUnmarshaler.")
	g.P("func (n *", name, ") UnmarshalText(text []byte) error {")
	g.P("return n.UnmarshalString(string(text))")
	g.P("}")

	g.P()
	g.P("func (n *", name, ") Type() string { return ", r.typeName(), "Type }")
	g.P()
	g.P("func (n *", name, ") Pattern() string { return ", name, "Pattern }")
	g.P()
	if id := p.id(); id != "" {
		g.P("func (n *", name, ") ID() string { return n.", fieldName(id), " }")
	} else {
		g.P(`func (n *`, name, `) ID() string { return "" }`)
	}
	g.P()
	if p.parent == "" {
		g.P(`func (n *`, name, `) Parent() string { return "" }`)
	} else {
		g.P("func (n *", name, ") Parent() string {")
		g.P("return ", resourcenamePkg.Ident("Sprint"), "(", strconv.Quote(p.parent), ", ", varList(p.parentVariables()), ")")
		g.P("}")
	}
}

// generateChildConstructor emits `parent.{Child}Rn(id)` on the parent's struct.
func (r *resource) generateChildConstructor(g *protogen.GeneratedFile, p *pattern, parentType string) {
	g.P()
	g.P("// ", p.typeName, " returns the child ", r.desc.GetType(), " of n.")
	if id := p.id(); id != "" {
		g.P("func (n *", parentType, ") ", p.typeName, "(", paramName(id), " string) *", p.typeName, " {")
	} else {
		g.P("func (n *", parentType, ") ", p.typeName, "() *", p.typeName, " {")
	}
	g.P("return &", p.typeName, "{")
	for _, v := range p.parentVariables() {
		g.P(fieldName(v), ": n.", fieldName(v), ",")
	}
	if id := p.id(); id != "" {
		g.P(fieldName(id), ": ", paramName(id), ",")
	}
	g.P("}")
	g.P("}")
}

// generateParentAccessor emits `n.{Parent}Rn()`.
func (r *resource) generateParentAccessor(g *protogen.GeneratedFile, p *pattern, parentType string) {
	g.P()
	g.P("// ", parentType, " returns the parent of n.")
	g.P("func (n *", p.typeName, ") ", parentType, "() *", parentType, " {")
	g.P("return &", parentType, "{")
	for _, v := range p.parentVariables() {
		g.P(fieldName(v), ": n.", fieldName(v), ",")
	}
	g.P("}")
	g.P("}")
}

func varList(variables []string) string {
	parts := make([]string, len(variables))
	for i, v := range variables {
		parts[i] = "n." + fieldName(v)
	}
	return strings.Join(parts, ", ")
}

func addrList(variables []string) string {
	parts := make([]string, len(variables))
	for i, v := range variables {
		parts[i] = "&n." + fieldName(v)
	}
	return strings.Join(parts, ", ")
}

func fieldName(variable string) string { return xstrings.ToPascalCase(variable) }
func paramName(variable string) string { return xstrings.ToCamelCase(variable) }
