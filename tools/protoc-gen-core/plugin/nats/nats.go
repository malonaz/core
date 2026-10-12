// Package nats holds helpers shared by the NATS stream, event and rpc generators.
package nats

import (
	"regexp"
	"strings"

	"github.com/huandu/xstrings"
	"google.golang.org/protobuf/compiler/protogen"
)

var (
	versionPrefixRe = regexp.MustCompile(`^.*\.v[0-9]+\.`)

	syncPkg     = protogen.GoImportPath("sync")
	natsPkg     = protogen.GoImportPath("github.com/malonaz/core/go/nats")
	natsOptsPkg = protogen.GoImportPath("github.com/malonaz/core/genproto/nats/v1")
)

// StreamGoName derives the Go type name of a stream from its fully qualified
// name by stripping the package/version prefix:
// "malonaz.test.library.v1.book" → "BookStream".
func StreamGoName(streamFQN string) string {
	suffix := versionPrefixRe.ReplaceAllString(streamFQN, "")
	return xstrings.ToPascalCase(strings.ReplaceAll(suffix, ".", "-")) + "Stream"
}

// GenerateStreamSingleton emits a lazily-initialized singleton accessor wrapping a NATS stream.
func GenerateStreamSingleton(g *protogen.GeneratedFile, streamFQN, streamGoName string) {
	varName := xstrings.ToCamelCase(streamGoName)
	ident := func(path protogen.GoImportPath, name string) string {
		return g.QualifiedGoIdent(protogen.GoIdent{GoName: name, GoImportPath: path})
	}

	g.P("var (")
	g.P("  ", varName, "Once ", ident(syncPkg, "Once"))
	g.P("  ", varName, "Val *", streamGoName)
	g.P(")")
	g.P()

	g.P("type ", streamGoName, " struct {")
	g.P("  stream *", ident(natsPkg, "Stream"))
	g.P("}")
	g.P()

	g.P("func Get", streamGoName, "() *", streamGoName, " {")
	g.P("  ", varName, "Once.Do(func() {")
	g.P("    ", varName, "Val = &", streamGoName, "{")
	g.P("      stream: ", ident(natsPkg, "NewStream"), "(&", ident(natsOptsPkg, "StreamOptions"), "{Name: \"", streamFQN, "\"}),")
	g.P("    }")
	g.P("  })")
	g.P("  return ", varName, "Val")
	g.P("}")
	g.P()

	g.P("func (s *", streamGoName, ") Get() *", ident(natsPkg, "Stream"), " {")
	g.P("  return s.stream")
	g.P("}")
	g.P()
}
