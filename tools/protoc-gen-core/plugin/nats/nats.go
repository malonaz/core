// Package nats holds helpers shared by the NATS stream, event and rpc generators.
package nats

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/huandu/xstrings"
	"google.golang.org/protobuf/compiler/protogen"

	natspb "github.com/malonaz/core/genproto/codegen/nats/v1"
	"github.com/malonaz/core/go/pbutil"
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

// EventOptions returns a message's NATS event options, nil when it declares
// none.
func EventOptions(message *protogen.Message) (*natspb.EventOptions, error) {
	eventOptions, err := pbutil.GetExtension[*natspb.EventOptions](message.Desc.Options(), natspb.E_Event)
	if err != nil {
		if errors.Is(err, pbutil.ErrExtensionNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("getting nats event opts for %s: %w", message.GoIdent.GoName, err)
	}
	return eventOptions, nil
}

// Outbox reports whether a message's events are journaled in the transaction
// of the write that caused them, rather than published inline by the RPC.
func Outbox(message *protogen.Message) (bool, error) {
	eventOptions, err := EventOptions(message)
	if err != nil {
		return false, err
	}
	return eventOptions.GetOutbox(), nil
}
