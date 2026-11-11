// Package nats holds helpers shared by the NATS stream, event and rpc generators.
package nats

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/huandu/xstrings"
	"google.golang.org/protobuf/compiler/protogen"

	codegennatspb "github.com/malonaz/core/genproto/codegen/nats/v1"
	"github.com/malonaz/core/go/pbutil"

	natspb "github.com/malonaz/core/genproto/nats/v1"
)

var (
	versionPrefixRe = regexp.MustCompile(`^.*\.v[0-9]+\.`)

	syncPkg     = protogen.GoImportPath("sync")
	natsPkg     = protogen.GoImportPath("github.com/malonaz/core/go/nats")
	natsOptsPkg = protogen.GoImportPath("github.com/malonaz/core/genproto/nats/v1")
	durationPkg = protogen.GoImportPath("google.golang.org/protobuf/types/known/durationpb")
)

// StreamGoName derives the Go type name of a stream from its fully qualified
// name by stripping the package/version prefix:
// "malonaz.test.library.v1.book" → "BookStream".
func StreamGoName(streamFQN string) string {
	suffix := versionPrefixRe.ReplaceAllString(streamFQN, "")
	return xstrings.ToPascalCase(strings.ReplaceAll(suffix, ".", "-")) + "Stream"
}

// GenerateStreamSingleton emits a lazily-initialized singleton accessor wrapping a NATS stream.
// The stream carries every option set in streamOptions, so that CreateOrUpdateStream on it
// applies the proto's limits rather than resetting them.
func GenerateStreamSingleton(g *protogen.GeneratedFile, streamOptions *natspb.StreamOptions, streamGoName string) {
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
	g.P("      stream: ", ident(natsPkg, "NewStream"), "(&", ident(natsOptsPkg, "StreamOptions"), "{")
	g.P("        Name: ", strconv.Quote(streamOptions.GetName()), ",")
	if maxAge := streamOptions.GetMaxAge(); maxAge != nil {
		g.P("        MaxAge: &", ident(durationPkg, "Duration"), "{Seconds: ", maxAge.GetSeconds(), ", Nanos: ", maxAge.GetNanos(), "},")
	}
	if maxBytes := streamOptions.GetMaxBytes(); maxBytes != 0 {
		g.P("        MaxBytes: ", maxBytes, ",")
	}
	if maxMsgs := streamOptions.GetMaxMsgs(); maxMsgs != 0 {
		g.P("        MaxMsgs: ", maxMsgs, ",")
	}
	if maxMsgSize := streamOptions.GetMaxMsgSize(); maxMsgSize != 0 {
		g.P("        MaxMsgSize: ", maxMsgSize, ",")
	}
	if replicas := streamOptions.GetReplicas(); replicas != 0 {
		g.P("        Replicas: ", replicas, ",")
	}
	if retention := streamOptions.GetRetention(); retention != 0 {
		g.P("        Retention: ", ident(natsOptsPkg, "RetentionPolicy_"+retention.String()), ",")
	}
	if storage := streamOptions.GetStorage(); storage != 0 {
		g.P("        Storage: ", ident(natsOptsPkg, "StorageType_"+storage.String()), ",")
	}
	if discard := streamOptions.GetDiscard(); discard != 0 {
		g.P("        Discard: ", ident(natsOptsPkg, "DiscardPolicy_"+discard.String()), ",")
	}
	g.P("      }),")
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

// Outbox reports whether a message's events are published to its schema's
// outbox journal in the write's own transaction, rather than inline by the RPC.
func Outbox(message *protogen.Message) (bool, error) {
	eventOptions, err := pbutil.GetExtension[*codegennatspb.EventOptions](message.Desc.Options(), codegennatspb.E_Event)
	if err != nil {
		if errors.Is(err, pbutil.ErrExtensionNotFound) {
			return false, nil
		}
		return false, fmt.Errorf("getting nats event opts for %s: %w", message.GoIdent.GoName, err)
	}
	return eventOptions.GetOutbox(), nil
}
