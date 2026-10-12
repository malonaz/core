package logging

import (
	"context"

	"google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/malonaz/core/go/pbutil"
)

var logFieldsTagKey = logFieldsTagKeyType("log-fields-tag-key")

type logFieldsTagKeyType string

type logFieldsTag struct {
	fields []any
}

// WithLogFieldsTag attaches an empty fields tag for one unit of work, e.g. an RPC or a NATS message.
// Code deeper in the call stack adds fields with InjectLogFields; the owner of the unit of work
// reads them with GetFields when it logs the outcome.
//
// Loggers from NewLogger emit the tag's fields on every *Context call; plain calls without ctx do not.
// The tag is mutable and unsynchronized: do not share one across goroutines.
// Calling it on a tagged ctx starts a new tag that hides the parent's fields.
func WithLogFieldsTag(ctx context.Context) context.Context {
	tag := &logFieldsTag{}
	return context.WithValue(ctx, logFieldsTagKey, tag)
}

// InjectLogFields appends key/value pairs to ctx's tag. Returns false, dropping them, if ctx has no tag.
func InjectLogFields(ctx context.Context, args ...any) bool {
	tag, ok := ctx.Value(logFieldsTagKey).(*logFieldsTag)
	if !ok {
		return false
	}
	tag.fields = append(tag.fields, args...)
	return true
}

// extractLogFields is the ExtractFromContextFn that emits a ctx's tag on every *Context log call.
func extractLogFields(ctx context.Context) []any {
	fields, _ := GetFields(ctx)
	return fields
}

// GetFields returns the key/value pairs injected into ctx's tag, or false if ctx has no tag.
func GetFields(ctx context.Context) ([]any, bool) {
	tag, ok := ctx.Value(logFieldsTagKey).(*logFieldsTag)
	if !ok {
		return nil, false
	}
	return tag.fields, true
}

// InjectAIPLogFields injects message's resource names, keyed keyPrefix + field path.
func InjectAIPLogFields(ctx context.Context, keyPrefix string, message proto.Message) {
	injectResourceReferenceFields(ctx, message.ProtoReflect(), keyPrefix, "", false)
}

func injectResourceReferenceFields(ctx context.Context, reflectMessage protoreflect.Message, keyPrefix, prefix string, nested bool) {
	descriptor := reflectMessage.Descriptor()
	fields := descriptor.Fields()

	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		if field.IsList() || field.IsMap() {
			continue
		}

		fieldName := string(field.Name())
		key := keyPrefix + prefix + fieldName

		if field.Kind() == protoreflect.StringKind {
			_, refErr := pbutil.GetExtension[*annotations.ResourceReference](field.Options(), annotations.E_ResourceReference)
			if refErr != nil {
				if !nested || fieldName != "name" {
					continue
				}
				behaviors, behaviorErr := pbutil.GetExtension[[]annotations.FieldBehavior](field.Options(), annotations.E_FieldBehavior)
				if behaviorErr != nil {
					continue
				}
				isIdentifier := false
				for _, b := range behaviors {
					if b == annotations.FieldBehavior_IDENTIFIER {
						isIdentifier = true
						break
					}
				}
				if !isIdentifier {
					continue
				}
			}
			InjectLogFields(ctx, key, reflectMessage.Get(field).String())
			continue
		}

		if !nested && field.Kind() == protoreflect.MessageKind && reflectMessage.Has(field) {
			injectResourceReferenceFields(ctx, reflectMessage.Get(field).Message(), keyPrefix, fieldName+".", true)
		}
	}
}
