package middleware

import (
	"context"
	"fmt"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	canonicalizepb "github.com/malonaz/core/genproto/canonicalize/v1"
	"github.com/malonaz/core/go/canonicalize"
	"github.com/malonaz/core/go/pbutil"
)

// UnaryServerCanonicalize returns a unary server interceptor that canonicalizes
// incoming request messages based on proto field annotations (e.g., normalizing
// email addresses to lowercase, formatting phone numbers to E.164).
func UnaryServerCanonicalize() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if message, ok := req.(proto.Message); ok {
			if err := canonicalizeMessage(message); err != nil {
				return nil, err
			}
		}
		return handler(ctx, req)
	}
}

// StreamServerCanonicalize returns a stream server interceptor that wraps the
// server stream to canonicalize each received message before it reaches the handler.
func StreamServerCanonicalize() grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		return handler(srv, &canonicalizeServerStream{ServerStream: stream})
	}
}

// canonicalizeServerStream wraps a grpc.ServerStream to intercept RecvMsg calls,
// applying canonicalization to each incoming message.
type canonicalizeServerStream struct {
	grpc.ServerStream
}

func (s *canonicalizeServerStream) RecvMsg(m any) error {
	if err := s.ServerStream.RecvMsg(m); err != nil {
		return err
	}
	if message, ok := m.(proto.Message); ok {
		if err := canonicalizeMessage(message); err != nil {
			return err
		}
	}
	return nil
}

// canonicalizationError holds the field path and description so we can produce
// a structured FieldViolation in the gRPC error details.
type canonicalizationError struct {
	field       string
	description string
}

func (e *canonicalizationError) Error() string {
	return fmt.Sprintf("field %s: %s", e.field, e.description)
}

// canonicalizeMessage is the entry point for recursively walking a proto message
// and applying canonicalize rules defined via the (canonicalize.field) extension.
func canonicalizeMessage(message proto.Message) error {
	if err := canonicalizeReflectMessage(message.ProtoReflect()); err != nil {
		// Build a BadRequest error detail with a FieldViolation so clients can
		// programmatically identify which field failed canonicalization.
		canonicalizationError := err.(*canonicalizationError)
		badRequest := &errdetails.BadRequest{
			FieldViolations: []*errdetails.BadRequest_FieldViolation{
				{
					Field:       canonicalizationError.field,
					Description: canonicalizationError.description,
				},
			},
		}
		st, attachErr := status.New(codes.InvalidArgument, fmt.Sprintf("canonicalization failed: %s", canonicalizationError.description)).WithDetails(badRequest)
		if attachErr != nil {
			return status.Errorf(codes.Internal, "canonicalization failed and could not attach details: %v", attachErr)
		}
		return st.Err()
	}
	return nil
}

// canonicalizeReflectMessage recursively traverses all fields of a protoreflect.Message.
// For message-typed fields (singular, list, map), it recurses into them.
// For string fields annotated with the canonicalize extension, it applies the
// appropriate transformation (email normalization, phone E.164 formatting, etc.).
func canonicalizeReflectMessage(reflectMessage protoreflect.Message) error {
	var canonicalizeErr error
	reflectMessage.Range(func(fieldDescriptor protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		// Recurse into nested message fields (singular, repeated, and map values).
		if fieldDescriptor.Kind() == protoreflect.MessageKind || fieldDescriptor.Kind() == protoreflect.GroupKind {
			if fieldDescriptor.IsList() {
				list := value.List()
				for i := range list.Len() {
					if err := canonicalizeReflectMessage(list.Get(i).Message()); err != nil {
						canonicalizeErr = err
						return false
					}
				}
			} else if fieldDescriptor.IsMap() {
				// Only recurse into map values that are themselves messages.
				if fieldDescriptor.MapValue().Kind() == protoreflect.MessageKind {
					value.Map().Range(func(_ protoreflect.MapKey, mapValue protoreflect.Value) bool {
						if err := canonicalizeReflectMessage(mapValue.Message()); err != nil {
							canonicalizeErr = err
							return false
						}
						return true
					})
					if canonicalizeErr != nil {
						return false
					}
				}
			} else {
				if err := canonicalizeReflectMessage(value.Message()); err != nil {
					canonicalizeErr = err
					return false
				}
			}
			return true
		}

		// Skip fields that don't have the canonicalize extension.
		if !proto.HasExtension(fieldDescriptor.Options(), canonicalizepb.E_Field) {
			return true
		}
		fieldRules, err := pbutil.GetExtension[*canonicalizepb.FieldRules](fieldDescriptor.Options(), canonicalizepb.E_Field)
		if err != nil {
			canonicalizeErr = &canonicalizationError{
				field:       string(fieldDescriptor.FullName()),
				description: fmt.Sprintf("get extension: %v", err),
			}
			return false
		}
		if fieldRules == nil {
			canonicalizeErr = &canonicalizationError{
				field:       string(fieldDescriptor.FullName()),
				description: "expected non-nil field rules",
			}
			return false
		}

		// Only string fields support canonicalization for now.
		if fieldDescriptor.Kind() != protoreflect.StringKind {
			canonicalizeErr = &canonicalizationError{
				field:       string(fieldDescriptor.FullName()),
				description: "canonicalize extension set on non-string field",
			}
			return false
		}

		// Handle repeated string fields by canonicalizing each element in place.
		if fieldDescriptor.IsList() {
			list := value.List()
			for i := range list.Len() {
				stringValue := list.Get(i).String()
				if stringValue == "" {
					continue
				}
				if fieldRules.GetEmailAddress() {
					list.Set(i, protoreflect.ValueOfString(canonicalize.EmailAddress(stringValue)))
				} else if fieldRules.GetPhoneNumberE164() {
					canonicalized, err := canonicalize.PhoneNumber(stringValue, canonicalize.RegionCodeUS)
					if err != nil {
						canonicalizeErr = &canonicalizationError{
							field:       fmt.Sprintf("%s[%d]", fieldDescriptor.FullName(), i),
							description: err.Error(),
						}
						return false
					}
					list.Set(i, protoreflect.ValueOfString(canonicalized))
				}
			}
			return true
		}

		// Handle singular string fields.
		stringValue := value.String()
		if stringValue == "" {
			return true
		}

		if fieldRules.GetEmailAddress() {
			reflectMessage.Set(fieldDescriptor, protoreflect.ValueOfString(canonicalize.EmailAddress(stringValue)))
		} else if fieldRules.GetPhoneNumberE164() {
			canonicalized, err := canonicalize.PhoneNumber(stringValue, canonicalize.RegionCodeUS)
			if err != nil {
				canonicalizeErr = &canonicalizationError{
					field:       string(fieldDescriptor.FullName()),
					description: err.Error(),
				}
				return false
			}
			reflectMessage.Set(fieldDescriptor, protoreflect.ValueOfString(canonicalized))
		}

		return true
	})
	return canonicalizeErr
}
