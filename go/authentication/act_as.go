package authentication

import (
	"fmt"
	"strings"

	"github.com/google/cel-go/cel"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	authenticationpb "github.com/malonaz/core/genproto/authentication/v1"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/pbutil"
)

// actAsAuthorizer decides who may make a resource act as an identity other than their own: the methods carrying a
// principal_field name the identity, and each service account's act_as_cel who may act as it.
type actAsAuthorizer struct {
	serviceAccountIDToServiceAccount map[string]*authenticationpb.ServiceAccount
	serviceAccountIDToProgram        map[string]cel.Program
	// methodToPrincipalPath holds, per method, the fields leading from its request to the principal it names.
	methodToPrincipalPath map[string][]protoreflect.FieldDescriptor
}

func newActAsAuthorizer(serviceAccountIDToServiceAccount map[string]*authenticationpb.ServiceAccount) (*actAsAuthorizer, error) {
	environment, err := cel.NewEnv(sessionCelOptions...)
	if err != nil {
		return nil, fmt.Errorf("creating act-as CEL environment: %w", err)
	}
	serviceAccountIDToProgram := map[string]cel.Program{}
	for serviceAccountID, serviceAccount := range serviceAccountIDToServiceAccount {
		if serviceAccount.GetActAsCel() == "" {
			continue
		}
		program, err := compileBoolCel(environment, serviceAccount.GetActAsCel())
		if err != nil {
			return nil, fmt.Errorf("compiling act-as CEL of service account %q: %w", serviceAccountID, err)
		}
		serviceAccountIDToProgram[serviceAccountID] = program
	}

	methodToPrincipalPath := map[string][]protoreflect.FieldDescriptor{}
	var rangeErr error
	rangeMethods(func(fullMethod string, method protoreflect.MethodDescriptor) bool {
		principalField, _ := proto.GetExtension(method.Options(), authenticationpb.E_PrincipalField).(string)
		if principalField == "" {
			return true
		}
		path, err := resolvePrincipalPath(method.Input(), principalField)
		if err != nil {
			rangeErr = fmt.Errorf("resolving the principal field of %s: %w", fullMethod, err)
			return false
		}
		methodToPrincipalPath[fullMethod] = path
		return true
	})
	if rangeErr != nil {
		return nil, rangeErr
	}
	return &actAsAuthorizer{
		serviceAccountIDToServiceAccount: serviceAccountIDToServiceAccount,
		serviceAccountIDToProgram:        serviceAccountIDToProgram,
		methodToPrincipalPath:            methodToPrincipalPath,
	}, nil
}

// resolvePrincipalPath resolves a dotted path of singular message fields ending on an Identity.
func resolvePrincipalPath(message protoreflect.MessageDescriptor, principalField string) ([]protoreflect.FieldDescriptor, error) {
	var path []protoreflect.FieldDescriptor
	for _, name := range strings.Split(principalField, ".") {
		if message == nil {
			return nil, fmt.Errorf("%q descends into a non-message field", principalField)
		}
		field := message.Fields().ByName(protoreflect.Name(name))
		if field == nil || field.Kind() != protoreflect.MessageKind || field.IsList() || field.IsMap() {
			return nil, fmt.Errorf("%q: %s has no singular message field %q", principalField, message.FullName(), name)
		}
		path = append(path, field)
		message = field.Message()
	}
	if want := (&authenticationpb.Identity{}).ProtoReflect().Descriptor().FullName(); message.FullName() != want {
		return nil, fmt.Errorf("%q is a %s, not a %s", principalField, message.FullName(), want)
	}
	return path, nil
}

// authorizeRequest checks that the caller may act as the principal a request names, if its method names one.
func (a *actAsAuthorizer) authorizeRequest(fullMethod string, session *authenticationpb.Session, request any) error {
	path, ok := a.methodToPrincipalPath[fullMethod]
	if !ok {
		return nil
	}
	// A bidi stream's request is never peeked.
	requestMessage, ok := request.(proto.Message)
	if !ok {
		return nil
	}
	message := requestMessage.ProtoReflect()
	for _, field := range path {
		if !message.Has(field) {
			return nil
		}
		message = message.Get(field).Message()
	}
	principal, ok := message.Interface().(*authenticationpb.Identity)
	if !ok {
		// A peeked stream request is dynamic.
		bytes, err := pbutil.Marshal(message.Interface())
		if err != nil {
			return status.Errorf(codes.Internal, "marshaling principal: %v", err).Err()
		}
		principal = &authenticationpb.Identity{}
		if err := pbutil.Unmarshal(bytes, principal); err != nil {
			return status.Errorf(codes.Internal, "unmarshaling principal: %v", err).Err()
		}
	}
	return a.authorize(session, principal)
}

// authorize checks that a session may act as a principal. A JWT identity cannot be named, only copied from its own
// session: claims are verified once, by the issuer. A service account may be acted as by itself, and by the
// sessions its act_as_cel admits.
func (a *actAsAuthorizer) authorize(session *authenticationpb.Session, principal *authenticationpb.Identity) error {
	serviceAccountIdentity := principal.GetServiceAccountIdentity()
	if serviceAccountIdentity == nil {
		return status.Errorf(codes.PermissionDenied, "only a service account may be named as a principal").Err()
	}
	serviceAccountID := serviceAccountIdentity.GetServiceAccountId()
	serviceAccount, ok := a.serviceAccountIDToServiceAccount[serviceAccountID]
	if !ok || serviceAccount.GetType() != serviceAccountIdentity.GetServiceAccountType() {
		return status.Errorf(codes.PermissionDenied, "unknown service account %q of type %s", serviceAccountID, serviceAccountIdentity.GetServiceAccountType()).Err()
	}
	if session.GetServiceAccountIdentity().GetServiceAccountId() == serviceAccountID {
		return nil
	}
	program, ok := a.serviceAccountIDToProgram[serviceAccountID]
	if !ok {
		return status.Errorf(codes.PermissionDenied, "may not act as service account %q", serviceAccountID).Err()
	}
	allowed, err := evalBool(program, sessionCelVariables(session))
	if err != nil {
		return status.Errorf(codes.Internal, "evaluating act-as authorization of service account %q: %v", serviceAccountID, err).Err()
	}
	if !allowed {
		return status.Errorf(codes.PermissionDenied, "may not act as service account %q", serviceAccountID).Err()
	}
	return nil
}
