package authentication

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"unicode"

	"go.einride.tech/aip/resourcename"
	"google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	authenticationpb "github.com/malonaz/core/genproto/authentication/v2"
	"github.com/malonaz/core/go/aip"
)

// resourcesFunc reads the resource names a request names.
type resourcesFunc func(request protoreflect.Message) []string

// streamMethodType is no AIP standard method, but is named and shaped as a List.
const streamMethodType aip.StandardMethodType = "Stream"

// Verbs whose request names its resources elsewhere than in `name`.
var resourceVerbs = []aip.StandardMethodType{
	aip.StandardMethodTypeBatchCreate,
	aip.StandardMethodTypeBatchGet,
	aip.StandardMethodTypeCreate,
	aip.StandardMethodTypeUpdate,
	aip.StandardMethodTypeList,
	aip.StandardMethodTypeSearch,
	streamMethodType,
}

// deriveResources returns, per method, how to read the resources its request names, nil for a method whose
// request has no shape this knows.
func deriveResources(methodToDescriptor map[string]protoreflect.MethodDescriptor) map[string]resourcesFunc {
	methodToResources := make(map[string]resourcesFunc, len(methodToDescriptor))
	for method, descriptor := range methodToDescriptor {
		methodToResources[method] = methodResources(descriptor)
	}
	return methodToResources
}

// methodResources derives the resources a method names from its AIP shape: Create, List, Search and Stream its
// parent; Update its payload's name; BatchGet its parent and every name; BatchCreate its parent and every request's;
// every other method, Get and Delete included, its name.
func methodResources(method protoreflect.MethodDescriptor) resourcesFunc {
	// Only a stream's first message is read, and the next could name anything.
	if method.IsStreamingClient() {
		return nil
	}
	request := method.Input()
	switch methodVerb(method.Name()) {
	case aip.StandardMethodTypeCreate, aip.StandardMethodTypeList, aip.StandardMethodTypeSearch, streamMethodType:
		return fieldResources(stringField(request, "parent"))
	case aip.StandardMethodTypeUpdate:
		return updateResources(request)
	case aip.StandardMethodTypeBatchGet:
		return batchGetResources(request)
	case aip.StandardMethodTypeBatchCreate:
		return batchCreateResources(request)
	default:
		return fieldResources(stringField(request, "name"))
	}
}

// methodVerb returns the verb a method's name starts with, as a whole word: ListBooks is a List, Listen is not.
func methodVerb(name protoreflect.Name) aip.StandardMethodType {
	for _, verb := range resourceVerbs {
		rest, ok := strings.CutPrefix(string(name), string(verb))
		if ok && rest != "" && unicode.IsUpper(rune(rest[0])) {
			return verb
		}
	}
	return aip.StandardMethodTypeUnspecified
}

func fieldResources(field protoreflect.FieldDescriptor) resourcesFunc {
	if field == nil {
		return nil
	}
	return func(request protoreflect.Message) []string {
		return []string{request.Get(field).String()}
	}
}

// updateResources reads the name of the resource an Update carries, in its request's one resource field.
func updateResources(request protoreflect.MessageDescriptor) resourcesFunc {
	var payload protoreflect.FieldDescriptor
	fields := request.Fields()
	for i := range fields.Len() {
		field := fields.Get(i)
		if field.Message() == nil || field.IsList() || !proto.HasExtension(field.Message().Options(), annotations.E_Resource) {
			continue
		}
		if payload != nil {
			return nil
		}
		payload = field
	}
	if payload == nil {
		return nil
	}
	name := stringField(payload.Message(), "name")
	if name == nil {
		return nil
	}
	return func(request protoreflect.Message) []string {
		return []string{request.Get(payload).Message().Get(name).String()}
	}
}

// batchGetResources reads every name a BatchGet asks for, and its parent when set: a handler may not hold the
// names to it.
func batchGetResources(request protoreflect.MessageDescriptor) resourcesFunc {
	parent, names := stringField(request, "parent"), request.Fields().ByName("names")
	if names == nil || !names.IsList() || names.Kind() != protoreflect.StringKind {
		return nil
	}
	return func(request protoreflect.Message) []string {
		list := request.Get(names).List()
		resources := make([]string, 0, list.Len()+1)
		if parent != nil && request.Has(parent) {
			resources = append(resources, request.Get(parent).String())
		}
		for i := range list.Len() {
			resources = append(resources, list.Get(i).String())
		}
		return resources
	}
}

// batchCreateResources reads the parent of every resource a BatchCreate makes: each request's, or the batch's it
// inherits when unset; and the batch's when set.
func batchCreateResources(request protoreflect.MessageDescriptor) resourcesFunc {
	parent, requests := stringField(request, "parent"), request.Fields().ByName("requests")
	if requests == nil || !requests.IsList() || requests.Message() == nil {
		return nil
	}
	requestParent := stringField(requests.Message(), "parent")
	if requestParent == nil {
		return nil
	}
	return func(request protoreflect.Message) []string {
		var batchParent string
		list := request.Get(requests).List()
		resources := make([]string, 0, list.Len()+1)
		if parent != nil && request.Has(parent) {
			batchParent = request.Get(parent).String()
			resources = append(resources, batchParent)
		}
		for i := range list.Len() {
			resource := list.Get(i).Message().Get(requestParent).String()
			if resource == "" {
				resource = batchParent
			}
			resources = append(resources, resource)
		}
		return resources
	}
}

// stringField returns a message's singular string field of that name, nil when it has none.
func stringField(message protoreflect.MessageDescriptor, name protoreflect.Name) protoreflect.FieldDescriptor {
	field := message.Fields().ByName(name)
	if field == nil || field.IsList() || field.Kind() != protoreflect.StringKind {
		return nil
	}
	return field
}

// covers reports whether a scope covers every resource a call names, a call naming none never: segment for segment,
// organizations/1 covers itself and organizations/1/shelves/-, never organizations/12 or organizations/-.
func covers(scope string, resources []string) bool {
	if len(resources) == 0 || !validScope(scope) {
		return false
	}
	for _, resource := range resources {
		if resource != scope && !strings.HasPrefix(resource, scope+"/") {
			return false
		}
	}
	return true
}

// validScope reports whether a scope other than "*" can cover anything: a wildcard in it would cover names with
// the same wildcard, which mean any resource.
func validScope(scope string) bool {
	return scope != "" && !resourcename.ContainsWildcard(scope)
}

// checkScope refuses a service account's scoped grant that some method of its role could never be allowed under.
func (i *Interceptor) checkScope(grant *authenticationpb.Grant, methods methodToConditions) error {
	scope := grant.GetScope()
	if scope == wildcardScope {
		return nil
	}
	if !validScope(scope) {
		return fmt.Errorf("scope %q of role %q: a scope is %q or a resource name without wildcards", scope, grant.GetRole(), wildcardScope)
	}
	for _, method := range slices.Sorted(maps.Keys(methods)) {
		if i.methodToResources[method] == nil {
			return fmt.Errorf("scope %q of role %q: the resources %s names cannot be derived", scope, grant.GetRole(), method)
		}
	}
	return nil
}
