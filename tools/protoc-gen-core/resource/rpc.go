package resource

import (
	"fmt"

	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/proto"

	aippb "github.com/malonaz/core/genproto/codegen/aip/v1"
)

// RPC represents a parsed standard AIP method bound to its target resource.
type RPC struct {
	StandardMethod                              *aippb.StandardMethod
	Message                                     *protogen.Message
	ParsedResource                              *ParsedResource
	Create, Update, Delete, Get, BatchGet, List bool
}

// ParseRPC extracts the standard method annotation from a gRPC method and resolves
// it to a parsed resource.
func ParseRPC(method *protogen.Method) (*RPC, error) {
	methodOpts := method.Desc.Options()
	if methodOpts == nil {
		return nil, nil
	}
	if !proto.HasExtension(methodOpts, aippb.E_StandardMethod) {
		return nil, nil
	}

	standardMethodExt := proto.GetExtension(methodOpts, aippb.E_StandardMethod)
	standardMethod, ok := standardMethodExt.(*aippb.StandardMethod)
	if !ok || standardMethod == nil {
		return nil, fmt.Errorf("method %s has invalid standard_method annotation", method.Desc.Name())
	}

	resourceType := standardMethod.Resource
	if resourceType == "" {
		return nil, fmt.Errorf("method %s must define a resource type", method.Desc.Name())
	}

	message, err := GetMessageByResourceType(resourceType)
	if err != nil {
		return nil, fmt.Errorf("cannot find message type for resource %s: %w", resourceType, err)
	}

	parsedResource, err := ParseFromMessage(message)
	if err != nil {
		return nil, fmt.Errorf("parsing resource %s: %w", resourceType, err)
	}

	resourceNameSingular := parsedResource.SingularGoName()
	resourceNamePlural := parsedResource.PluralGoName()
	create := method.GoName == "Create"+resourceNameSingular
	get := method.GoName == "Get"+resourceNameSingular
	batchGet := method.GoName == "BatchGet"+resourceNamePlural
	update := method.GoName == "Update"+resourceNameSingular
	delete := method.GoName == "Delete"+resourceNameSingular
	list := method.GoName == "List"+resourceNamePlural
	if !(create || get || update || delete || batchGet || list) {
		return nil, fmt.Errorf("method %s does not match any standard CRUD pattern for resource %s", method.GoName, resourceType)
	}

	return &RPC{
		StandardMethod: standardMethod,
		Message:        message,
		ParsedResource: parsedResource,
		Create:         create,
		Get:            get,
		BatchGet:       batchGet,
		Update:         update,
		Delete:         delete,
		List:           list,
	}, nil
}
