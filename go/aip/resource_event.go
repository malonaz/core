package aip

import (
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	aippb "github.com/malonaz/core/genproto/aip/v1"
	"github.com/malonaz/core/go/pbutil"
)

// NewResourceCreatedEvent constructs a ResourceCreatedEvent by marshaling the given resource into an Any.
func NewResourceCreatedEvent[R proto.Message](resource R) (*aippb.ResourceEvent, error) {
	resourceAny, err := anypb.New(resource)
	if err != nil {
		return nil, fmt.Errorf("marshaling %s to Any: %w", resource.ProtoReflect().Descriptor().FullName(), err)
	}
	return &aippb.ResourceEvent{
		Type:     aippb.ResourceEventType_RESOURCE_EVENT_TYPE_CREATED,
		Resource: resourceAny,
	}, nil
}

// NewResourceUpdatedEvent constructs a ResourceUpdatedEvent by marshaling the current and previous resource
// into Any fields, along with the update mask describing which fields were modified.
func NewResourceUpdatedEvent[R proto.Message](resource, previousResource R, updateMask *fieldmaskpb.FieldMask) (*aippb.ResourceEvent, error) {
	fullName := resource.ProtoReflect().Descriptor().FullName()
	resourceAny, err := anypb.New(resource)
	if err != nil {
		return nil, fmt.Errorf("marshaling %s to Any: %w", fullName, err)
	}
	previousResourceAny, err := anypb.New(previousResource)
	if err != nil {
		return nil, fmt.Errorf("marshaling previous %s to Any: %w", fullName, err)
	}
	return &aippb.ResourceEvent{
		Type:             aippb.ResourceEventType_RESOURCE_EVENT_TYPE_UPDATED,
		Resource:         resourceAny,
		PreviousResource: previousResourceAny,
		UpdateMask:       updateMask,
	}, nil
}

// NewResourceDeletedEvent constructs a ResourceDeletedEvent by marshaling the given resource into an Any.
func NewResourceDeletedEvent[R proto.Message](resource R) (*aippb.ResourceEvent, error) {
	resourceAny, err := anypb.New(resource)
	if err != nil {
		return nil, fmt.Errorf("marshaling %s to Any: %w", resource.ProtoReflect().Descriptor().FullName(), err)
	}
	return &aippb.ResourceEvent{
		Type:     aippb.ResourceEventType_RESOURCE_EVENT_TYPE_DELETED,
		Resource: resourceAny,
	}, nil
}

// ParseEventResource extracts and unmarshals the resource from a ResourceEvent.
func ParseEventResource[R proto.Message](event *aippb.ResourceEvent) (R, error) {
	return pbutil.ParseAny[R](event.GetResource())
}

// ParseEventPreviousResource extracts and unmarshals the previous resource from a ResourceEvent.
// Returns an error if the event type is not RESOURCE_EVENT_TYPE_UPDATED.
func ParseEventPreviousResource[R proto.Message](event *aippb.ResourceEvent) (R, error) {
	var zero R
	if event.GetType() != aippb.ResourceEventType_RESOURCE_EVENT_TYPE_UPDATED {
		return zero, fmt.Errorf("expected updated event, got %s", event.GetType())
	}
	return pbutil.ParseAny[R](event.GetPreviousResource())
}
