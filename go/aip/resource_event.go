package aip

import (
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	aippb "github.com/malonaz/core/genproto/aip/v1"
)

// NewResourceCreatedEvent constructs a ResourceCreatedEvent by marshaling the given resource into an Any.
func NewResourceCreatedEvent[R proto.Message](resource R) (*aippb.ResourceCreatedEvent, error) {
	resourceAny, err := anypb.New(resource)
	if err != nil {
		return nil, fmt.Errorf("marshaling %s to Any: %w", resource.ProtoReflect().Descriptor().FullName(), err)
	}
	return &aippb.ResourceCreatedEvent{
		Resource: resourceAny,
	}, nil
}

// NewResourceUpdatedEvent constructs a ResourceUpdatedEvent by marshaling the current and old resource
// into Any fields, along with the update mask describing which fields were modified.
func NewResourceUpdatedEvent[R proto.Message](resource, oldResource R, updateMask *fieldmaskpb.FieldMask) (*aippb.ResourceUpdatedEvent, error) {
	fullName := resource.ProtoReflect().Descriptor().FullName()
	resourceAny, err := anypb.New(resource)
	if err != nil {
		return nil, fmt.Errorf("marshaling %s to Any: %w", fullName, err)
	}
	oldResourceAny, err := anypb.New(oldResource)
	if err != nil {
		return nil, fmt.Errorf("marshaling old %s to Any: %w", fullName, err)
	}
	return &aippb.ResourceUpdatedEvent{
		Resource:    resourceAny,
		OldResource: oldResourceAny,
		UpdateMask:  updateMask,
	}, nil
}

// NewResourceDeletedEvent constructs a ResourceDeletedEvent by marshaling the given resource into an Any.
func NewResourceDeletedEvent[R proto.Message](resource R) (*aippb.ResourceDeletedEvent, error) {
	resourceAny, err := anypb.New(resource)
	if err != nil {
		return nil, fmt.Errorf("marshaling %s to Any: %w", resource.ProtoReflect().Descriptor().FullName(), err)
	}
	return &aippb.ResourceDeletedEvent{
		Resource: resourceAny,
	}, nil
}

// ParseResourceCreatedEvent extracts the resource from a ResourceCreatedEvent by unmarshaling the Any
// into a new instance of R.
func ParseResourceCreatedEvent[R proto.Message](event *aippb.ResourceCreatedEvent) (R, error) {
	var zero R
	resource := zero.ProtoReflect().New().Interface().(R)
	if err := anypb.UnmarshalTo(event.GetResource(), resource, proto.UnmarshalOptions{}); err != nil {
		return zero, fmt.Errorf("unmarshaling %s from Any: %w", resource.ProtoReflect().Descriptor().FullName(), err)
	}
	return resource, nil
}

// ParseResourceUpdatedEvent extracts the current resource, old resource, and update mask from a
// ResourceUpdatedEvent by unmarshaling the Any fields into new instances of R.
func ParseResourceUpdatedEvent[R proto.Message](event *aippb.ResourceUpdatedEvent) (R, R, *fieldmaskpb.FieldMask, error) {
	var zero R
	resource := zero.ProtoReflect().New().Interface().(R)
	fullName := resource.ProtoReflect().Descriptor().FullName()
	if err := anypb.UnmarshalTo(event.GetResource(), resource, proto.UnmarshalOptions{}); err != nil {
		return zero, zero, nil, fmt.Errorf("unmarshaling %s from Any: %w", fullName, err)
	}
	oldResource := zero.ProtoReflect().New().Interface().(R)
	if err := anypb.UnmarshalTo(event.GetOldResource(), oldResource, proto.UnmarshalOptions{}); err != nil {
		return zero, zero, nil, fmt.Errorf("unmarshaling old %s from Any: %w", fullName, err)
	}
	return resource, oldResource, event.GetUpdateMask(), nil
}

// ParseResourceDeletedEvent extracts the resource from a ResourceDeletedEvent by unmarshaling the Any
// into a new instance of R.
func ParseResourceDeletedEvent[R proto.Message](event *aippb.ResourceDeletedEvent) (R, error) {
	var zero R
	resource := zero.ProtoReflect().New().Interface().(R)
	if err := anypb.UnmarshalTo(event.GetResource(), resource, proto.UnmarshalOptions{}); err != nil {
		return zero, fmt.Errorf("unmarshaling %s from Any: %w", resource.ProtoReflect().Descriptor().FullName(), err)
	}
	return resource, nil
}
