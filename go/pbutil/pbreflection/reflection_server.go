package pbreflection

import (
	"fmt"

	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
)

func NewTypesFromFiles(files *protoregistry.Files) (*protoregistry.Types, error) {
	types := new(protoregistry.Types)
	var registrationErr error
	files.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		if err := registerTypes(types, fd); err != nil {
			registrationErr = fmt.Errorf("registering types from %q: %v", fd.Path(), err)
			return false
		}
		return true
	})
	if registrationErr != nil {
		return nil, registrationErr
	}
	return types, nil
}

func registerTypes(types *protoregistry.Types, fd protoreflect.FileDescriptor) error {
	for i := 0; i < fd.Messages().Len(); i++ {
		if err := registerMessageTypes(types, fd.Messages().Get(i)); err != nil {
			return err
		}
	}
	for i := 0; i < fd.Extensions().Len(); i++ {
		if err := types.RegisterExtension(dynamicpb.NewExtensionType(fd.Extensions().Get(i))); err != nil {
			return fmt.Errorf("registering extension %q: %v", fd.Extensions().Get(i).FullName(), err)
		}
	}
	return nil
}

func registerMessageTypes(types *protoregistry.Types, md protoreflect.MessageDescriptor) error {
	if err := types.RegisterMessage(dynamicpb.NewMessageType(md)); err != nil {
		return fmt.Errorf("registering message %q: %v", md.FullName(), err)
	}
	for i := 0; i < md.Messages().Len(); i++ {
		if err := registerMessageTypes(types, md.Messages().Get(i)); err != nil {
			return err
		}
	}
	for i := 0; i < md.Extensions().Len(); i++ {
		if err := types.RegisterExtension(dynamicpb.NewExtensionType(md.Extensions().Get(i))); err != nil {
			return fmt.Errorf("registering extension %q: %v", md.Extensions().Get(i).FullName(), err)
		}
	}
	for i := 0; i < md.Enums().Len(); i++ {
		if err := types.RegisterEnum(dynamicpb.NewEnumType(md.Enums().Get(i))); err != nil {
			return fmt.Errorf("registering enum %q: %v", md.Enums().Get(i).FullName(), err)
		}
	}
	return nil
}
