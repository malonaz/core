package resource

import (
	"fmt"

	annotationspb "google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Registry holds all resource mappings scoped to a single proto package.
type Registry struct {
	MessageTypeToResourceDescriptor     map[string]*annotationspb.ResourceDescriptor
	ResourceTypeToResourceDescriptor    map[string]*annotationspb.ResourceDescriptor
	ResourcePatternToResourceDescriptor map[string]*annotationspb.ResourceDescriptor
	ResourceTypeToChildResourceTypeSet  map[string]map[string]struct{}
	ResourceTypeToMessage               map[string]*protogen.Message
	ParsedResourceTypeToParsedResource  map[string]*ParsedResource
}

func NewRegistry() *Registry {
	return &Registry{
		MessageTypeToResourceDescriptor:     map[string]*annotationspb.ResourceDescriptor{},
		ResourceTypeToResourceDescriptor:    map[string]*annotationspb.ResourceDescriptor{},
		ResourcePatternToResourceDescriptor: map[string]*annotationspb.ResourceDescriptor{},
		ResourceTypeToChildResourceTypeSet:  map[string]map[string]struct{}{},
		ResourceTypeToMessage:               map[string]*protogen.Message{},
		ParsedResourceTypeToParsedResource:  map[string]*ParsedResource{},
	}
}

// PackageToRegistry maps each proto package to its isolated registry.
var PackageToRegistry = map[protoreflect.FullName]*Registry{}

func GetOrCreateRegistry(pkg protoreflect.FullName) *Registry {
	if reg, ok := PackageToRegistry[pkg]; ok {
		return reg
	}
	reg := NewRegistry()
	PackageToRegistry[pkg] = reg
	return reg
}

// RegisterAnnotations scans all files and populates per-package registries with
// resource descriptors from both file-level resource_definition annotations and
// message-level resource annotations.
func RegisterAnnotations(files []*protogen.File) error {
	type patternOrigin struct {
		descriptor *annotationspb.ResourceDescriptor
		filePath   string
	}
	packagePatternToOrigin := map[protoreflect.FullName]map[string]patternOrigin{}

	registerPattern := func(pkg protoreflect.FullName, pattern string, rd *annotationspb.ResourceDescriptor, filePath string) error {
		origins, ok := packagePatternToOrigin[pkg]
		if !ok {
			origins = map[string]patternOrigin{}
			packagePatternToOrigin[pkg] = origins
		}
		if existing, ok := origins[pattern]; ok {
			if !proto.Equal(existing.descriptor, rd) {
				return fmt.Errorf(
					"pattern %q already registered by resource %q (from %s), cannot register again for resource %q (from %s)",
					pattern, existing.descriptor.Type, existing.filePath, rd.Type, filePath,
				)
			}
		}
		origins[pattern] = patternOrigin{descriptor: rd, filePath: filePath}
		GetOrCreateRegistry(pkg).ResourcePatternToResourceDescriptor[pattern] = rd
		return nil
	}

	for _, f := range files {
		pkg := f.Desc.Package()
		reg := GetOrCreateRegistry(pkg)
		filePath := string(f.Desc.Path())

		fileOpts := f.Desc.Options()
		if fileOpts != nil && proto.HasExtension(fileOpts, annotationspb.E_ResourceDefinition) {
			ext := proto.GetExtension(fileOpts, annotationspb.E_ResourceDefinition)
			resourceDefinitions := ext.([]*annotationspb.ResourceDescriptor)
			for _, rd := range resourceDefinitions {
				if rd != nil && rd.Type != "" {
					reg.ResourceTypeToResourceDescriptor[rd.Type] = rd
				}
				for _, pattern := range rd.Pattern {
					if err := registerPattern(pkg, pattern, rd, filePath); err != nil {
						return err
					}
				}
			}
		}

		for _, message := range f.Messages {
			messageType := string(message.Desc.FullName())
			msgOpts := message.Desc.Options()
			if msgOpts == nil {
				continue
			}
			if !proto.HasExtension(msgOpts, annotationspb.E_Resource) {
				continue
			}
			ext := proto.GetExtension(msgOpts, annotationspb.E_Resource)
			rd, ok := ext.(*annotationspb.ResourceDescriptor)
			if !ok || rd == nil || rd.Type == "" {
				continue
			}
			for _, pattern := range rd.Pattern {
				if err := registerPattern(pkg, pattern, rd, filePath); err != nil {
					return err
				}
			}
			reg.ResourceTypeToResourceDescriptor[rd.Type] = rd
			reg.MessageTypeToResourceDescriptor[messageType] = rd
			reg.ResourceTypeToMessage[rd.Type] = message
		}
	}
	return nil
}

// RegisterAncestors links every resource to its immediate parent within each
// proto package by matching each of its patterns against the registered prefix
// patterns. Every pattern contributes a parent-child link, so multi-pattern
// resources are registered as children of all of their parents.
//
// Unresolvable prefixes are tolerated here: this pass runs over every resource
// in the package, including ones no generator consumes. Parse reports missing
// sub-pattern descriptors for the resources that are actually used.
func RegisterAncestors(files []*protogen.File) error {
	registeredPackages := map[protoreflect.FullName]bool{}
	for _, f := range files {
		pkg := f.Desc.Package()
		if registeredPackages[pkg] {
			continue
		}
		registeredPackages[pkg] = true
		GetOrCreateRegistry(pkg).registerAncestors()
	}
	return nil
}

func (reg *Registry) registerAncestors() {
	for resourceType, rd := range reg.ResourceTypeToResourceDescriptor {
		for _, pattern := range rd.Pattern {
			_, variableEnds, singleton, err := scanPattern(reg, pattern)
			if err != nil {
				continue
			}
			parentEnd, hasParent := parentPatternEnd(variableEnds, singleton)
			if !hasParent {
				continue
			}
			parentDescriptor, ok := reg.ResourcePatternToResourceDescriptor[pattern[:parentEnd]]
			if !ok {
				continue
			}
			if reg.ResourceTypeToChildResourceTypeSet[parentDescriptor.Type] == nil {
				reg.ResourceTypeToChildResourceTypeSet[parentDescriptor.Type] = map[string]struct{}{}
			}
			reg.ResourceTypeToChildResourceTypeSet[parentDescriptor.Type][resourceType] = struct{}{}
		}
	}
}
