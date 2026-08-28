package middleware

import (
  "context"
  "fmt"
  "log/slog"
  "maps"
  "slices"
  "sync"

  validatepb "buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"
  "golang.org/x/sync/errgroup"
  "google.golang.org/genproto/googleapis/api/annotations"
  "google.golang.org/grpc"
  "google.golang.org/grpc/codes"
  "google.golang.org/grpc/status"
  "google.golang.org/protobuf/proto"
  "google.golang.org/protobuf/reflect/protoreflect"
  "google.golang.org/protobuf/types/known/anypb"

  "github.com/malonaz/core/go/pbutil"
)

const (
  relatedRequestFieldName  = protoreflect.Name("include_related")
  relatedResponseFieldName = protoreflect.Name("related")
  relatedWildcard          = "*"
  anyFullName              = protoreflect.FullName("google.protobuf.Any")
)

// RelatedResolver resolves resource names, all of a single resource type, into their resources.
type RelatedResolver func(ctx context.Context, names []string) ([]proto.Message, error)

type relatedRegistry struct {
  mu                     sync.RWMutex
  resourceTypeToResolver map[string]RelatedResolver
}

func (r *relatedRegistry) hasResolvers() bool {
  r.mu.RLock()
  defer r.mu.RUnlock()
  return len(r.resourceTypeToResolver) > 0
}

var globalRelatedRegistry = &relatedRegistry{
  resourceTypeToResolver: map[string]RelatedResolver{},
}

// RegisterRelatedResolver registers a custom resolver for the given resource type
// (e.g. "library.test.malonaz.com/Author"). Typically called during server setup.
// Prefer RegisterBatchGetRelatedResolver for standard AIP-231 BatchGet methods.
func RegisterRelatedResolver(resourceType string, resolver RelatedResolver) error {
  globalRelatedRegistry.mu.Lock()
  defer globalRelatedRegistry.mu.Unlock()
  if _, ok := globalRelatedRegistry.resourceTypeToResolver[resourceType]; ok {
    return fmt.Errorf("related resolver already registered for resource type %q", resourceType)
  }
  globalRelatedRegistry.resourceTypeToResolver[resourceType] = resolver
  return nil
}

// RegisterBatchGetRelatedResolver adapts a generated AIP-231 BatchGetX client method into a
// RelatedResolver and registers it. The resource type is derived from the request's `names`
// field (google.api.resource_reference).type annotation, which prevents registering a method
// under the wrong type. The batch size is derived from the field's buf.validate max_items rule;
// registration fails if it is unset, since chunking would otherwise be unbounded.
func RegisterBatchGetRelatedResolver[RequestT, ResponseT proto.Message](
  batchGet func(ctx context.Context, request RequestT, opts ...grpc.CallOption) (ResponseT, error),
) error {
  var zero RequestT
  requestDescriptor := zero.ProtoReflect().Descriptor()
  namesField := requestDescriptor.Fields().ByName("names")
  if namesField == nil || !namesField.IsList() || namesField.Kind() != protoreflect.StringKind {
    return fmt.Errorf("request %s has no repeated string `names` field", requestDescriptor.FullName())
  }
  resourceType, ok := resourceReferenceType(namesField)
  if !ok {
    return fmt.Errorf("request %s `names` field has no (google.api.resource_reference).type annotation", requestDescriptor.FullName())
  }
  maxItems, err := batchGetMaxItems(namesField)
  if err != nil {
    return fmt.Errorf("request %s: %w", requestDescriptor.FullName(), err)
  }

  resolver := func(ctx context.Context, names []string) ([]proto.Message, error) {
    var resources []proto.Message
    for chunk := range slices.Chunk(names, maxItems) {
      var zero RequestT
      reflectRequest := zero.ProtoReflect().New()
      namesList := reflectRequest.Mutable(namesField).List()
      for _, name := range chunk {
        namesList.Append(protoreflect.ValueOfString(name))
      }
      batchGetRequest := reflectRequest.Interface().(RequestT)
      batchGetResponse, err := batchGet(ctx, batchGetRequest)
      if err != nil {
        return nil, err
      }
      chunkResources, err := extractBatchResources(batchGetResponse)
      if err != nil {
        return nil, err
      }
      resources = append(resources, chunkResources...)
    }
    return resources, nil
  }
  return RegisterRelatedResolver(resourceType, resolver)
}

// batchGetMaxItems reads the max batch size from the `names` field's buf.validate rules.
func batchGetMaxItems(namesField protoreflect.FieldDescriptor) (int, error) {
  fieldRules, err := pbutil.GetExtension[*validatepb.FieldRules](namesField.Options(), validatepb.E_Field)
  if err != nil {
    return 0, fmt.Errorf("`names` field has no buf.validate rules: %w", err)
  }
  maxItems := fieldRules.GetRepeated().GetMaxItems()
  if maxItems == 0 {
    return 0, fmt.Errorf("`names` field has no buf.validate repeated.max_items rule")
  }
  return int(maxItems), nil
}

func extractBatchResources(response proto.Message) ([]proto.Message, error) {
  reflectResponse := response.ProtoReflect()
  fields := reflectResponse.Descriptor().Fields()
  for i := 0; i < fields.Len(); i++ {
    field := fields.Get(i)
    if !field.IsList() || field.Kind() != protoreflect.MessageKind {
      continue
    }
    if !hasResourceAnnotation(field.Message()) {
      continue
    }
    list := reflectResponse.Get(field).List()
    resources := make([]proto.Message, 0, list.Len())
    for j := 0; j < list.Len(); j++ {
      resources = append(resources, list.Get(j).Message().Interface())
    }
    return resources, nil
  }
  return nil, fmt.Errorf("response %s has no repeated resource field", reflectResponse.Descriptor().FullName())
}

// UnaryServerRelated resolves resource references in responses into their `related` field,
// as requested via the request's `include_related` field.
//
// Resolution happens at exactly one layer: a server with no registered resolvers (e.g. a
// pass-through gateway) forwards the request untouched, while the first server with resolvers
// consumes the `include_related` field before invoking the handler so that downstream services
// do not resolve a second time.
func UnaryServerRelated() grpc.UnaryServerInterceptor {
  return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
    requestMessage, ok := req.(proto.Message)
    if !ok {
      return handler(ctx, req)
    }
    requestedResourceTypes := extractIncludeRelated(requestMessage)
    if len(requestedResourceTypes) == 0 {
      return handler(ctx, req)
    }
    // A server with no resolvers proxies the request untouched; the first downstream
    // server with resolvers owns resolution.
    if !globalRelatedRegistry.hasResolvers() {
      return handler(ctx, req)
    }
    // Validate requested types upfront so typos fail fast, before doing any work.
    resourceTypeToResolver, err := resolveRequestedResolvers(requestedResourceTypes)
    if err != nil {
      return nil, err
    }
    // Consume the field so downstream services don't resolve a second time.
    clearIncludeRelated(requestMessage)

    response, err := handler(ctx, req)
    if err != nil {
      return nil, err
    }
    responseMessage, ok := response.(proto.Message)
    if !ok {
      return response, nil
    }
    reflectResponse := responseMessage.ProtoReflect()
    relatedField := reflectResponse.Descriptor().Fields().ByName(relatedResponseFieldName)
    if relatedField == nil || !relatedField.IsList() ||
      relatedField.Kind() != protoreflect.MessageKind ||
      relatedField.Message().FullName() != anyFullName {
      return nil, status.Errorf(codes.Internal, "response %s has no repeated google.protobuf.Any `related` field", reflectResponse.Descriptor().FullName())
    }

    resourceTypeToNameSet := map[string]map[string]struct{}{}
    collectResourceReferences(reflectResponse, resourceTypeToResolver, resourceTypeToNameSet)
    if len(resourceTypeToNameSet) == 0 {
      return response, nil
    }

    // Resolve each resource type concurrently. Sorted types and names keep output deterministic.
    resourceTypes := slices.Sorted(maps.Keys(resourceTypeToNameSet))
    resolvedResources := make([][]proto.Message, len(resourceTypes))
    eg, ctxEg := errgroup.WithContext(ctx)
    for i, resourceType := range resourceTypes {
      eg.Go(func() error {
        names := slices.Sorted(maps.Keys(resourceTypeToNameSet[resourceType]))
        resources, err := resourceTypeToResolver[resourceType](ctxEg, names)
        if err != nil {
          // Dangling references are expected; resolution is best-effort on NotFound.
          if status.Code(err) == codes.NotFound {
            slog.DebugContext(ctxEg, "skipping related resources", "resource_type", resourceType, "error", err)
            return nil
          }
          return status.Errorf(codes.Internal, "resolving related %s resources: %v", resourceType, err)
        }
        resolvedResources[i] = resources
        return nil
      })
    }
    if err := eg.Wait(); err != nil {
      return nil, err
    }

    relatedList := reflectResponse.Mutable(relatedField).List()
    for _, resources := range resolvedResources {
      for _, resource := range resources {
        anyResource, err := anypb.New(resource)
        if err != nil {
          return nil, status.Errorf(codes.Internal, "packing related resource: %v", err)
        }
        relatedList.Append(protoreflect.ValueOfMessage(anyResource.ProtoReflect()))
      }
    }
    return response, nil
  }
}

func extractIncludeRelated(request proto.Message) []string {
  reflectRequest := request.ProtoReflect()
  field := reflectRequest.Descriptor().Fields().ByName(relatedRequestFieldName)
  if field == nil || !field.IsList() || field.Kind() != protoreflect.StringKind {
    return nil
  }
  list := reflectRequest.Get(field).List()
  requestedResourceTypes := make([]string, 0, list.Len())
  for i := 0; i < list.Len(); i++ {
    requestedResourceTypes = append(requestedResourceTypes, list.Get(i).String())
  }
  return requestedResourceTypes
}

func clearIncludeRelated(request proto.Message) {
  reflectRequest := request.ProtoReflect()
  field := reflectRequest.Descriptor().Fields().ByName(relatedRequestFieldName)
  if field != nil {
    reflectRequest.Clear(field)
  }
}

func resolveRequestedResolvers(requestedResourceTypes []string) (map[string]RelatedResolver, error) {
  globalRelatedRegistry.mu.RLock()
  defer globalRelatedRegistry.mu.RUnlock()
  resourceTypeToResolver := map[string]RelatedResolver{}
  for _, resourceType := range requestedResourceTypes {
    if resourceType == relatedWildcard {
      maps.Copy(resourceTypeToResolver, globalRelatedRegistry.resourceTypeToResolver)
      continue
    }
    resolver, ok := globalRelatedRegistry.resourceTypeToResolver[resourceType]
    if !ok {
      return nil, status.Errorf(codes.InvalidArgument, "no related resolver registered for resource type %q", resourceType)
    }
    resourceTypeToResolver[resourceType] = resolver
  }
  return resourceTypeToResolver, nil
}

func collectResourceReferences(
  message protoreflect.Message,
  resourceTypeToResolver map[string]RelatedResolver,
  resourceTypeToNameSet map[string]map[string]struct{},
) {
  fields := message.Descriptor().Fields()
  for i := 0; i < fields.Len(); i++ {
    field := fields.Get(i)
    switch {
    case field.IsMap():
      if field.MapValue().Kind() != protoreflect.MessageKind {
        continue
      }
      message.Get(field).Map().Range(func(_ protoreflect.MapKey, value protoreflect.Value) bool {
        collectResourceReferences(value.Message(), resourceTypeToResolver, resourceTypeToNameSet)
        return true
      })
    case field.IsList():
      switch field.Kind() {
      case protoreflect.MessageKind:
        // Skip Any fields (notably `related` itself) to avoid re-resolving packed resources.
        if field.Message().FullName() == anyFullName {
          continue
        }
        list := message.Get(field).List()
        for j := 0; j < list.Len(); j++ {
          collectResourceReferences(list.Get(j).Message(), resourceTypeToResolver, resourceTypeToNameSet)
        }
      case protoreflect.StringKind:
        resourceType, ok := resourceReferenceType(field)
        if !ok {
          continue
        }
        if _, ok := resourceTypeToResolver[resourceType]; !ok {
          continue
        }
        list := message.Get(field).List()
        for j := 0; j < list.Len(); j++ {
          addResourceName(resourceTypeToNameSet, resourceType, list.Get(j).String())
        }
      }
    case field.Kind() == protoreflect.MessageKind:
      if field.Message().FullName() == anyFullName {
        continue
      }
      if message.Has(field) {
        collectResourceReferences(message.Get(field).Message(), resourceTypeToResolver, resourceTypeToNameSet)
      }
    case field.Kind() == protoreflect.StringKind:
      resourceType, ok := resourceReferenceType(field)
      if !ok {
        continue
      }
      if _, ok := resourceTypeToResolver[resourceType]; !ok {
        continue
      }
      addResourceName(resourceTypeToNameSet, resourceType, message.Get(field).String())
    }
  }
}

func resourceReferenceType(field protoreflect.FieldDescriptor) (string, bool) {
  reference, err := pbutil.GetExtension[*annotations.ResourceReference](field.Options(), annotations.E_ResourceReference)
  if err != nil || reference.GetType() == "" {
    return "", false
  }
  return reference.GetType(), true
}

func addResourceName(resourceTypeToNameSet map[string]map[string]struct{}, resourceType, name string) {
  if name == "" {
    return
  }
  nameSet, ok := resourceTypeToNameSet[resourceType]
  if !ok {
    nameSet = map[string]struct{}{}
    resourceTypeToNameSet[resourceType] = nameSet
  }
  nameSet[name] = struct{}{}
}
