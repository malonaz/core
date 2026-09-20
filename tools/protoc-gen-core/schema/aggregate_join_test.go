package schema_test

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	annotationspb "google.golang.org/genproto/googleapis/api/annotations"
	decimalpb "google.golang.org/genproto/googleapis/type/decimal"
	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/pluginpb"

	modelpb "github.com/malonaz/core/genproto/codegen/model/v1"
	librarypb "github.com/malonaz/core/genproto/test/library/v1"
	"github.com/malonaz/core/tools/protoc-gen-core/resource"
	"github.com/malonaz/core/tools/protoc-gen-core/schema"
)

const bookType = "library.test.malonaz.com/Book"

// field declares an extra output-only field appended to a library resource:
// the one under test, or the one named by on.
type field struct {
	name     string
	typ      descriptorpb.FieldDescriptorProto_Type
	typeName string
	opts     *modelpb.FieldOpts
	on       protoreflect.FullName
}

func aggregate(function modelpb.Aggregate_Function, field, filter string) *modelpb.FieldOpts {
	return &modelpb.FieldOpts{Nullable: true, Join: &modelpb.Join{
		ResourceType: bookType,
		Field:        field,
		Selector:     &modelpb.Join_Aggregate{Aggregate: &modelpb.Aggregate{Function: function, Filter: filter}},
	}}
}

// parseJoins compiles the library protos with the given fields appended to
// the named message and resolves that message's joins, as generation would.
func parseJoins(t *testing.T, messageName protoreflect.FullName, fields ...field) error {
	t.Helper()
	files := map[string]*descriptorpb.FileDescriptorProto{}
	var order []string
	var collect func(fd protoreflect.FileDescriptor)
	collect = func(fd protoreflect.FileDescriptor) {
		if _, ok := files[fd.Path()]; ok {
			return
		}
		imports := fd.Imports()
		for i := 0; i < imports.Len(); i++ {
			collect(imports.Get(i).FileDescriptor)
		}
		files[fd.Path()] = protodesc.ToFileDescriptorProto(fd)
		order = append(order, fd.Path())
	}
	// Appended Decimal fields need google/type/decimal.proto in the unit
	// and imported by the file they land in.
	collect(decimalpb.File_google_type_decimal_proto)
	// The whole package is generated together: resources resolve against
	// their package's registry, and protogen only surfaces generated files
	// and their imports.
	request := &pluginpb.CodeGeneratorRequest{}
	protoregistry.GlobalFiles.RangeFilesByPackage(librarypb.File_malonaz_test_library_v1_shelf_proto.Package(), func(fd protoreflect.FileDescriptor) bool {
		collect(fd)
		request.FileToGenerate = append(request.FileToGenerate, fd.Path())
		return true
	})

	messageProto := func(name protoreflect.FullName) *descriptorpb.DescriptorProto {
		descriptor, err := protoregistry.GlobalFiles.FindDescriptorByName(name)
		require.NoError(t, err)
		file := files[descriptor.ParentFile().Path()]
		if decimalPath := decimalpb.File_google_type_decimal_proto.Path(); !slices.Contains(file.Dependency, decimalPath) {
			file.Dependency = append(file.Dependency, decimalPath)
		}
		for _, candidate := range file.MessageType {
			if candidate.GetName() == string(name.Name()) {
				return candidate
			}
		}
		t.Fatalf("message %s not found", name)
		return nil
	}
	for i, f := range fields {
		on := f.on
		if on == "" {
			on = messageName
		}
		options := &descriptorpb.FieldOptions{}
		if f.opts != nil {
			proto.SetExtension(options, modelpb.E_FieldOpts, f.opts)
		}
		proto.SetExtension(options, annotationspb.E_FieldBehavior, []annotationspb.FieldBehavior{annotationspb.FieldBehavior_OUTPUT_ONLY})
		mutated := messageProto(on)
		mutated.Field = append(mutated.Field, &descriptorpb.FieldDescriptorProto{
			Name:     proto.String(f.name),
			JsonName: proto.String(f.name),
			Number:   proto.Int32(int32(1000 + i)),
			Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
			Type:     f.typ.Enum(),
			TypeName: proto.String(f.typeName),
			Options:  options,
		})
	}

	for _, path := range order {
		request.ProtoFile = append(request.ProtoFile, files[path])
	}
	plugin, err := protogen.Options{}.New(request)
	require.NoError(t, err)

	// The resource registry is process-wide: start each compilation clean.
	resource.PackageToRegistry = map[protoreflect.FullName]*resource.Registry{}
	require.NoError(t, resource.RegisterAnnotations(plugin.Files))
	require.NoError(t, resource.RegisterAncestors(plugin.Files))
	registry := new(protoregistry.Files)
	for _, f := range plugin.Files {
		require.NoError(t, registry.RegisterFile(f.Desc))
	}
	schema.SetFilesRegistry(registry)

	for _, f := range plugin.Files {
		for _, m := range f.Messages {
			if m.Desc.FullName() == messageName {
				_, err := schema.ParseJoins(m)
				return err
			}
		}
	}
	t.Fatalf("message %s not found", messageName)
	return nil
}

func TestAggregateJoin_Generation(t *testing.T) {
	const shelf, book = "malonaz.test.library.v1.Shelf", "malonaz.test.library.v1.Book"
	int64Type := descriptorpb.FieldDescriptorProto_TYPE_INT64
	int32Type := descriptorpb.FieldDescriptorProto_TYPE_INT32
	stringType := descriptorpb.FieldDescriptorProto_TYPE_STRING
	messageType := descriptorpb.FieldDescriptorProto_TYPE_MESSAGE

	t.Run("Valid", func(t *testing.T) {
		require.NoError(t, parseJoins(t, shelf,
			field{"pages", int64Type, "", aggregate(modelpb.Aggregate_FUNCTION_SUM, "page_count", "page_count > 0"), ""},
			field{"books", int64Type, "", aggregate(modelpb.Aggregate_FUNCTION_COUNT, "name", ""), ""},
			field{"last", messageType, ".google.protobuf.Timestamp", aggregate(modelpb.Aggregate_FUNCTION_MAX, "create_time", ""), ""},
		))
	})

	t.Run("NonNullableTarget", func(t *testing.T) {
		opts := aggregate(modelpb.Aggregate_FUNCTION_SUM, "page_count", "")
		opts.Nullable = false
		err := parseJoins(t, shelf, field{"pages", int64Type, "", opts, ""})
		require.ErrorContains(t, err, `field "pages" must be nullable`)
	})

	t.Run("Int32TargetOnIntegerSum", func(t *testing.T) {
		err := parseJoins(t, shelf, field{"pages", int32Type, "", aggregate(modelpb.Aggregate_FUNCTION_SUM, "page_count", ""), ""})
		require.ErrorContains(t, err, "yields int64, not int32")
	})

	t.Run("CountMustNameName", func(t *testing.T) {
		err := parseJoins(t, shelf, field{"books", int64Type, "", aggregate(modelpb.Aggregate_FUNCTION_COUNT, "page_count", ""), ""})
		require.ErrorContains(t, err, `COUNT must aggregate "name"`)
	})

	t.Run("MaxKeepsSourceType", func(t *testing.T) {
		err := parseJoins(t, shelf, field{"last", int64Type, "", aggregate(modelpb.Aggregate_FUNCTION_MAX, "create_time", ""), ""})
		require.ErrorContains(t, err, "keeps its type google.protobuf.Timestamp, not int64")
	})

	t.Run("MinOverBool", func(t *testing.T) {
		boolType := descriptorpb.FieldDescriptorProto_TYPE_BOOL
		err := parseJoins(t, shelf,
			field{name: "signed", typ: boolType, on: book},
			field{name: "all_signed", typ: boolType, opts: aggregate(modelpb.Aggregate_FUNCTION_MIN, "signed", "")},
		)
		require.ErrorContains(t, err, "MIN over bool")
	})

	t.Run("SumOverDecimal", func(t *testing.T) {
		cost := field{name: "cost", typ: messageType, typeName: ".google.type.Decimal", on: book}
		require.NoError(t, parseJoins(t, shelf, cost,
			field{"total_cost", messageType, ".google.type.Decimal", aggregate(modelpb.Aggregate_FUNCTION_SUM, "cost", ""), ""}))
		err := parseJoins(t, shelf, cost,
			field{"total_cost", int64Type, "", aggregate(modelpb.Aggregate_FUNCTION_SUM, "cost", ""), ""})
		require.ErrorContains(t, err, "yields google.type.Decimal, not int64")
	})

	t.Run("ChainingOntoAggregate", func(t *testing.T) {
		chained := &modelpb.FieldOpts{Nullable: true, Join: &modelpb.Join{
			ResourceType: bookType,
			Field:        "title",
			Selector:     &modelpb.Join_Reference{Reference: "pages"},
		}}
		err := parseJoins(t, shelf,
			field{"pages", int64Type, "", aggregate(modelpb.Aggregate_FUNCTION_SUM, "page_count", ""), ""},
			field{"pages_title", stringType, "", chained, ""},
		)
		require.ErrorContains(t, err, `field "pages" is an aggregate join, which nothing can chain onto`)
	})

	t.Run("OverAncestor", func(t *testing.T) {
		opts := aggregate(modelpb.Aggregate_FUNCTION_COUNT, "name", "")
		opts.Join.ResourceType = "library.test.malonaz.com/Shelf"
		err := parseJoins(t, book, field{"shelves", int64Type, "", opts, ""})
		require.ErrorContains(t, err, "does not extend")
	})

	t.Run("BadFilter", func(t *testing.T) {
		err := parseJoins(t, shelf, field{"pages", int64Type, "", aggregate(modelpb.Aggregate_FUNCTION_SUM, "page_count", "no_such_field > 0"), ""})
		require.ErrorContains(t, err, "aggregate join filter")
	})
}
