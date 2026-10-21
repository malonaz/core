package schema_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"

	modelpb "github.com/malonaz/core/genproto/codegen/model/v1"
	"github.com/malonaz/core/tools/protoc-gen-core/resource"
	"github.com/malonaz/core/tools/protoc-gen-core/schema"
)

func lazySingleton(t *testing.T, messageName protoreflect.FullName, fields ...field) (bool, error) {
	t.Helper()
	message := compileMessage(t, messageName, fields...)
	parsedResource, err := resource.ParseFromMessage(message)
	require.NoError(t, err)
	return schema.LazySingleton(message, parsedResource)
}

func TestLazySingleton(t *testing.T) {
	const libraryConfig = "malonaz.test.library.v1.LibraryConfig"

	t.Run("SilentParent", func(t *testing.T) {
		lazy, err := lazySingleton(t, libraryConfig)
		require.NoError(t, err)
		require.True(t, lazy)
	})

	t.Run("PersistedParent", func(t *testing.T) {
		lazy, err := lazySingleton(t, "malonaz.test.library.v1.AuthorProfile")
		require.NoError(t, err)
		require.False(t, lazy)
	})

	t.Run("Collection", func(t *testing.T) {
		lazy, err := lazySingleton(t, "malonaz.test.library.v1.Author")
		require.NoError(t, err)
		require.False(t, lazy)
	})

	t.Run("DeleteTimeRejected", func(t *testing.T) {
		_, err := lazySingleton(t, libraryConfig,
			field{"delete_time", descriptorpb.FieldDescriptorProto_TYPE_MESSAGE, ".google.protobuf.Timestamp", &modelpb.FieldOpts{Nullable: true}, ""})
		require.ErrorContains(t, err, "cannot have a delete_time field")
	})

	t.Run("AggregateJoinRejected", func(t *testing.T) {
		_, err := lazySingleton(t, libraryConfig,
			field{"books", descriptorpb.FieldDescriptorProto_TYPE_INT64, "", aggregate(modelpb.Aggregate_FUNCTION_COUNT, "name", ""), ""})
		require.ErrorContains(t, err, "only reference joins")
	})
}
