package pbjson

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	_ "google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	_ "google.golang.org/protobuf/types/known/timestamppb"

	"github.com/malonaz/core/go/pbutil/pbfieldmask"
)

const readMaskTestFile = `
name: "read_mask_test.proto"
package: "readmask.test"
dependency: ["google/api/resource.proto", "google/protobuf/timestamp.proto"]
syntax: "proto3"
message_type {
  name: "Thing"
  options { [google.api.resource] { type: "test.com/Thing" pattern: "things/{thing}" } }
  field { name: "name" number: 1 type: TYPE_STRING label: LABEL_OPTIONAL }
  field { name: "create_time" number: 2 type: TYPE_MESSAGE type_name: ".google.protobuf.Timestamp" label: LABEL_OPTIONAL }
  field { name: "labels" number: 3 type: TYPE_MESSAGE type_name: ".readmask.test.Thing.LabelsEntry" label: LABEL_REPEATED }
  field { name: "metadata" number: 4 type: TYPE_MESSAGE type_name: ".readmask.test.ThingMetadata" label: LABEL_OPTIONAL }
  field { name: "parts" number: 5 type: TYPE_MESSAGE type_name: ".readmask.test.Part" label: LABEL_REPEATED }
  field { name: "wide" number: 6 type: TYPE_MESSAGE type_name: ".readmask.test.Wide" label: LABEL_OPTIONAL }
  nested_type {
    name: "LabelsEntry"
    options { map_entry: true }
    field { name: "key" number: 1 type: TYPE_STRING label: LABEL_OPTIONAL }
    field { name: "value" number: 2 type: TYPE_STRING label: LABEL_OPTIONAL }
  }
}
message_type {
  name: "ThingMetadata"
  field { name: "color" number: 1 type: TYPE_STRING label: LABEL_OPTIONAL }
  field { name: "update_time" number: 2 type: TYPE_MESSAGE type_name: ".google.protobuf.Timestamp" label: LABEL_OPTIONAL }
}
message_type {
  name: "Part"
  field { name: "title" number: 1 type: TYPE_STRING label: LABEL_OPTIONAL }
  field { name: "url" number: 2 type: TYPE_STRING label: LABEL_OPTIONAL }
}
message_type {
  name: "Wide"
  field { name: "f01" number: 1 type: TYPE_STRING label: LABEL_OPTIONAL }
  field { name: "f02" number: 2 type: TYPE_STRING label: LABEL_OPTIONAL }
  field { name: "f03" number: 3 type: TYPE_STRING label: LABEL_OPTIONAL }
  field { name: "f04" number: 4 type: TYPE_STRING label: LABEL_OPTIONAL }
  field { name: "f05" number: 5 type: TYPE_STRING label: LABEL_OPTIONAL }
  field { name: "f06" number: 6 type: TYPE_STRING label: LABEL_OPTIONAL }
  field { name: "f07" number: 7 type: TYPE_STRING label: LABEL_OPTIONAL }
  field { name: "f08" number: 8 type: TYPE_STRING label: LABEL_OPTIONAL }
  field { name: "f09" number: 9 type: TYPE_STRING label: LABEL_OPTIONAL }
  field { name: "f10" number: 10 type: TYPE_STRING label: LABEL_OPTIONAL }
  field { name: "f11" number: 11 type: TYPE_STRING label: LABEL_OPTIONAL }
  field { name: "f12" number: 12 type: TYPE_STRING label: LABEL_OPTIONAL }
  field { name: "f13" number: 13 type: TYPE_STRING label: LABEL_OPTIONAL }
}
message_type {
  name: "ListThingsResponse"
  field { name: "things" number: 1 type: TYPE_MESSAGE type_name: ".readmask.test.Thing" label: LABEL_REPEATED }
  field { name: "next_page_token" number: 2 type: TYPE_STRING label: LABEL_OPTIONAL }
}
message_type {
  name: "WrapThingResponse"
  field { name: "note" number: 1 type: TYPE_STRING label: LABEL_OPTIONAL }
  field { name: "thing" number: 2 type: TYPE_MESSAGE type_name: ".readmask.test.Thing" label: LABEL_OPTIONAL }
}
message_type {
  name: "SearchResponse"
  field { name: "results" number: 1 type: TYPE_MESSAGE type_name: ".readmask.test.Part" label: LABEL_REPEATED }
  field { name: "query" number: 2 type: TYPE_STRING label: LABEL_OPTIONAL }
}
message_type { name: "EmptyResponse" }
`

func readMaskTestMessage(t *testing.T, name protoreflect.Name) protoreflect.MessageDescriptor {
	t.Helper()
	fileProto := &descriptorpb.FileDescriptorProto{}
	require.NoError(t, prototext.Unmarshal([]byte(readMaskTestFile), fileProto))
	file, err := protodesc.NewFile(fileProto, protoregistry.GlobalFiles)
	require.NoError(t, err)
	message := file.Messages().ByName(name)
	require.NotNil(t, message)
	return message
}

const thingFields = " Fields: name, create_time, labels, metadata.{color,update_time}, parts.{title,url}, wide.{f01,f02,f03,f04,f05,f06,f07,f08,f09,f10,f11,f12,...}." +
	" (`f.{a,b}` stands for the paths f.a and f.b; f alone returns all of f.)"

func TestBuildResponseReadMaskDescription(t *testing.T) {
	tests := []struct {
		name     string
		response protoreflect.Name
		expected string
	}{
		{
			name:     "resource response",
			response: "Thing",
			expected: responseReadMaskBaseDescription + " Paths are relative to the Thing resource." + thingFields,
		},
		{
			name:     "repeated resource field",
			response: "ListThingsResponse",
			expected: responseReadMaskBaseDescription + " Paths are relative to each Thing resource in the response (not the response envelope)." + thingFields,
		},
		{
			name:     "singular resource field",
			response: "WrapThingResponse",
			expected: responseReadMaskBaseDescription + " Paths are relative to the Thing resource in the response (not the response envelope)." + thingFields,
		},
		{
			name:     "non-resource response",
			response: "SearchResponse",
			expected: responseReadMaskBaseDescription + " Paths are relative to the SearchResponse message. Fields: results.{title,url}, query." +
				" (`f.{a,b}` stands for the paths f.a and f.b; f alone returns all of f.)",
		},
		{
			name:     "message without fields",
			response: "EmptyResponse",
			expected: responseReadMaskBaseDescription + " Paths are relative to the EmptyResponse message.",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, buildResponseReadMaskDescription(readMaskTestMessage(t, tt.response)))
		})
	}

	t.Run("no response", func(t *testing.T) {
		require.Equal(t, responseReadMaskBaseDescription, buildResponseReadMaskDescription(nil))
	})
}

// Every listed path must pass the strict validation the read-mask middleware applies.
func TestDescribeReadMaskPathsAreValid(t *testing.T) {
	for _, name := range []protoreflect.Name{"Thing", "SearchResponse"} {
		message := readMaskTestMessage(t, name)
		listed, _ := describeReadMaskPaths(message)
		var paths []string
		for _, path := range strings.Split(listed, ", ") {
			field, subfields, nested := strings.Cut(path, ".{")
			paths = append(paths, field)
			if !nested {
				continue
			}
			for _, subfield := range strings.Split(strings.TrimSuffix(subfields, "}"), ",") {
				if subfield != "..." {
					paths = append(paths, field+"."+subfield)
				}
			}
		}
		require.NoError(t, pbfieldmask.FromPaths(paths...).Validate(dynamicpb.NewMessage(message)), name)
	}
}
