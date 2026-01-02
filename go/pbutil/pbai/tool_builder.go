package pbai

import (
	"fmt"

	"google.golang.org/protobuf/reflect/protoreflect"

	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/pbutil/pbreflection"
)

func toolName(svcName, methodName protoreflect.Name) string {
	return string(string(svcName) + "_" + string(methodName))
}

func (m *ToolManager) buildMethodTool(method protoreflect.MethodDescriptor) (*aipb.Tool, error) {
	svc := method.Parent().(protoreflect.ServiceDescriptor)
	description := m.schema.GetComment(method.FullName(), pbreflection.CommentStyleMultiline)

	standardMethodType := m.schema.GetStandardMethodType(method.FullName())
	schema, err := m.schemaBuilder.BuildSchema(method.Input().FullName(), standardMethodType)
	if err != nil {
		return nil, fmt.Errorf("building schema for: %v", method.Input().FullName())
	}

	return &aipb.Tool{
		Name:        toolName(svc.Name(), method.Name()),
		Description: description,
		JsonSchema:  schema,
		Annotations: map[string]string{
			annotationKeyToolType:   annotationValueToolTypeMethod,
			annotationKeyGRPCMethod: string(method.FullName()),
		},
	}, nil
}
