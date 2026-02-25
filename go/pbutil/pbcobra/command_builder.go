package pbcobra

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/huandu/xstrings"
	"github.com/spf13/cobra"
	"google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/malonaz/core/go/pbutil"
	"github.com/malonaz/core/go/pbutil/pbreflection"
)

const (
	defaultMaxDepth = 10

	annotationKeyService = "pbcobra.service"
	annotationKeyMethod  = "pbcobra.method"
)

var (
	timestampFullName = (&timestamppb.Timestamp{}).ProtoReflect().Descriptor().FullName()
	durationFullName  = (&durationpb.Duration{}).ProtoReflect().Descriptor().FullName()
	fieldMaskFullName = (&fieldmaskpb.FieldMask{}).ProtoReflect().Descriptor().FullName()

	patternSegmentRegexp = regexp.MustCompile(`\{[^}]+\}`)

	defaultResponseHandler = func(m proto.Message) error {
		bytes, err := pbutil.JSONMarshalPretty(m)
		if err != nil {
			return err
		}
		fmt.Println(string(bytes))
		return nil
	}

	defaultErrorHandler = func(err error) error {
		return err
	}
)

type ResponseHandler func(proto.Message) error
type ErrorHandler func(error) error

type CommandBuilder struct {
	schema          *pbreflection.Schema
	invoker         *pbreflection.MethodInvoker
	responseHandler ResponseHandler
	errorHandler    ErrorHandler
	maxDepth        int
}

func NewCommandBuilder(schema *pbreflection.Schema, invoker *pbreflection.MethodInvoker) *CommandBuilder {
	return &CommandBuilder{
		schema:          schema,
		invoker:         invoker,
		maxDepth:        defaultMaxDepth,
		responseHandler: defaultResponseHandler,
		errorHandler:    defaultErrorHandler,
	}
}

func (b *CommandBuilder) WithResponseHandler(responseHandler ResponseHandler) *CommandBuilder {
	b.responseHandler = responseHandler
	return b
}

func (b *CommandBuilder) WithErrorHandler(errorHandler ErrorHandler) *CommandBuilder {
	b.errorHandler = errorHandler
	return b
}

func (b *CommandBuilder) WithMaxDepth(maxDepth int) *CommandBuilder {
	b.maxDepth = maxDepth
	return b
}

func (b *CommandBuilder) Build() ([]*cobra.Command, error) {
	var cmds []*cobra.Command
	var errServices error
	b.schema.Services(func(serviceDescriptor protoreflect.ServiceDescriptor) bool {
		cmd, err := b.buildServiceCommand(serviceDescriptor)
		if err != nil {
			errServices = fmt.Errorf("build command for service %q: %w", serviceDescriptor.FullName(), err)
			return false
		}
		cmds = append(cmds, cmd)
		return true
	})
	return cmds, errServices
}

func (b *CommandBuilder) buildServiceCommand(serviceDescriptor protoreflect.ServiceDescriptor) (*cobra.Command, error) {
	cmd := &cobra.Command{
		Use:   xstrings.ToKebabCase(string(serviceDescriptor.Name())),
		Short: b.schema.GetComment(serviceDescriptor.FullName(), pbreflection.CommentStyleFirstLine),
		Long:  b.schema.GetComment(serviceDescriptor.FullName(), pbreflection.CommentStyleMultiline),
		Annotations: map[string]string{
			annotationKeyService: string(serviceDescriptor.FullName()),
		},
	}

	methods := serviceDescriptor.Methods()
	for i := 0; i < methods.Len(); i++ {
		method := methods.Get(i)
		methodCmd, err := b.buildMethodCommand(method)
		if err != nil {
			return nil, fmt.Errorf("build command for method %q: %w", method.FullName, err)
		}
		methodCmd.Annotations[annotationKeyService] = string(serviceDescriptor.FullName())
		cmd.AddCommand(methodCmd)
	}
	return cmd, nil
}

func (b *CommandBuilder) buildMethodCommand(methodDescriptor protoreflect.MethodDescriptor) (*cobra.Command, error) {
	// Get standard method type.
	standardMethodType, err := b.schema.GetStandardMethodType(methodDescriptor.FullName())
	if err != nil {
		return nil, fmt.Errorf("getting standard method type %q: %v", methodDescriptor.FullName(), err)
	}

	longDesc := b.schema.GetComment(methodDescriptor.FullName(), pbreflection.CommentStyleMultiline)
	if responseDoc := b.formatResponseDoc(methodDescriptor.Output()); responseDoc != "" {
		if longDesc != "" {
			longDesc += "\n\n"
		}
		longDesc += responseDoc
	}

	cmd := &cobra.Command{
		Use:   xstrings.ToKebabCase(string(methodDescriptor.Name())),
		Short: b.schema.GetComment(methodDescriptor.FullName(), pbreflection.CommentStyleFirstLine),
		Long:  longDesc,
		Args:  cobra.NoArgs,
		Annotations: map[string]string{
			annotationKeyMethod: string(methodDescriptor.FullName()),
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			return nil, cobra.ShellCompDirectiveNoFileComp
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			err := b.invokeMethod(cmd, methodDescriptor)
			if err != nil {
				return b.errorHandler(err)
			}
			return nil
		},
	}

	methodName := string(methodDescriptor.Name())
	fields := methodDescriptor.Input().Fields()
	for i := 0; i < fields.Len(); i++ {
		b.addFlagWithPrefix(cmd, standardMethodType, fields.Get(i), "", 0, methodName)
	}
	return cmd, nil
}

func (b *CommandBuilder) formatResponseDoc(msg protoreflect.MessageDescriptor) string {
	var sb strings.Builder
	sb.WriteString("Response:\n")
	b.formatMessageFields(&sb, msg, "  ", 0)
	return sb.String()
}

func (b *CommandBuilder) formatMessageFields(sb *strings.Builder, msg protoreflect.MessageDescriptor, indent string, depth int) {
	if depth > b.maxDepth {
		return
	}
	fields := msg.Fields()
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		fieldName := string(field.Name())
		fieldType := b.fieldTypeName(field)
		comment := b.schema.GetComment(field.FullName(), pbreflection.CommentStyleSingleLine)

		if comment != "" {
			fmt.Fprintf(sb, "%s%s (%s): %s\n", indent, fieldName, fieldType, comment)
		} else {
			fmt.Fprintf(sb, "%s%s (%s)\n", indent, fieldName, fieldType)
		}

		if field.Kind() == protoreflect.MessageKind && !field.IsList() && !field.IsMap() {
			switch field.Message().FullName() {
			case timestampFullName, durationFullName, fieldMaskFullName:
			default:
				b.formatMessageFields(sb, field.Message(), indent+"  ", depth+1)
			}
		}
	}
}

func (b *CommandBuilder) fieldTypeName(field protoreflect.FieldDescriptor) string {
	var typeName string
	switch field.Kind() {
	case protoreflect.MessageKind:
		typeName = string(field.Message().Name())
	case protoreflect.EnumKind:
		typeName = string(field.Enum().Name())
	default:
		typeName = field.Kind().String()
	}
	if field.IsList() {
		return "[]" + typeName
	}
	if field.IsMap() {
		return "map"
	}
	return typeName
}

// In command_builder.go, modify addFlagWithPrefix to set default for parent fields:
func (b *CommandBuilder) addFlagWithPrefix(
	cmd *cobra.Command,
	standardMethodType pbreflection.StandardMethodType,
	fieldDescriptor protoreflect.FieldDescriptor,
	prefix string,
	depth int,
	methodName string,
) error {
	// Depth check.
	if depth > b.maxDepth {
		return nil
	}

	// Grab the field behavior.
	fieldBehavior, err := pbutil.GetFieldBehavior(fieldDescriptor)
	if err != nil {
		return fmt.Errorf("getting field behavior: %w", err)
	}

	var isRequired bool
	switch standardMethodType {
	case pbreflection.StandardMethodTypeCreate:
		if fieldBehavior.Identifier {
			return nil
		}
		isRequired = fieldBehavior.Required
	case pbreflection.StandardMethodTypeUpdate:
		if fieldBehavior.Immutable {
			return nil
		}
		isRequired = fieldBehavior.Identifier
	default:
		isRequired = fieldBehavior.Required
	}

	// Compute default value for parent fields with resource references
	var defaultValue string
	if fieldDescriptor.Name() == "parent" && fieldDescriptor.Kind() == protoreflect.StringKind {
		var err error
		defaultValue, err = b.getParentDefault(fieldDescriptor)
		if err != nil {
			return fmt.Errorf("getting parent default for %q: %w", fieldDescriptor.Name(), err)
		}
	}
	if defaultValue != "" {
		isRequired = false
	}

	name := prefix + xstrings.ToKebabCase(string(fieldDescriptor.Name()))
	help := b.schema.GetComment(fieldDescriptor.FullName(), pbreflection.CommentStyleSingleLine)

	if isRequired {
		help = "(required) " + help
	}

	if fieldDescriptor.IsList() {
		cmd.Flags().StringArray(name, nil, help)
		if isRequired {
			cmd.MarkFlagRequired(name)
		}
		return nil
	}

	switch fieldDescriptor.Kind() {
	case protoreflect.MessageKind:
		switch fieldDescriptor.Message().FullName() {
		case timestampFullName:
			cmd.Flags().String(name, "", help+" (Format: RFC3339, e.g. 2006-01-02T15:04:05Z)")
			if isRequired {
				cmd.MarkFlagRequired(name)
			}
			return nil
		case durationFullName:
			cmd.Flags().String(name, "", help+" (Format: Go duration, e.g. 1h30m)")
			if isRequired {
				cmd.MarkFlagRequired(name)
			}
			return nil
		case fieldMaskFullName:
			cmd.Flags().String(name, "", help+" (comma-separated paths)")
			if isRequired {
				cmd.MarkFlagRequired(name)
			}
			return nil

		default:
			// Add flag to explicitly instantiate this message
			cmd.Flags().Bool(name, false, help)
			if isRequired {
				cmd.MarkFlagRequired(name)
			}

			nestedFields := fieldDescriptor.Message().Fields()
			nestedPrefix := name + "-"
			for i := 0; i < nestedFields.Len(); i++ {
				b.addFlagWithPrefix(cmd, standardMethodType, nestedFields.Get(i), nestedPrefix, depth+1, methodName)
			}
		}

	case protoreflect.StringKind:
		cmd.Flags().String(name, defaultValue, help)
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		cmd.Flags().Int32(name, 0, help)
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		cmd.Flags().Int64(name, 0, help)
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		cmd.Flags().Uint32(name, 0, help)
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		cmd.Flags().Uint64(name, 0, help)
	case protoreflect.BoolKind:
		cmd.Flags().Bool(name, false, help)
	case protoreflect.FloatKind:
		cmd.Flags().Float32(name, 0, help)
	case protoreflect.DoubleKind:
		cmd.Flags().Float64(name, 0, help)
	case protoreflect.BytesKind:
		cmd.Flags().String(name, "", help)
	case protoreflect.EnumKind:
		enumHelp := b.schema.GetComment(fieldDescriptor.Enum().FullName(), pbreflection.CommentStyleSingleLine)
		if help != "" && enumHelp != "" {
			help = help + " (" + enumHelp + ")"
		} else if enumHelp != "" {
			help = enumHelp
		}
		cmd.Flags().String(name, "", help)
	}

	if isRequired {
		cmd.MarkFlagRequired(name)
	}
	return nil
}

func (b *CommandBuilder) invokeMethod(cmd *cobra.Command, method protoreflect.MethodDescriptor) error {
	req := dynamicpb.NewMessage(method.Input())

	fields := method.Input().Fields()
	for i := 0; i < fields.Len(); i++ {
		if err := b.setFieldWithPrefix(req, fields.Get(i), cmd, "", 0); err != nil {
			return err
		}
	}

	resp, err := b.invoker.Invoke(cmd.Context(), method, req)
	if err != nil {
		return err
	}

	return b.responseHandler(resp)
}

func (b *CommandBuilder) setFieldWithPrefix(msg *dynamicpb.Message, field protoreflect.FieldDescriptor, cmd *cobra.Command, prefix string, depth int) error {
	if depth > b.maxDepth {
		return nil
	}
	name := prefix + xstrings.ToKebabCase(string(field.Name()))

	if field.IsList() {
		return b.setListField(msg, field, cmd, name)
	}

	if field.Kind() == protoreflect.MessageKind {
		switch field.Message().FullName() {
		case timestampFullName:
			if !cmd.Flags().Changed(name) {
				return nil
			}
			str, err := cmd.Flags().GetString(name)
			if err != nil {
				return err
			}
			t, err := time.Parse(time.RFC3339, str)
			if err != nil {
				return fmt.Errorf("parsing %s as timestamp: %w", name, err)
			}
			ts := timestamppb.New(t)
			nested := msg.Mutable(field).Message()
			nested.Set(nested.Descriptor().Fields().ByName("seconds"), protoreflect.ValueOfInt64(ts.Seconds))
			nested.Set(nested.Descriptor().Fields().ByName("nanos"), protoreflect.ValueOfInt32(ts.Nanos))
			return nil

		case durationFullName:
			if !cmd.Flags().Changed(name) {
				return nil
			}
			str, err := cmd.Flags().GetString(name)
			if err != nil {
				return err
			}
			d, err := time.ParseDuration(str)
			if err != nil {
				return fmt.Errorf("parsing %s as duration: %w", name, err)
			}
			dur := durationpb.New(d)
			nested := msg.Mutable(field).Message()
			nested.Set(nested.Descriptor().Fields().ByName("seconds"), protoreflect.ValueOfInt64(dur.Seconds))
			nested.Set(nested.Descriptor().Fields().ByName("nanos"), protoreflect.ValueOfInt32(dur.Nanos))
			return nil

		case fieldMaskFullName:
			if !cmd.Flags().Changed(name) {
				return nil
			}
			str, err := cmd.Flags().GetString(name)
			if err != nil {
				return err
			}
			var paths []string
			if str != "" {
				paths = strings.Split(str, ",")
			}
			fm := &fieldmaskpb.FieldMask{Paths: paths}
			fm.Normalize()
			nested := msg.Mutable(field).Message()
			pathsList := nested.Mutable(nested.Descriptor().Fields().ByName("paths")).List()
			for _, p := range fm.Paths {
				pathsList.Append(protoreflect.ValueOfString(strings.TrimSpace(p)))
			}
			return nil

		default:
			// Check if explicitly instantiated via bool flag or has nested fields set
			explicitlySet, _ := cmd.Flags().GetBool(name)
			hasNestedChanges := b.anyNestedFlagChanged(cmd, field, name+"-", depth)

			if !explicitlySet && !hasNestedChanges {
				return nil
			}

			nestedMsg := msg.Mutable(field).Message()
			nestedFields := field.Message().Fields()
			for i := 0; i < nestedFields.Len(); i++ {
				if err := b.setFieldWithPrefix(nestedMsg.(*dynamicpb.Message), nestedFields.Get(i), cmd, name+"-", depth+1); err != nil {
					return err
				}
			}
			return nil
		}
	}

	if !cmd.Flags().Changed(name) {
		if field.Kind() == protoreflect.StringKind {
			if v, _ := cmd.Flags().GetString(name); v != "" {
				msg.Set(field, protoreflect.ValueOfString(v))
			}
		}
		return nil
	}

	val, err := b.getScalarValueByName(field, cmd, name)
	if err != nil {
		return err
	}
	if val.IsValid() {
		msg.Set(field, val)
	}
	return nil
}

func (b *CommandBuilder) anyNestedFlagChanged(cmd *cobra.Command, field protoreflect.FieldDescriptor, prefix string, depth int) bool {
	if depth > b.maxDepth {
		return false
	}
	if field.Kind() != protoreflect.MessageKind {
		return cmd.Flags().Changed(prefix[:len(prefix)-1])
	}
	nestedFields := field.Message().Fields()
	for i := 0; i < nestedFields.Len(); i++ {
		f := nestedFields.Get(i)
		name := prefix + xstrings.ToKebabCase(string(f.Name()))
		if f.Kind() == protoreflect.MessageKind {
			if b.anyNestedFlagChanged(cmd, f, name+"-", depth+1) {
				return true
			}
		} else if cmd.Flags().Changed(name) {
			return true
		}
	}
	return false
}

func (b *CommandBuilder) setListField(msg *dynamicpb.Message, field protoreflect.FieldDescriptor, cmd *cobra.Command, name string) error {
	vals, err := cmd.Flags().GetStringArray(name)
	if err != nil {
		return err
	}

	list := msg.Mutable(field).List()
	for _, v := range vals {
		if field.Kind() == protoreflect.MessageKind {
			nested := dynamicpb.NewMessage(field.Message())
			if err := pbutil.JSONUnmarshal([]byte(v), nested); err != nil {
				return fmt.Errorf("parsing %s: %w", name, err)
			}
			list.Append(protoreflect.ValueOfMessage(nested))
		} else {
			elem, err := parseScalar(field, v)
			if err != nil {
				return err
			}
			list.Append(elem)
		}
	}
	return nil
}

func (b *CommandBuilder) getScalarValueByName(field protoreflect.FieldDescriptor, cmd *cobra.Command, name string) (protoreflect.Value, error) {
	flags := cmd.Flags()

	switch field.Kind() {
	case protoreflect.StringKind:
		v, err := flags.GetString(name)
		return protoreflect.ValueOfString(v), err

	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		v, err := flags.GetInt32(name)
		return protoreflect.ValueOfInt32(v), err

	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		v, err := flags.GetInt64(name)
		return protoreflect.ValueOfInt64(v), err

	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		v, err := flags.GetUint32(name)
		return protoreflect.ValueOfUint32(v), err

	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		v, err := flags.GetUint64(name)
		return protoreflect.ValueOfUint64(v), err

	case protoreflect.BoolKind:
		v, err := flags.GetBool(name)
		return protoreflect.ValueOfBool(v), err

	case protoreflect.FloatKind:
		v, err := flags.GetFloat32(name)
		return protoreflect.ValueOfFloat32(v), err

	case protoreflect.DoubleKind:
		v, err := flags.GetFloat64(name)
		return protoreflect.ValueOfFloat64(v), err

	case protoreflect.BytesKind:
		v, err := flags.GetString(name)
		return protoreflect.ValueOfBytes([]byte(v)), err

	case protoreflect.EnumKind:
		v, err := flags.GetString(name)
		if err != nil {
			return protoreflect.Value{}, err
		}
		enumVal := field.Enum().Values().ByName(protoreflect.Name(v))
		if enumVal == nil {
			return protoreflect.Value{}, fmt.Errorf("unknown enum value: %s", v)
		}
		return protoreflect.ValueOfEnum(enumVal.Number()), nil
	}

	return protoreflect.Value{}, nil
}

func parseScalar(field protoreflect.FieldDescriptor, s string) (protoreflect.Value, error) {
	switch field.Kind() {
	case protoreflect.StringKind:
		return protoreflect.ValueOfString(s), nil
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		var v int32
		_, err := fmt.Sscanf(s, "%d", &v)
		return protoreflect.ValueOfInt32(v), err
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		var v int64
		_, err := fmt.Sscanf(s, "%d", &v)
		return protoreflect.ValueOfInt64(v), err
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		var v uint32
		_, err := fmt.Sscanf(s, "%d", &v)
		return protoreflect.ValueOfUint32(v), err
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		var v uint64
		_, err := fmt.Sscanf(s, "%d", &v)
		return protoreflect.ValueOfUint64(v), err
	case protoreflect.BoolKind:
		return protoreflect.ValueOfBool(s == "true"), nil
	case protoreflect.FloatKind:
		var v float32
		_, err := fmt.Sscanf(s, "%f", &v)
		return protoreflect.ValueOfFloat32(v), err
	case protoreflect.DoubleKind:
		var v float64
		_, err := fmt.Sscanf(s, "%f", &v)
		return protoreflect.ValueOfFloat64(v), err
	default:
		return protoreflect.Value{}, fmt.Errorf("unsupported list element type: %v", field.Kind())
	}
}

func (b *CommandBuilder) getParentDefault(fieldDescriptor protoreflect.FieldDescriptor) (string, error) {
	resourceReference, err := pbutil.GetExtension[*annotations.ResourceReference](fieldDescriptor.Options(), annotations.E_ResourceReference)
	if err != nil {
		return "", fmt.Errorf("getting resource reference: %w", err)
	}
	resourceType := resourceReference.GetType()
	if resourceType == "" {
		return "", fmt.Errorf("resource reference has no type")
	}
	resourceDescriptor, ok := b.schema.GetResourceDescriptor(resourceType)
	if !ok {
		return "", fmt.Errorf("resource descriptor not found for %q", resourceType)
	}
	if len(resourceDescriptor.GetPattern()) == 0 {
		return "", fmt.Errorf("resource descriptor %q has no patterns", resourceType)
	}
	return patternSegmentRegexp.ReplaceAllString(resourceDescriptor.GetPattern()[0], "-"), nil
}
