package pbcobra

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/huandu/xstrings"
	"github.com/spf13/cobra"
	"google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/malonaz/core/go/pbutil/pbreflection"
)

var (
	timestampFullName = (&timestamppb.Timestamp{}).ProtoReflect().Descriptor().FullName()
	durationFullName  = (&durationpb.Duration{}).ProtoReflect().Descriptor().FullName()
	fieldMaskFullName = (&fieldmaskpb.FieldMask{}).ProtoReflect().Descriptor().FullName()

	patternSegmentRegexp = regexp.MustCompile(`\{[^}]+\}`)
)

type ResponseHandler func(m proto.Message) error

type CommandBuilder struct {
	schema          *pbreflection.Schema
	invoker         *pbreflection.MethodInvoker
	responseHandler ResponseHandler
	maxDepth        int
}

func NewCommandBuilder(schema *pbreflection.Schema, invoker *pbreflection.MethodInvoker, responseHandler ResponseHandler) *CommandBuilder {
	return &CommandBuilder{
		schema:          schema,
		invoker:         invoker,
		responseHandler: responseHandler,
		maxDepth:        10,
	}
}

func (b *CommandBuilder) WithMaxDepth(maxDepth int) *CommandBuilder {
	b.maxDepth = maxDepth
	return b
}

func (b *CommandBuilder) Build() []*cobra.Command {
	var cmds []*cobra.Command
	b.schema.Services(func(svc protoreflect.ServiceDescriptor) bool {
		cmds = append(cmds, b.buildServiceCommand(svc))
		return true
	})
	return cmds
}

func (b *CommandBuilder) buildServiceCommand(svc protoreflect.ServiceDescriptor) *cobra.Command {
	cmd := &cobra.Command{
		Use:   xstrings.ToKebabCase(string(svc.Name())),
		Short: b.commentFor(string(svc.FullName()), commentFirstLine),
		Long:  b.commentFor(string(svc.FullName()), commentMultiline),
	}

	methods := svc.Methods()
	for i := 0; i < methods.Len(); i++ {
		cmd.AddCommand(b.buildMethodCommand(methods.Get(i)))
	}
	return cmd
}

func (b *CommandBuilder) buildMethodCommand(method protoreflect.MethodDescriptor) *cobra.Command {
	longDesc := b.commentFor(string(method.FullName()), commentMultiline)
	if responseDoc := b.formatResponseDoc(method.Output()); responseDoc != "" {
		if longDesc != "" {
			longDesc += "\n\n"
		}
		longDesc += responseDoc
	}

	cmd := &cobra.Command{
		Use:   xstrings.ToKebabCase(string(method.Name())),
		Short: b.commentFor(string(method.FullName()), commentFirstLine),
		Long:  longDesc,
		Args:  cobra.NoArgs,
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			return nil, cobra.ShellCompDirectiveNoFileComp
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return b.invokeMethod(cmd, method)
		},
	}

	methodName := string(method.Name())
	fields := method.Input().Fields()
	for i := 0; i < fields.Len(); i++ {
		b.addFlagWithPrefix(cmd, fields.Get(i), "", 0, methodName)
	}
	return cmd
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
		comment := b.commentFor(string(field.FullName()), commentSingleLine)

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

func (b *CommandBuilder) addFlagWithPrefix(cmd *cobra.Command, field protoreflect.FieldDescriptor, prefix string, depth int, methodName string) {
	if depth > b.maxDepth {
		return
	}

	behaviors := getFieldBehaviors(field)
	if behaviors.outputOnly {
		return
	}

	isCreate := strings.HasPrefix(methodName, "Create")
	isUpdate := strings.HasPrefix(methodName, "Update")

	if isCreate && behaviors.identifier {
		return
	}
	if isUpdate && behaviors.immutable {
		return
	}

	// Compute default value for parent fields with resource references
	var defaultValue string
	if field.Name() == "parent" && field.Kind() == protoreflect.StringKind {
		defaultValue = b.getParentDefault(field)
	}

	name := prefix + xstrings.ToKebabCase(string(field.Name()))
	help := b.commentFor(string(field.FullName()), commentSingleLine)

	isRequired := defaultValue == "" && (behaviors.required || (isUpdate && behaviors.identifier))
	if isRequired {
		help = "(required) " + help
	}

	if field.IsList() {
		cmd.Flags().StringSlice(name, nil, help)
		if isRequired {
			cmd.MarkFlagRequired(name)
		}
		return
	}

	switch field.Kind() {
	case protoreflect.MessageKind:
		switch field.Message().FullName() {
		case timestampFullName:
			cmd.Flags().String(name, "", help+" (Format: RFC3339, e.g. 2006-01-02T15:04:05Z)")
			if isRequired {
				cmd.MarkFlagRequired(name)
			}
			return
		case durationFullName:
			cmd.Flags().String(name, "", help+" (Format: Go duration, e.g. 1h30m)")
			if isRequired {
				cmd.MarkFlagRequired(name)
			}
			return
		case fieldMaskFullName:
			cmd.Flags().String(name, "", help+" (comma-separated paths)")
			if isRequired {
				cmd.MarkFlagRequired(name)
			}
			return

		default:
			nestedFields := field.Message().Fields()
			nestedPrefix := name + "-"
			for i := 0; i < nestedFields.Len(); i++ {
				b.addFlagWithPrefix(cmd, nestedFields.Get(i), nestedPrefix, depth+1, methodName)
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
		enumHelp := b.commentFor(string(field.Enum().FullName()), commentSingleLine)
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
}

type fieldBehaviors struct {
	required   bool
	outputOnly bool
	immutable  bool
	identifier bool
}

func getFieldBehaviors(field protoreflect.FieldDescriptor) fieldBehaviors {
	var fb fieldBehaviors
	opts := field.Options()
	if opts == nil {
		return fb
	}
	if !proto.HasExtension(opts, annotations.E_FieldBehavior) {
		return fb
	}
	behaviors := proto.GetExtension(opts, annotations.E_FieldBehavior).([]annotations.FieldBehavior)
	for _, b := range behaviors {
		switch b {
		case annotations.FieldBehavior_REQUIRED:
			fb.required = true
		case annotations.FieldBehavior_OUTPUT_ONLY:
			fb.outputOnly = true
		case annotations.FieldBehavior_IMMUTABLE:
			fb.immutable = true
		case annotations.FieldBehavior_IDENTIFIER:
			fb.identifier = true
		}
	}
	return fb
}

func (b *CommandBuilder) invokeMethod(cmd *cobra.Command, method protoreflect.MethodDescriptor) error {
	req := dynamicpb.NewMessage(method.Input())

	fields := method.Input().Fields()
	for i := 0; i < fields.Len(); i++ {
		if err := b.setFieldWithPrefix(req, fields.Get(i), cmd, ""); err != nil {
			return err
		}
	}

	resp, err := b.invoker.Invoke(cmd.Context(), method, req)
	if err != nil {
		return err
	}

	return b.responseHandler(resp)
}

func (b *CommandBuilder) setFieldWithPrefix(msg *dynamicpb.Message, field protoreflect.FieldDescriptor, cmd *cobra.Command, prefix string) error {
	name := prefix + xstrings.ToKebabCase(string(field.Name()))

	if field.IsList() {
		if !cmd.Flags().Changed(name) {
			return nil
		}
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
			if !b.anyNestedFlagChanged(cmd, field, name+"-") {
				return nil
			}
			nestedMsg := msg.Mutable(field).Message()
			nestedFields := field.Message().Fields()
			for i := 0; i < nestedFields.Len(); i++ {
				if err := b.setFieldWithPrefix(nestedMsg.(*dynamicpb.Message), nestedFields.Get(i), cmd, name+"-"); err != nil {
					return err
				}
			}
			return nil
		}
	}

	if !cmd.Flags().Changed(name) {
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

func (b *CommandBuilder) anyNestedFlagChanged(cmd *cobra.Command, field protoreflect.FieldDescriptor, prefix string) bool {
	if field.Kind() != protoreflect.MessageKind {
		return cmd.Flags().Changed(prefix[:len(prefix)-1])
	}
	nestedFields := field.Message().Fields()
	for i := 0; i < nestedFields.Len(); i++ {
		f := nestedFields.Get(i)
		name := prefix + xstrings.ToKebabCase(string(f.Name()))
		if f.Kind() == protoreflect.MessageKind {
			if b.anyNestedFlagChanged(cmd, f, name+"-") {
				return true
			}
		} else if cmd.Flags().Changed(name) {
			return true
		}
	}
	return false
}

func (b *CommandBuilder) setListField(msg *dynamicpb.Message, field protoreflect.FieldDescriptor, cmd *cobra.Command, name string) error {
	vals, err := cmd.Flags().GetStringSlice(name)
	if err != nil {
		return err
	}

	list := msg.Mutable(field).List()
	for _, v := range vals {
		if field.Kind() == protoreflect.MessageKind {
			nested := dynamicpb.NewMessage(field.Message())
			if err := protojson.Unmarshal([]byte(v), nested); err != nil {
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

func (b *CommandBuilder) getParentDefault(field protoreflect.FieldDescriptor) string {
	opts := field.Options()
	if opts == nil {
		return ""
	}
	if !proto.HasExtension(opts, annotations.E_ResourceReference) {
		return ""
	}
	ref := proto.GetExtension(opts, annotations.E_ResourceReference).(*annotations.ResourceReference)
	resourceType := ref.GetType()
	if resourceType == "" {
		return ""
	}
	res := b.schema.GetResource(resourceType)
	if res == nil || len(res.GetPattern()) == 0 {
		return ""
	}
	return patternSegmentRegexp.ReplaceAllString(res.GetPattern()[0], "-")
}

type commentStyle int

const (
	commentFirstLine commentStyle = iota
	commentMultiline
	commentSingleLine
)

func (b *CommandBuilder) commentFor(fullName string, style commentStyle) string {
	c, ok := b.schema.Comments[fullName]
	if !ok {
		return ""
	}
	switch style {
	case commentFirstLine:
		if idx := strings.Index(c, "\n"); idx != -1 {
			return c[:idx]
		}
		return c
	case commentMultiline:
		return c
	case commentSingleLine:
		return strings.ReplaceAll(c, "\n", " ")
	default:
		return c
	}
}
