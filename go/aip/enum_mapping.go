package aip

import (
	"fmt"

	"google.golang.org/protobuf/reflect/protoreflect"

	aippb "github.com/malonaz/core/genproto/aip/v1"
	"github.com/malonaz/core/go/pbutil"
)

type enumMapping struct {
	toExternal   map[int32]string
	fromExternal map[string]int32
}

type EnumMapping[E interface {
	~int32
	Descriptor() protoreflect.EnumDescriptor
}] struct {
	mapping *enumMapping
}

func NewEnumMapping[E interface {
	~int32
	protoreflect.Enum
	Descriptor() protoreflect.EnumDescriptor
}]() *EnumMapping[E] {
	var zero E
	desc := zero.Descriptor()
	fullName := string(desc.FullName())

	mapping := &enumMapping{
		toExternal:   make(map[int32]string),
		fromExternal: make(map[string]int32),
	}

	values := desc.Values()
	for i := 0; i < values.Len(); i++ {
		valueDesc := values.Get(i)
		enumVal := int32(valueDesc.Number())
		if enumVal == 0 {
			continue
		}
		externals, err := pbutil.GetExtension[[]string](valueDesc.Options(), aippb.E_External)
		if err != nil || len(externals) == 0 {
			panic(fmt.Sprintf("enum %s value %s (%d) has no (malonaz.aip.v1.external) annotation", fullName, valueDesc.Name(), enumVal))
		}
		mapping.toExternal[enumVal] = externals[0]
		for _, ext := range externals {
			if existing, ok := mapping.fromExternal[ext]; ok {
				panic(fmt.Sprintf("enum %s: duplicate external string %q mapped to both %d and %d", fullName, ext, existing, enumVal))
			}
			mapping.fromExternal[ext] = enumVal
		}
	}

	return &EnumMapping[E]{mapping: mapping}
}

func (m *EnumMapping[E]) ToExternal(e E) (string, error) {
	if s, ok := m.mapping.toExternal[int32(e)]; ok {
		return s, nil
	}
	return "", fmt.Errorf("no external mapping for enum value %d", e)
}

func (m *EnumMapping[E]) FromExternal(s string) (E, error) {
	var zero E
	if v, ok := m.mapping.fromExternal[s]; ok {
		return E(v), nil
	}
	return zero, fmt.Errorf("no enum value for external string %q", s)
}

func (m *EnumMapping[E]) SliceToExternal(enums []E) ([]string, error) {
	result := make([]string, len(enums))
	for i, e := range enums {
		s, ok := m.mapping.toExternal[int32(e)]
		if !ok {
			return nil, fmt.Errorf("no external mapping for enum value %d", e)
		}
		result[i] = s
	}
	return result, nil
}

func (m *EnumMapping[E]) AllExternal() []string {
	result := make([]string, 0, len(m.mapping.toExternal))
	for _, s := range m.mapping.toExternal {
		result = append(result, s)
	}
	return result
}
