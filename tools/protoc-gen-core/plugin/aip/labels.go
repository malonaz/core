package aip

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/huandu/xstrings"
	"google.golang.org/protobuf/compiler/protogen"

	aippb "github.com/malonaz/core/genproto/aip/v1"
	codegenaippb "github.com/malonaz/core/genproto/codegen/aip/v1"
	"github.com/malonaz/core/go/pbutil"
)

var varPattern = regexp.MustCompile(`\{([^}]+)\}`)

type labelInfo struct {
	label     *aippb.Label
	goName    string
	isDynamic bool
	vars      []string
	key       string
}

// generateLabels emits the `Labels` set: one typed key (with its allowed values) per
// label the file declares.
func generateLabels(file *protogen.File, g *protogen.GeneratedFile) (bool, error) {
	labels, err := pbutil.GetExtension[[]*aippb.Label](file.Desc.Options(), codegenaippb.E_Label)
	if err != nil && !errors.Is(err, pbutil.ErrExtensionNotFound) {
		return false, fmt.Errorf("getting label extension: %w", err)
	}
	if len(labels) == 0 {
		return false, nil
	}
	infos, err := parseLabels(labels)
	if err != nil {
		return false, err
	}
	g.P()
	for _, info := range infos {
		generateLabelStruct(g, info)
	}
	g.P("type LabelSet struct {")
	for _, info := range infos {
		g.P(info.goName, " Label", info.goName)
	}
	g.P("}")
	g.P()
	g.P("var Labels = LabelSet{")
	for _, info := range infos {
		generateLabelValue(g, info)
	}
	g.P("}")
	return true, nil
}

func parseLabels(labels []*aippb.Label) ([]*labelInfo, error) {
	var infos []*labelInfo
	for _, label := range labels {
		info := &labelInfo{label: label}
		key := label.GetKey()
		dynamicKey := label.GetDynamicKey()
		info.isDynamic = dynamicKey != ""

		if info.isDynamic {
			if key != "" {
				return nil, fmt.Errorf("label: key and dynamic_key are mutually exclusive, got key=%q dynamic_key=%q", key, dynamicKey)
			}
			matches := varPattern.FindAllString(dynamicKey, -1)
			if len(matches) == 0 {
				return nil, fmt.Errorf("label %q: dynamic_key must contain at least one {variable} placeholder", dynamicKey)
			}
			info.vars = matches
			info.key = dynamicKey

			pathPart := dynamicKey
			if idx := strings.LastIndex(dynamicKey, "/"); idx >= 0 {
				pathPart = dynamicKey[idx+1:]
			}
			pathPart = strings.NewReplacer("{", "", "}", "", "-", "_", ".", "_").Replace(pathPart)
			info.goName = xstrings.ToPascalCase(pathPart)
		} else {
			info.key = key
			goName := key
			if idx := strings.LastIndex(key, "/"); idx >= 0 {
				goName = key[idx+1:]
			}
			goName = strings.NewReplacer("-", "_", ".", "_").Replace(goName)
			info.goName = xstrings.ToPascalCase(goName)
		}

		infos = append(infos, info)
	}
	return infos, nil
}

func generateLabelStruct(g *protogen.GeneratedFile, info *labelInfo) {
	g.P("type Label", info.goName, " struct {")
	g.P("GetKey func(", funcParams(info.vars), ") string")
	for _, value := range info.label.GetValues() {
		g.P(valueName(value), " string")
	}
	g.P("}")
	g.P()
}

func generateLabelValue(g *protogen.GeneratedFile, info *labelInfo) {
	g.P(info.goName, ": Label", info.goName, "{")

	g.P("GetKey: func(", funcParams(info.vars), ") string {")
	if info.isDynamic {
		expr := info.key
		for _, v := range info.vars {
			replacement := fmt.Sprintf(`" + _%s + "`, varParamName(v))
			expr = strings.Replace(expr, v, replacement, 1)
		}
		g.P(fmt.Sprintf(`return "%s"`, expr))
	} else {
		g.P(fmt.Sprintf(`return "%s"`, info.key))
	}
	g.P("},")

	for _, value := range info.label.GetValues() {
		g.P(fmt.Sprintf(`%s: "%s",`, valueName(value), value))
	}
	g.P("},")
}

func funcParams(vars []string) string {
	params := make([]string, len(vars))
	for i, v := range vars {
		params[i] = fmt.Sprintf("_%s string", varParamName(v))
	}
	return strings.Join(params, ", ")
}

func varParamName(v string) string {
	name := strings.TrimPrefix(v, "{")
	name = strings.TrimSuffix(name, "}")
	name = strings.ReplaceAll(name, "-", "_")
	return xstrings.ToCamelCase(name)
}

func valueName(v string) string {
	return xstrings.ToPascalCase(strings.ReplaceAll(v, "-", "_"))
}
