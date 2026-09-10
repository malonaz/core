package gen

import (
	"strings"

	"github.com/huandu/xstrings"

	"github.com/malonaz/core/tools/onyx/manifest"
)

// PB imports the generated package of a gRPC service as `{service}pb`.
func PB(f *File, svc *manifest.GRPCService) string {
	return f.ImportAs(svc.GoImportPath, strings.ToLower(svc.GoName)+"pb")
}

// Pascal converts a kebab or snake case name: `scheduler-service` -> `SchedulerService`.
func Pascal(name string) string {
	return xstrings.ToPascalCase(name)
}

// Camel converts a kebab or snake case name: `scheduler-service` -> `schedulerService`.
func Camel(name string) string {
	return xstrings.FirstRuneToLower(Pascal(name))
}

// Kebab converts a name: `scheduler_service` -> `scheduler-service`.
func Kebab(name string) string {
	return xstrings.ToKebabCase(Pascal(name))
}

// SnakeUpper converts a name: `scheduler-service` -> `SCHEDULER_SERVICE`.
func SnakeUpper(name string) string {
	return strings.ToUpper(xstrings.ToSnakeCase(Pascal(name)))
}

// Human converts a name: `scheduler-service` -> `Scheduler Service`.
func Human(name string) string {
	return strings.Title(strings.ReplaceAll(Kebab(name), "-", " "))
}

// Tag builds the go-flags struct tag of an options group.
func Tag(group, namespace string) string {
	return "`group:\"" + group + "\" namespace:\"" + namespace + "\" env-namespace:\"" + SnakeUpper(namespace) + "\"`"
}
