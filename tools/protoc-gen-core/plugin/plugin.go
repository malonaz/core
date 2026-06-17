package plugin

import (
	"google.golang.org/protobuf/compiler/protogen"
)

// Opts holds resolved configuration passed from protoc flags to each generator.
type Opts struct {
	// AdditionalGoImportPaths maps user-defined keys (e.g. "model") to their
	// resolved import paths, built from the --additional-go-import-paths flag.
	AdditionalGoImportPaths map[string]protogen.GoImportPath

	// Configuration holds the parsed JSON configuration file contents,
	// supplied via the --configuration flag.
	Configuration map[any]any
}

// GenerateFunc is the signature that all native Go generators must implement.
type GenerateFunc func(
	file *protogen.File,
	generatedFile *protogen.GeneratedFile,
	packageName protogen.GoPackageName,
	opts *Opts,
) error
