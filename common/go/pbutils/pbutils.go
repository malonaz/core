package pbutils

import (
	"strings"

	"github.com/mennanov/fmutils"
	"google.golang.org/protobuf/proto"
)

// ApplyMask filters a proto message with the given paths.
// Note that the given paths are structured as follow: "a.b,a.c" etc.
func ApplyMask(message proto.Message, paths string) {
	mask := fmutils.NestedMaskFromPaths(strings.Split(paths, ","))
	mask.Filter(message)
}

// ApplyMaskInverse prunes a proto message with the given paths.
// Note that the given paths are structured as follow: "a.b,a.c" etc.
func ApplyMaskInverse(message proto.Message, paths string) {
	mask := fmutils.NestedMaskFromPaths(strings.Split(paths, ","))
	mask.Prune(message)
}
