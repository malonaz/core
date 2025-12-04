package types

import (
	"google.golang.org/protobuf/proto"
)

type GeneratedFile struct {
	// Relative to 'working directory'.
	Output string
	// The content to output.
	Content []byte
}

type Manifest struct {
	Filepath string
	Message  proto.Message
}

type Rule struct {
	Name string
	Srcs []string
	Deps []string
	Outs []string
}
