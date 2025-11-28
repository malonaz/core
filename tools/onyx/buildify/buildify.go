package buildify

import (
	"fmt"

	onyxpb "github.com/malonaz/core/proto/onyx/v1"
	"github.com/malonaz/core/tools/onyx/template"
	"github.com/malonaz/core/tools/onyx/types"
)

type Opts struct {
}

type Buildify struct {
	opts           *Opts
	templateEngine *template.Engine
}

type Request struct {
	Manifests []*types.Manifest
}

type Response struct {
	GeneratedFiles []*types.GeneratedFile
}

func New(opts *Opts, templateEngine *template.Engine) *Buildify {
	return &Buildify{
		opts:           opts,
		templateEngine: templateEngine,
	}
}

// Buildify generates BUILD files based on the proto message
func (b *Buildify) Buildify(request *Request) (*Response, error) {
	response := &Response{}
	for _, manifest := range request.Manifests {
		switch manifest.Message.(type) {
		case *onyxpb.Service:
			generatedFiles, err := b.handleService(manifest)
			if err != nil {
				return nil, fmt.Errorf("service: %w", err)
			}
			response.GeneratedFiles = append(response.GeneratedFiles, generatedFiles...)
		default:
			return nil, fmt.Errorf("unsupported message type: %T", manifest.Message)
		}
	}
	return response, nil
}
