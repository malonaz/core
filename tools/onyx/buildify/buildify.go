package buildify

import (
	"fmt"

	onyxpb "github.com/malonaz/core/genproto/onyx/v1"
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
	Rules          []*types.Rule
	GeneratedFiles []*types.GeneratedFile
}

func New(opts *Opts, templateEngine *template.Engine) *Buildify {
	return &Buildify{
		opts:           opts,
		templateEngine: templateEngine,
	}
}

type handler func(manifest *types.Manifest) ([]*types.Rule, error)

// Buildify generates BUILD files based on the proto message
func (b *Buildify) Buildify(request *Request) (*Response, error) {
	response := &Response{}
	for _, manifest := range request.Manifests {
		var handler handler
		switch manifest.Message.(type) {
		case *onyxpb.Model:
			handler = b.handleModel
		default:
			return nil, fmt.Errorf("unsupported message type: %T", manifest.Message)
		}
		rules, err := handler(manifest)
		if err != nil {
			return nil, fmt.Errorf("service: %w", err)
		}
		response.Rules = append(response.Rules, rules...)
	}
	return response, nil
}
