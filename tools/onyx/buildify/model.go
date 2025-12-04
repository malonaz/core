package buildify

import (
	onyxpb "github.com/malonaz/core/genproto/onyx/v1"
	"github.com/malonaz/core/tools/onyx/types"
)

func (b *Buildify) handleModel(manifest *types.Manifest) ([]*types.Rule, error) {
	model := manifest.Message.(*onyxpb.Model)

	rule := &types.Rule{
		Name: model.GetMetadata().GetName(),
		Srcs: []string{model.GetSpec().GetTarget()},
	}
	return []*types.Rule{
		rule,
	}, nil
}
