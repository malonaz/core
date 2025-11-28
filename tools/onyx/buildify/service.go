package buildify

import (
	"fmt"
	"path/filepath"

	onyxpb "github.com/malonaz/core/genproto/onyx/v1"
	"github.com/malonaz/core/tools/onyx/types"
)

type Context struct {
	Manifest    *types.Manifest
	OutputFiles []string
}

func (b *Buildify) handleService(manifest *types.Manifest) ([]*types.GeneratedFile, error) {
	service := manifest.Message.(*onyxpb.Service)
	serviceName := service.Metadata.Name
	inputDir := filepath.Dir(manifest.Filepath)
	outputFile := fmt.Sprintf("%s.service.go", serviceName)

	// Prepare template data
	data := Context{
		Manifest:    manifest,
		OutputFiles: []string{outputFile},
	}

	// Generate BUILD content using template
	buildContent, err := b.templateEngine.EvaluateTemplate("buildify.service", data)
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate template: %w", err)
	}

	return []*types.GeneratedFile{
		{
			Output:  filepath.Join(inputDir, "BUILD"),
			Content: []byte(buildContent),
		},
	}, nil
}
