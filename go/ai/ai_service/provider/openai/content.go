package openai

import (
	"encoding/base64"
	"fmt"

	aipb "github.com/malonaz/core/genproto/ai/v1"
)

// imageURL returns the URL an image is sent as, inline data included, and its detail.
func imageURL(image *aipb.Image) (string, string, error) {
	detail, ok := imageQualityToOpenAI[image.Quality]
	if !ok {
		return "", "", fmt.Errorf("unknown image quality: %s", image.Quality)
	}
	switch source := image.Source.(type) {
	case *aipb.Image_Url:
		return source.Url, detail, nil
	case *aipb.Image_Data:
		return dataURL(image.MediaType, source.Data), detail, nil
	default:
		return "", "", fmt.Errorf("unknown image source type %T", source)
	}
}

func dataURL(mediaType string, data []byte) string {
	return fmt.Sprintf("data:%s;base64,%s", mediaType, base64.StdEncoding.EncodeToString(data))
}

var imageQualityToOpenAI = map[aipb.ImageQuality]string{
	aipb.ImageQuality_IMAGE_QUALITY_UNSPECIFIED: "auto",
	aipb.ImageQuality_IMAGE_QUALITY_AUTO:        "auto",
	aipb.ImageQuality_IMAGE_QUALITY_LOW:         "low",
	aipb.ImageQuality_IMAGE_QUALITY_HIGH:        "high",
}
