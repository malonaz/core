package ai

import (
	"fmt"
	"strings"
)

// ExtractJSONString finds and extracts the first JSON object from LLM output
func ExtractJSONString(content string) (string, error) {
	// Find the first '{' and last '}'
	start := strings.Index(content, "{")
	if start == -1 {
		return "", fmt.Errorf("no JSON object found in content")
	}

	end := strings.LastIndex(content, "}")
	if end == -1 || end < start {
		return "", fmt.Errorf("no valid JSON object found in content")
	}

	return content[start : end+1], nil
}
