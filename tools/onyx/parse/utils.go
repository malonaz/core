package parse

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"
)

// loadGitignorePatterns loads patterns from a .gitignore file
func loadGitignorePatterns(gitignorePath string) ([]string, error) {
	file, err := os.Open(gitignorePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer file.Close()

	var patterns []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		patterns = append(patterns, line)
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return patterns, nil
}

// shouldIgnore checks if a path matches any gitignore pattern
func shouldIgnore(path string, rootDir string, patterns []string) bool {
	// Get relative path from root
	relPath, err := filepath.Rel(rootDir, path)
	if err != nil {
		return false
	}

	for _, pattern := range patterns {
		// Remove leading slash if present
		pattern = strings.TrimPrefix(pattern, "/")

		// Handle directory patterns (ending with / or /*)
		if strings.HasSuffix(pattern, "/") || strings.HasSuffix(pattern, "/*") {
			pattern = strings.TrimSuffix(pattern, "/")
			pattern = strings.TrimSuffix(pattern, "/*")
			// Check if path starts with this directory
			if strings.HasPrefix(relPath, pattern+string(filepath.Separator)) || relPath == pattern {
				return true
			}
		} else {
			// Simple pattern matching
			matched, err := filepath.Match(pattern, filepath.Base(relPath))
			if err == nil && matched {
				return true
			}

			// Check if any parent directory matches
			parts := strings.Split(relPath, string(filepath.Separator))
			for _, part := range parts {
				matched, err := filepath.Match(pattern, part)
				if err == nil && matched {
					return true
				}
			}

			// Check full relative path match
			matched, err = filepath.Match(pattern, relPath)
			if err == nil && matched {
				return true
			}
		}
	}

	return false
}
