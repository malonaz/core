package manifest

import (
	"fmt"
	"path"
	"strings"
)

// Label is a please label as onyx_*_manifest rules rewrite it: the canonical label followed by the
// files it produces, `//pkg:name(path/a path/b)`. Bare labels carry no files.
type Label struct {
	Target string
	Files  []string
}

// ParseLabel parses `//pkg:name` or `//pkg:name(files...)`.
func ParseLabel(s string) (Label, error) {
	open := strings.Index(s, "(")
	if open == -1 {
		if !strings.HasPrefix(s, "//") {
			return Label{}, fmt.Errorf("non-canonical label %q", s)
		}
		return Label{Target: s}, nil
	}
	if !strings.HasSuffix(s, ")") {
		return Label{}, fmt.Errorf("malformed label %q", s)
	}
	label := Label{Target: s[:open], Files: strings.Fields(s[open+1 : len(s)-1])}
	if !strings.HasPrefix(label.Target, "//") {
		return Label{}, fmt.Errorf("non-canonical label %q", s)
	}
	if len(label.Files) == 0 {
		return Label{}, fmt.Errorf("label %q names no files", s)
	}
	return label, nil
}

// File returns the one file the label names.
func (l Label) File() (string, error) {
	if len(l.Files) != 1 {
		return "", fmt.Errorf("%s: expected one file, got %d", l.Target, len(l.Files))
	}
	return l.Files[0], nil
}

// GoImportPath resolves the Go import path of the package the label builds. Third-party labels
// (`//third_party/go:github.com__x__y`) encode the path; repo labels are rooted at goImportPath.
func (l Label) GoImportPath(goImportPath string) string {
	importPath := strings.TrimPrefix(l.Target, "//")
	if pkg, name, ok := strings.Cut(importPath, ":"); ok {
		importPath = pkg
		// `//user/v1:diff_name` builds a differently named package.
		if name != path.Base(pkg) {
			importPath = pkg + "/" + name
		}
	}
	if rest, ok := strings.CutPrefix(importPath, "third_party/go/"); ok {
		return strings.ReplaceAll(rest, "__", "/")
	}
	if goImportPath != "" {
		return goImportPath + "/" + importPath
	}
	return importPath
}
