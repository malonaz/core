package gen

import (
	"bytes"
	"fmt"
	"go/format"
	"path"
	"sort"
	"strings"
)

// File accumulates Go source. Imports are declared as they are used and emitted, aliased when two
// paths share a base name, when the file is formatted.
type File struct {
	pkg     string
	header  string
	body    bytes.Buffer
	imports map[string]string // import path -> alias
	aliases map[string]string // alias -> import path
	blank   []string          // side-effect imports
}

// NewFile starts a file of the package.
func NewFile(pkg, header string) *File {
	return &File{
		pkg:     pkg,
		header:  header,
		imports: map[string]string{},
		aliases: map[string]string{},
	}
}

// Import returns the identifier the package is referred to by in this file.
func (f *File) Import(importPath string) string {
	return f.ImportAs(importPath, path.Base(importPath))
}

// ImportAs imports the path under the requested alias, or the next free numbered variant of it.
func (f *File) ImportAs(importPath, alias string) string {
	if existing, ok := f.imports[importPath]; ok {
		return existing
	}
	candidate := alias
	for i := 2; ; i++ {
		if _, taken := f.aliases[candidate]; !taken {
			break
		}
		candidate = fmt.Sprintf("%s%d", alias, i)
	}
	f.imports[importPath] = candidate
	f.aliases[candidate] = importPath
	return candidate
}

// ImportBlank imports the path for its side effects.
func (f *File) ImportBlank(importPath string) {
	f.blank = append(f.blank, importPath)
}

// Qual returns `alias.Name` for a name of the package.
func (f *File) Qual(importPath, name string) string {
	return f.Import(importPath) + "." + name
}

// P writes the arguments, stringified and concatenated, as one line.
func (f *File) P(args ...any) {
	for _, arg := range args {
		fmt.Fprint(&f.body, arg)
	}
	f.body.WriteByte('\n')
}

// Bytes returns the formatted source.
func (f *File) Bytes() ([]byte, error) {
	var out bytes.Buffer
	if f.header != "" {
		out.WriteString(f.header)
		out.WriteString("\n")
	}
	fmt.Fprintf(&out, "package %s\n\n", f.pkg)
	if len(f.imports)+len(f.blank) > 0 {
		out.WriteString("import (\n")
		// Standard library first, then everything else.
		var groups [2][]string
		for importPath := range f.imports {
			group := 0
			if strings.Contains(strings.SplitN(importPath, "/", 2)[0], ".") {
				group = 1
			}
			groups[group] = append(groups[group], importPath)
		}
		for i, group := range groups {
			sort.Strings(group)
			if i > 0 && len(group) > 0 && len(groups[0]) > 0 {
				out.WriteString("\n")
			}
			for _, importPath := range group {
				alias := f.imports[importPath]
				if alias == path.Base(importPath) {
					fmt.Fprintf(&out, "\t%q\n", importPath)
				} else {
					fmt.Fprintf(&out, "\t%s %q\n", alias, importPath)
				}
			}
		}
		for _, importPath := range f.blank {
			fmt.Fprintf(&out, "\t_ %q\n", importPath)
		}
		out.WriteString(")\n\n")
	}
	out.Write(f.body.Bytes())
	formatted, err := format.Source(out.Bytes())
	if err != nil {
		return nil, fmt.Errorf("formatting generated code: %w\n%s", err, numbered(out.String()))
	}
	return formatted, nil
}

func numbered(src string) string {
	lines := strings.Split(src, "\n")
	for i, line := range lines {
		lines[i] = fmt.Sprintf("%4d  %s", i+1, line)
	}
	return strings.Join(lines, "\n")
}
