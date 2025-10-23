package jsonnet

import (
	"embed"

	"github.com/google/go-jsonnet"
)

type importer struct {
	fs    embed.FS
	cache map[string]jsonnet.Contents
}

// Import implements the jsonnet importer interface.
func (i *importer) Import(importedFrom, importedPath string) (jsonnet.Contents, string, error) {
	// Check filesystem.
	if content, ok := i.cache[importedPath]; ok {
		return content, importedPath, nil
	}
	bytes, err := i.fs.ReadFile(importedPath)
	if err != nil {
		return jsonnet.Contents{}, "", err
	}
	contents := jsonnet.MakeContentsRaw(bytes)
	i.cache[importedPath] = contents
	return contents, importedPath, nil
}

func EvaluateFile(filename string, fs embed.FS) ([]byte, error) {
	vm := jsonnet.MakeVM()
	vm.Importer(&importer{
		fs:    fs,
		cache: map[string]jsonnet.Contents{},
	})
	str, err := vm.EvaluateFile(filename)
	return []byte(str), err
}

func EvaluateSnippet(snippet string) ([]byte, error) {
	vm := jsonnet.MakeVM()
	str, err := vm.EvaluateAnonymousSnippet("anonymous.snippet", snippet)
	return []byte(str), err
}
