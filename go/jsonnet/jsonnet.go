package jsonnet

import (
	"io/fs"
	"os"

	"github.com/google/go-jsonnet"
)

type importer struct {
	fs    fs.FS
	cache map[string]jsonnet.Contents
}

// Note that we do not allow relative import paths.
func (i *importer) Import(importedFrom, importedPath string) (jsonnet.Contents, string, error) {
	// Check filesystem.
	if content, ok := i.cache[importedPath]; ok {
		return content, importedPath, nil
	}
	bytes, err := fs.ReadFile(i.fs, importedPath)
	if err != nil {
		return jsonnet.Contents{}, "", err
	}
	contents := jsonnet.MakeContentsRaw(bytes)
	i.cache[importedPath] = contents
	return contents, importedPath, nil
}

func EvaluateEmbeddedFile(filename string, embedFS fs.FS) ([]byte, error) {
	vm := jsonnet.MakeVM()
	vm.Importer(&importer{
		fs:    embedFS,
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

func EvaluateFile(path string) ([]byte, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	vm := jsonnet.MakeVM()
	vm.Importer(&importer{
		fs:    os.DirFS(cwd),
		cache: map[string]jsonnet.Contents{},
	})
	str, err := vm.EvaluateFile(path)
	return []byte(str), err
}
