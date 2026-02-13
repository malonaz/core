package jsonnet

import (
	"io/fs"
	"os"
	"path/filepath"

	"github.com/google/go-jsonnet"
	"github.com/google/go-jsonnet/ast"
)

type importer struct {
	fs    fs.FS
	cache map[string]jsonnet.Contents
}

func (i *importer) Import(importedFrom, importedPath string) (jsonnet.Contents, string, error) {
	var resolvedPath string
	if filepath.IsAbs(importedPath) {
		resolvedPath = importedPath
	} else {
		resolvedPath = filepath.Join(filepath.Dir(importedFrom), importedPath)
	}
	resolvedPath = filepath.Clean(resolvedPath)

	if content, ok := i.cache[resolvedPath]; ok {
		return content, resolvedPath, nil
	}

	var bytes []byte
	var err error
	if i.fs != nil {
		bytes, err = fs.ReadFile(i.fs, resolvedPath)
	} else {
		bytes, err = os.ReadFile(resolvedPath)
	}
	if err != nil {
		return jsonnet.Contents{}, "", err
	}
	contents := jsonnet.MakeContentsRaw(bytes)
	i.cache[resolvedPath] = contents
	return contents, resolvedPath, nil
}

type Option func(*jsonnet.VM)

func WithEnvVariables() Option {
	return func(vm *jsonnet.VM) {
		vm.NativeFunction(&jsonnet.NativeFunction{
			Name:   "env",
			Params: ast.Identifiers{"name"},
			Func: func(args []interface{}) (interface{}, error) {
				return os.Getenv(args[0].(string)), nil
			},
		})
	}
}

func EvaluateEmbeddedFile(filename string, embedFS fs.FS, options ...Option) ([]byte, error) {
	vm := jsonnet.MakeVM()
	for _, option := range options {
		option(vm)
	}
	vm.Importer(&importer{
		fs:    embedFS,
		cache: map[string]jsonnet.Contents{},
	})
	str, err := vm.EvaluateFile(filename)
	return []byte(str), err
}

func EvaluateSnippet(snippet string, options ...Option) ([]byte, error) {
	vm := jsonnet.MakeVM()
	for _, option := range options {
		option(vm)
	}
	str, err := vm.EvaluateAnonymousSnippet("anonymous.snippet", snippet)
	return []byte(str), err
}

func EvaluateFile(path string, options ...Option) ([]byte, error) {
	vm := jsonnet.MakeVM()
	for _, option := range options {
		option(vm)
	}
	vm.Importer(&importer{
		cache: map[string]jsonnet.Contents{},
	})
	str, err := vm.EvaluateFile(path)
	return []byte(str), err
}
