package testserver

import (
	"fmt"
	"os/exec"
	"path/filepath"
)

// getPostgresBinaryDir looks for the `postgres` directory.
func getPostgresBinaryDir() string {
	postgres, err := exec.LookPath("postgres")
	if err != nil {
		panic(fmt.Errorf("looking up postgres on PATH: %w", err))
	}
	postgres, err = filepath.EvalSymlinks(postgres)
	if err != nil {
		panic(fmt.Errorf("resolving postgres path"))
	}
	return filepath.Dir(postgres)
}
