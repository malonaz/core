package testserver

import (
	"os/exec"
	"path/filepath"
)

// getPostgresBinaryDir looks for the `postgres` directory.
func getPostgresBinaryDir() string {
	postgres, err := exec.LookPath("postgres")
	if err != nil {
		logger.Fatalf("Can't find postgres on PATH: %s", err)
	}
	postgres, err = filepath.EvalSymlinks(postgres)
	if err != nil {
		logger.Fatalf("Can't resolve postgres to a real path: %s", err)
	}
	return filepath.Dir(postgres)
}
