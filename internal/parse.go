package internal

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"

	"gopkg.in/yaml.v3"
)

// Read path to migrations directory
// Find order.(yml|yaml) and read it
// Generate an enum for the ordering

var ErrNotDirectory = errors.New("provided path wasn't a directory")

type DatabaseDescription struct {
	Ordering []string `yaml:"schema_ordering"`
}

var orderingRegex = regexp.MustCompile(`order\.ya?ml$`)

func ParseMigrationsDirectory(migrationsDir string) (*DatabaseDescription, error) {

	stat, err := os.Stat(migrationsDir)
	if err != nil {
		return nil, fmt.Errorf("could not stat path %s: %w", migrationsDir, err)
	}
	if !stat.IsDir() {
		return nil, fmt.Errorf("for path %s: %w", migrationsDir, ErrNotDirectory)
	}
	dir, err := os.ReadDir(migrationsDir)
	if err != nil {
		return nil, fmt.Errorf("could not read dir: %w", err)
	}
	var dd DatabaseDescription
	var found bool
	for _, d := range dir {
		if d.IsDir() || !orderingRegex.MatchString(d.Name()) {
			continue
		}
		open, err := os.Open(filepath.Join(migrationsDir, d.Name()))
		if err != nil {
			return nil, fmt.Errorf("could not open %s: %w", d.Name(), err)
		}
		err = yaml.NewDecoder(open).Decode(&dd)
		if err != nil {
			return nil, err
		}
		found = true
		break
	}
	if !found {
		return nil, errors.New("no order.yaml found")
	}

	return &dd, nil
}
