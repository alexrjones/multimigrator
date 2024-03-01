package multimigrator

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

// Read path to migrations directory
// Find order.(yml|yaml) and read it
// Generate an enum for the ordering

var ErrNotDirectory = errors.New("provided path wasn't a directory")
var ErrIncorrectUseOfOrderYaml = errors.New("order.yaml was in the wrong directory")

type DatabaseDescription struct {
	DatabaseName string   `yaml:"database_name"`
	Ordering     []string `yaml:"schema_ordering"`
}

type MigrationParseResult struct {
	DatabaseDescription DatabaseDescription
	SchemaDirectories   map[string]string
}

var orderingRegex = regexp.MustCompile("order\\.ya?ml$")

func ParseMigrationsDirectory(migrationsDir string) (*MigrationParseResult, error) {

	stat, err := os.Stat(migrationsDir)
	if err != nil {
		return nil, err
	}
	if !stat.IsDir() {
		return nil, fmt.Errorf("for path %s: %w", migrationsDir, ErrNotDirectory)
	}
	dir, err := os.ReadDir(migrationsDir)
	if err != nil {
		return nil, err
	}
	var dd DatabaseDescription
	migrationDirs := make(map[string]string)
	for _, d := range dir {
		if orderingRegex.MatchString(d.Name()) {
			open, err := os.Open(filepath.Join(migrationsDir, d.Name()))
			if err != nil {
				return nil, err
			}
			err = yaml.NewDecoder(open).Decode(&dd)
			if err != nil {
				return nil, err
			}
		} else if d.IsDir() {
			migrationDirs[d.Name()] = filepath.Join(migrationsDir, d.Name())
		}
	}
	if dd.Ordering == nil {
		return nil, errors.New("no order.yaml found")
	}
	var errs []string
	for _, schema := range dd.Ordering {
		if _, ok := migrationDirs[schema]; !ok {
			errs = append(errs, schema)
		}
	}
	if len(errs) > 0 {
		return nil, fmt.Errorf("missing migration directories: %s", strings.Join(errs, ", "))
	}

	return &MigrationParseResult{
		DatabaseDescription: dd,
		SchemaDirectories:   migrationDirs,
	}, nil
}
