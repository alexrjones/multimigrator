package migrator

import (
	"database/sql"
	"errors"
	"fmt"
	"math"
	"os"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source"
)

type Migrator struct {
	RootDir  string
	Schemata []string
}

var ErrNoSchema = errors.New("schema not found")

type migratorPart struct {
	sourceDrv          migrationVersioner
	instance           migrationStepper
	lastAppliedVersion uint
}

type migrationVersioner interface {
	Next(version uint) (nextVersion uint, err error)
}

type migrationStepper interface {
	Steps(n int) error
}

type migratorParts []*migratorPart

func (m *Migrator) Up(upToSchema string, db *sql.DB) error {

	index, ok := findSchema(upToSchema, m.Schemata)
	if !ok {
		return fmt.Errorf("couldn't find schema %s: %w", upToSchema, ErrNoSchema)
	}
	migrators := make(migratorParts, 0, index+1)

	for i := 0; i < index+1; i++ {

		schema := m.Schemata[i]
		schemaPath := fmt.Sprintf("file://%s/%s", m.RootDir, schema)
		sourceDrv, err := source.Open(schemaPath)
		if err != nil {
			return err
		}
		// Make sure there's at least one migration version available
		_, err = sourceDrv.First()
		if err != nil {
			return err
		}
		driver, err := postgres.WithInstance(db, &postgres.Config{SchemaName: schema})
		if err != nil {
			return err
		}
		instance, err := migrate.NewWithInstance(schema, sourceDrv, "test", driver)
		if err != nil {
			return err
		}
		migrators = append(migrators, &migratorPart{
			sourceDrv:          sourceDrv,
			instance:           instance,
			lastAppliedVersion: 0,
		})
	}

	return nil
}

func (mp migratorParts) applyMigrations() error {

	iter := 0
	var highestAppliedVersion uint = 0
	for {
		curr := mp[iter]
		nextVersion, err := curr.sourceDrv.Next(curr.lastAppliedVersion)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				// We've migrated to the current version of this schema
				if iter+1 == len(mp) {
					break
				}
				// We've finished this schema, but there are still
				// others to migrate
				iter = (iter + 1) % len(mp)
				continue
			}
			return err
		}
		if curr.lastAppliedVersion+1 == nextVersion || highestAppliedVersion == nextVersion {
			err = mp[iter].instance.Steps(1)
			if err != nil {
				return err
			}
			curr.lastAppliedVersion = nextVersion
			highestAppliedVersion = uint(math.Max(float64(highestAppliedVersion), float64(nextVersion)))
		}
		iter = (iter + 1) % len(mp)
	}

	return nil
}

func findSchema(name string, schemata []string) (int, bool) {

	for i, s := range schemata {
		if strings.EqualFold(s, name) {
			return i, true
		}
	}

	return -1, false
}
