package multimigrator

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

type Migrator struct {
	RootDir  string
	Schemata []string
	ConnStr  string
}

var ErrNoSchema = errors.New("schema not found")

type migratorPart struct {
	sourceDrv    migrationSource
	instance     migrationTarget
	firstVersion uint
}

type migrationSource interface {
	Next(version uint) (nextVersion uint, err error)
}

type migrationTarget interface {
	Version() (version uint, dirty bool, err error)
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
		first, err := sourceDrv.First()
		if err != nil {
			return err
		}
		driver, err := postgres.WithInstance(db, &postgres.Config{MigrationsTable: schema + "_" + postgres.DefaultMigrationsTable})
		if err != nil {
			return err
		}
		instance, err := migrate.NewWithInstance(schema, sourceDrv, "test", driver)
		if err != nil {
			return err
		}
		migrators = append(migrators, &migratorPart{
			sourceDrv:    sourceDrv,
			instance:     instance,
			firstVersion: first,
		})
	}

	return migrators.applyMigrations()
}

func (mp migratorParts) applyMigrations() error {

	iter := 0
	var versionToApply uint = 1
	for {
		curr := mp[iter]
		appliedVersion, _, err := curr.instance.Version()
		if err != nil && !errors.Is(err, migrate.ErrNilVersion) {
			return err
		}
		if appliedVersion < versionToApply && versionToApply >= curr.firstVersion {
			var nextVersion uint
			if versionToApply == curr.firstVersion {
				nextVersion = curr.firstVersion
			} else {
				// Get the next version that this schema has
				nextVersion, err = curr.sourceDrv.Next(versionToApply - 1)
				if err != nil {
					if errors.Is(err, os.ErrNotExist) {
						// We've finished migrating this schema
						iter = (iter + 1) % len(mp)
						if iter == 0 {
							// We've migrated to the current version of our target schema
							break
						}
						// There are still more schemata to migrate
						continue
					}
					return err
				}
			}
			if nextVersion == versionToApply {
				err = mp[iter].instance.Steps(1)
				if err != nil {
					return err
				}
			}
		}
		iter = (iter + 1) % len(mp)
		if iter == 0 {
			versionToApply++
		}
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
