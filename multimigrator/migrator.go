package multimigrator

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/alexrjones/multimigrator/internal/schematadriver"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

var ErrNoSchema = errors.New("schema not found")

type Migrator struct {
	RootDir     string
	Schemata    []string
	driverPaths []string
	enableLog   bool
}

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

func NewMigrator(rootDir string, schemata []string, enableLog bool) (*Migrator, error) {

	var paths map[string][]string
	var err error
	rootDir, err = filepath.Abs(rootDir)
	if err != nil {
		return nil, err
	}
	paths, err = schematadriver.ExpandPaths(rootDir, schemata)
	if err != nil {
		return nil, err
	}

	driverPaths := make([]string, len(schemata))
	for i := 0; i < len(schemata); i++ {
		driverPaths[i] = schematadriver.BuildURL(rootDir, paths[schemata[i]])
	}

	return &Migrator{
		RootDir:     rootDir,
		Schemata:    schemata,
		driverPaths: driverPaths,
		enableLog:   enableLog,
	}, nil
}

func (m *Migrator) Up(upToSchema string, db *sql.DB) error {

	var logger migrate.Logger = NilLogger{}
	index, ok := findSchema(upToSchema, m.Schemata)
	if !ok {
		return fmt.Errorf("couldn't find schema %s: %w", upToSchema, ErrNoSchema)
	}
	migrators := make(migratorParts, 0, index+1)

	for i := 0; i < index+1; i++ {

		schema := m.Schemata[i]
		sourceDrv, err := source.Open(m.driverPaths[i])
		if err != nil {
			return fmt.Errorf("while opening driver for schema %s: %w", schema, err)
		}
		defer sourceDrv.Close()
		// Make sure there's at least one migration version available
		first, err := sourceDrv.First()
		if err != nil {
			return fmt.Errorf("while getting first version for schema %s: %w", schema, err)
		}
		driver, err := postgres.WithInstance(db, &postgres.Config{MigrationsTable: schema + "_" + postgres.DefaultMigrationsTable})
		if err != nil {
			return fmt.Errorf("while opening database for schema %s: %w", schema, err)
		}
		defer driver.Close()
		instance, err := migrate.NewWithInstance(schema, sourceDrv, "test", driver)
		if err != nil {
			return fmt.Errorf("while creating migrate instance for schema %s: %w", schema, err)
		}
		if m.enableLog {
			logger = NewMigrateLogger()
			instance.Log = logger
		}
		migrators = append(migrators, &migratorPart{
			sourceDrv:    sourceDrv,
			instance:     instance,
			firstVersion: first,
		})
	}

	return migrators.applyMigrations(logger)
}

func (mp migratorParts) applyMigrations(logger migrate.Logger) error {

	iter := 0
	appliedCount := 0
	var versionToApply uint = 1
	lastSeenNextVersions := make([]uint, len(mp))
	done := make([]bool, len(mp))
	for {
		if !done[iter] && lastSeenNextVersions[iter] <= versionToApply && mp[iter].firstVersion <= versionToApply {
			curr := mp[iter]
			// Get the current applied version for this schema
			appliedVersion, _, err := curr.instance.Version()
			if err != nil && !errors.Is(err, migrate.ErrNilVersion) {
				return err
			}
			if appliedVersion < versionToApply && versionToApply >= curr.firstVersion {
				var nextVersion uint
				if versionToApply == curr.firstVersion {
					nextVersion = curr.firstVersion
				} else if lastSeenNextVersions[iter] != 0 && lastSeenNextVersions[iter] == versionToApply {
					nextVersion = versionToApply
				} else {
					// Get the next version that this schema has
					nextVersion, err = curr.sourceDrv.Next(versionToApply - 1)
					if err != nil {
						if !errors.Is(err, os.ErrNotExist) {
							return err
						}
						done[iter] = true
						// We've finished migrating this schema
						iter = (iter + 1) % len(mp)
						if iter == 0 {
							// We've migrated to the current version of our target schema
							break
						}
						// There are still more schemata to migrate
						continue
					}
					if nextVersion > versionToApply {
						lastSeenNextVersions[iter] = nextVersion
					}
				}
				if nextVersion == versionToApply {
					err = mp[iter].instance.Steps(1)
					if err != nil {
						return err
					}
					appliedCount++
				}
			}
		}
		iter = (iter + 1) % len(mp)
		if iter == 0 {
			versionToApply++
		}
	}

	logger.Printf("Ran %d migrations", appliedCount)

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

func NewMigrateLogger() migrate.Logger {
	return MigrateLogger{true}
}

type MigrateLogger struct {
	verbose bool
}

func (ml MigrateLogger) Printf(format string, v ...any) {
	log.Printf(format, v...)
}

func (ml MigrateLogger) Verbose() bool {
	return ml.verbose
}

type NilLogger struct{}

func (ml NilLogger) Printf(format string, v ...any) {}

func (ml NilLogger) Verbose() bool { return false }
