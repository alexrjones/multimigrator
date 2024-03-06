package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"multimigrator/internal"
	"multimigrator/multimigrator"
	"os"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

func main() {

	upFlags := flag.NewFlagSet("up", flag.ExitOnError)
	migrationsUp := upFlags.String("migrations", "", "Path to migrations directory")
	connStr := upFlags.String("connStr", "", "Connection string for target database")
	level := upFlags.String("level", "", "Target schema level to migrate to")

	codegenFlags := flag.NewFlagSet("codegen", flag.ExitOnError)
	migrationsCodegen := codegenFlags.String("migrations", "", "Path to migrations directory")
	packageName := codegenFlags.String("package", "migrationlevel", "Output package name")

	flag.Parse()

	if len(os.Args) < 2 {
		log.Fatalf("No subcommand provided")
	}
	switch os.Args[1] {
	case "up":
		{
			err := upFlags.Parse(os.Args[2:])
			if err != nil {
				log.Fatalf("%v", err)
			}
			err = migrate(*migrationsUp, *connStr, *level)
			if err != nil {
				log.Fatalf("%v", err)
			}
			return
		}
	case "codegen":
		{
			err := codegenFlags.Parse(os.Args[2:])
			if err != nil {
				log.Fatalf("%v", err)
			}
			err = codegen(*migrationsCodegen, *packageName)
			if err != nil {
				log.Fatalf("%v", err)
			}
			return
		}
	}
	log.Fatalf("Invalid subcommand name %s", os.Args[1])
}

func migrate(migrationsDir, connStr, target string) error {
	if migrationsDir == "" {
		return errors.New("no migrations directory provided")
	}
	if connStr == "" {
		return errors.New("no connection string provided")
	}
	if target == "" {
		return errors.New("no target level provided")
	}
	result, err := internal.ParseMigrationsDirectory(migrationsDir)
	if err != nil {
		return err
	}
	migrator, err := multimigrator.NewMigrator(migrationsDir, result.Ordering, multimigrator.MigratorModeFlat)
	if err != nil {
		return err
	}
	config, err := pgx.ParseConfig(connStr)
	if err != nil {
		return err
	}
	db := stdlib.OpenDB(*config)
	return migrator.Up(target, db)
}

func codegen(migrationsDir, packageName string) error {
	if migrationsDir == "" {
		return errors.New("no migrations directory provided")
	}
	if packageName == "" {
		return errors.New("no package name provided")
	}
	result, err := internal.ParseMigrationsDirectory(migrationsDir)
	if err != nil {
		return err
	}
	src, err := internal.ProcessTemplate(internal.TemplateArgs{
		PackageName: packageName,
		Schemata:    result.Ordering,
	})
	if err != nil {
		return err
	}
	fmt.Println(src)
	return nil
}
