package main

import (
	"flag"
	"log"
	"multimigrator/internal"
	"multimigrator/multimigrator"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

func main() {

	migrations := flag.String("migrations", "", "Path to migrations directory")
	//packageName := flag.String("package", "migrationlevel", "Output package name")
	connStr := flag.String("connStr", "", "Connection string for target database")
	target := flag.String("target", "", "Target schema level to migrate to")

	flag.Parse()

	if migrations == nil || *migrations == "" {
		log.Fatal("No migrations directory provided, exiting")
	}
	if connStr == nil || *connStr == "" {
		log.Fatal("No connection string provided, exiting")
	}
	if target == nil || *target == "" {
		log.Fatal("No target level provided, exiting")
	}
	result, err := internal.ParseMigrationsDirectory(*migrations)
	if err != nil {
		log.Fatal(err)
	}

	migrator := &multimigrator.Migrator{
		RootDir:  *migrations,
		Schemata: result.Ordering,
		ConnStr:  *connStr,
	}
	config, err := pgx.ParseConfig(*connStr)
	if err != nil {
		log.Fatalf("%v", err)
	}
	db := stdlib.OpenDB(*config)
	err = migrator.Up(*target, db)
	if err != nil {
		log.Fatalf("%v", err)
	}

	//src, err := multimigrator.ProcessTemplate(multimigrator.TemplateArgs{
	//	PackageName: *packageName,
	//	Schemata:    result.DatabaseDescription.Ordering,
	//})
	//if err != nil {
	//	log.Fatal(err)
	//}
	//fmt.Println(src)
}
