package main

import (
	"flag"
	"fmt"
	"log"
	"multimigrator/internal/multimigrator"
)

func main() {

	migrations := flag.String("migrations", "", "Path to migrations directory")
	packageName := flag.String("package", "migrationlevel", "Output package name")
	flag.Parse()

	if migrations == nil || *migrations == "" {
		log.Fatal("No migrations directory provided, exiting")
	}
	result, err := multimigrator.ParseMigrationsDirectory(*migrations)
	if err != nil {
		log.Fatal(err)
	}

	src, err := multimigrator.ProcessTemplate(multimigrator.TemplateArgs{
		PackageName: *packageName,
		Schemata:    result.DatabaseDescription.Ordering,
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(src)
}
