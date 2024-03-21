package schematadriver

import (
	"io"
	"testing"

	assert "github.com/stretchr/testify/require"
)

const (
	testDataRoot = "../../testdata/"
	testSchema1  = testDataRoot + "test_schema_1"
	testSchema2  = testDataRoot + "test_schema_2"
)

func TestSchemataDriver_Open(t *testing.T) {

	d := &SchemataDriver{}
	driver, err := d.Open(testSchema1 + "?path=0001_01_first_Start.up.sql&path=0002_01_first_Amend.up.sql")
	assert.Nil(t, err)

	version, err := driver.First()
	assert.Nil(t, err)
	assert.Equal(t, uint(1), version)
	bodyOne, identifierOne, err := driver.ReadUp(version)
	assert.Nil(t, err)
	defer bodyOne.Close()
	assert.Equal(t, "01_first_Start", identifierOne)
	bodyContentsOne, err := io.ReadAll(bodyOne)
	assert.Nil(t, err)
	assert.Equal(t, "CREATE SCHEMA first;\n", string(bodyContentsOne))

	secondVersion, err := driver.Next(version)
	assert.Nil(t, err)
	assert.Equal(t, uint(2), secondVersion)
	bodyTwo, identifierTwo, err := driver.ReadUp(secondVersion)
	assert.Nil(t, err)
	defer bodyTwo.Close()
	assert.Equal(t, "01_first_Amend", identifierTwo)
	bodyContentsTwo, err := io.ReadAll(bodyTwo)
	assert.Nil(t, err)
	assert.Equal(t, "DROP SCHEMA first;\n", string(bodyContentsTwo))
}

func TestExpandPaths(t *testing.T) {

	schemata := []string{"first", "second"}
	paths, err := ExpandPaths(testSchema1, schemata)
	assert.Nil(t, err)
	assert.Len(t, paths, 2)
	assert.Equal(t, []string{"0001_01_first_Start.up.sql", "0002_01_first_Amend.up.sql"}, paths["first"])
	assert.Equal(t, []string{"0001_02_second_Start.up.sql"}, paths["second"])
}

func TestExpandPaths_MultipleMatches(t *testing.T) {

	schemata := []string{"abcd", "abcde"}
	paths, err := ExpandPaths(testSchema2, schemata)
	assert.Nil(t, err)
	assert.Len(t, paths, 2)
	assert.Equal(t, []string{"001_100_abcd_Start.up.sql", "002_100_abcd_Amend.up.sql"}, paths["abcd"])
	assert.Equal(t, []string{"001_200_abcde_Start.up.sql"}, paths["abcde"])
}
