package schematadriver

import (
	"io"
	"testing"

	assert "github.com/stretchr/testify/require"
)

const testDataPath = "../../testdata/test_schema_1"

func TestSchemataDriver_Open(t *testing.T) {

	d := &SchemataDriver{}
	driver, err := d.Open(testDataPath + "?path=0001_01_first_Start.up.sql&path=0002_01_first_Amend.up.sql")
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
	paths, err := ExpandPaths(testDataPath, schemata)
	assert.Nil(t, err)
	assert.Len(t, paths, 2)
	assert.Equal(t, []string{"0001_01_first_Start.up.sql", "0002_01_first_Amend.up.sql"}, paths["first"])
	assert.Equal(t, []string{"0001_02_second_Start.up.sql"}, paths["second"])
}
