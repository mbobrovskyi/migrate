package bigquery

import (
	"github.com/golang-migrate/migrate/v4/database"
	"strings"
	"testing"
)

const (
	//connectionUrl = "bigquery://https://bigquery.googleapis.com/bigquery/v2/?x-migrations-table=schema_migrations&x-statement-timeout=0&credentials_filename=./tmp/myproject-XXXXXXXXXXXXX-XXXXXXXXXXXX.json&dataset_id=mydataset"
	connectionUrl = "bigquery://http://0.0.0.0:9050/?x-migrations-table=schema_migrations&project_id=myproject&dataset_id=mydataset"
)

func openConnection() (database.Driver, error) {
	b := &BigQuery{}

	driver, err := b.Open(connectionUrl)
	if err != nil {
		return nil, err
	}

	return driver, nil
}

func TestOpen(t *testing.T) {
	driver, err := openConnection()
	if err != nil {
		t.Error(err)
		return
	}

	defer driver.Close()
}

func TestClose(t *testing.T) {
	driver, err := openConnection()
	if err != nil {
		t.Error(err)
		return
	}

	defer driver.Close()

	err = driver.Close()
	if err != nil {
		t.Error(err)
		return
	}
}

func TestVersion(t *testing.T) {
	driver, err := openConnection()
	if err != nil {
		t.Error(err)
		return
	}

	defer driver.Close()

	version, dirty, err := driver.Version()
	if err != nil {
		t.Error(err)
		return
	}

	t.Log(version, dirty)
}

func TestSetVersion(t *testing.T) {
	driver, err := openConnection()
	if err != nil {
		t.Error(err)
		return
	}

	defer driver.Close()

	err = driver.SetVersion(-1, false)
	if err != nil {
		t.Error(err)
		return
	}
}

func TestDrop(t *testing.T) {
	driver, err := openConnection()
	if err != nil {
		t.Error(err)
		return
	}

	defer driver.Close()

	err = driver.Drop()
	if err != nil {
		t.Error(err)
		return
	}
}

func TestRun(t *testing.T) {
	driver, err := openConnection()
	if err != nil {
		t.Error(err)
		return
	}

	defer driver.Close()

	err = driver.Run(strings.NewReader(`
		CREATE TABLE IF NOT EXISTS users (
			first_name STRING,
		  	last_name STRING
		)`))
	if err != nil {
		t.Error(err)
		return
	}
}

func TestRunWithError(t *testing.T) {
	driver, err := openConnection()
	if err != nil {
		t.Error(err)
		return
	}

	defer driver.Close()

	err = driver.Run(strings.NewReader(`
		CREATE TABLE IF NOT EXISTS users (
			first_name STRINGa,
		  	last_name STRING
		)`))
	if err != nil {
		t.Log(err)
		return
	}

	t.Error("error is nil, should be 'googleapi: Error 400: Query error: Type not found: STRINGa at [4:36], invalidQuery'")
	return
}
