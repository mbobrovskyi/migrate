//go:build bigquery
// +build bigquery

package cli

import (
	_ "github.com/golang-migrate/migrate/v4/database/bigquery"
)
