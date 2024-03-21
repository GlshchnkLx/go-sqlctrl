package sqlctrl

import (
	"testing"

	_ "github.com/go-sql-driver/mysql"
	_ "modernc.org/sqlite"
)

func TestNewDatabase(t *testing.T) {
	sqlDriver := "sqlite"
	sqlSource := "./test.db"
	sqlScheme := "./test.json"

	db, err := NewDatabase(sqlDriver, sqlSource, sqlScheme)
	if err != nil {
		t.Errorf("Error to create NewDatabase: %v", err)
		t.FailNow()
	}

	if db.sqlDriver != sqlDriver {
		t.Errorf("db.sqlDriver != sqlDriver")
	}

	if db.sqlSource != sqlSource {
		t.Errorf("db.sqlSource != sqlSource")
	}

	if db.sqlScheme != sqlScheme {
		t.Errorf("db.sqlScheme != sqlScheme")
	}
}
