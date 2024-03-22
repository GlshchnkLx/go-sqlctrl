package sqlctrl

import (
	"os"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	_ "modernc.org/sqlite"
)

func TestNewDatabase(t *testing.T) {
	sqlDriver := "sqlite"
	sqlSource := "./test.db"
	sqlScheme := "./test.json"

	if _, err := os.Stat(sqlScheme); os.IsExist(err) { // if sqlScheme file already exist
		err = os.Remove(sqlScheme)
		if err != nil {
			t.Errorf("os.Remove(sqlScheme) error: %v", err)
			t.FailNow()
		}
	}

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

	if _, err := os.Stat(sqlScheme); os.IsNotExist(err) { // if sqlScheme file does not exist
		t.Errorf("sqlScheme file is not created: %v", err)
		t.FailNow()
	}
}
