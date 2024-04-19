package sqlctrl

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	_ "modernc.org/sqlite"
)

func TestNewDatabase(t *testing.T) {
	sqlDriver := "sqlite"
	sqlSource := "./test.db"

	// if _, err := os.Stat(sqlScheme); os.IsExist(err) { // if sqlScheme file already exist
	// 	err = os.Remove(sqlScheme)
	// 	if err != nil {
	// 		t.Errorf("os.Remove(sqlScheme) error: %v", err)
	// 		t.FailNow()
	// 	}
	// }

	db, err := NewDatabase(sqlDriver, sqlSource)
	if err != nil {
		t.Errorf("Error to create NewDatabase: %v", err)
		t.FailNow()
	}

	if db == nil {
		t.Errorf("db == nil")
		t.FailNow()
	}

	if db.sqlDriver != sqlDriver {
		t.Errorf("db.sqlDriver != sqlDriver")
	}

	if db.sqlSource != sqlSource {
		t.Errorf("db.sqlSource != sqlSource")
	}

	// if _, err := os.Stat(sqlScheme); os.IsNotExist(err) { // if sqlScheme file does not exist
	// 	t.Errorf("sqlScheme file is not created: %v", err)
	// 	t.FailNow()
	// }
}

func TestCheckExistTable(t *testing.T) {
	type TestTable struct {
		ParamA int64  `sql:"NAME=paramA, TYPE=INTEGER, PRIMARY_KEY, AUTO_INCREMENT"`
		ParamB string `sql:"NAME=paramB, TYPE=TEXT(32)"`
	}

	sqlDriver := "sqlite"
	sqlSource := "./test.db"

	db, err := NewDatabase(sqlDriver, sqlSource)
	if err != nil {
		t.Errorf("NewDatabase error: %v", err)
		t.FailNow()
	}

	table, err := NewTable("test_table", TestTable{})
	if err != nil {
		t.Errorf("NewTable error: %v", err)
		t.FailNow()
	}

	if db.CheckExistTable(table) {
		t.Errorf("table already exist")
		t.FailNow()
	}

	err = db.CreateTable(table)
	if err != nil {
		t.Errorf("db.CreateTable error: %v", err)
		t.FailNow()
	}

	if !db.CheckExistTable(table) {
		t.Errorf("table does not exist")
		t.FailNow()
	}

	err = db.DropTable(table)
	if err != nil {
		t.Errorf("db.DropTable error: %v", err)
		t.FailNow()
	}
}

func TestCreateTable(t *testing.T) {
	type TestTable struct {
		ParamA int64  `sql:"NAME=paramA, TYPE=INTEGER, PRIMARY_KEY, AUTO_INCREMENT"`
		ParamB string `sql:"NAME=paramB, TYPE=TEXT(32)"`
	}

	sqlDriver := "sqlite"
	sqlSource := "./test.db"

	db, err := NewDatabase(sqlDriver, sqlSource)
	if err != nil {
		t.Errorf("NewDatabase error: %v", err)
		t.FailNow()
	}

	table, err := NewTable("test_table", TestTable{})
	if err != nil {
		t.Errorf("NewTable error: %v", err)
		t.FailNow()
	}

	if db.CheckExistTable(table) {
		t.Errorf("table already exist")
		t.FailNow()
	}

	err = db.CreateTable(table)
	if err != nil {
		t.Errorf("db.CreateTable error: %v", err)
		t.FailNow()
	}

	if !db.CheckExistTable(table) {
		t.Errorf("table does not exist")
		t.FailNow()
	}

	err = db.DropTable(table)
	if err != nil {
		t.Errorf("db.DropTable error: %v", err)
		t.FailNow()
	}
}

func TestDropTable(t *testing.T) {
	type TestTable struct {
		ParamA int64  `sql:"NAME=paramA, TYPE=INTEGER, PRIMARY_KEY, AUTO_INCREMENT"`
		ParamB string `sql:"NAME=paramB, TYPE=TEXT(32)"`
	}

	sqlDriver := "sqlite"
	sqlSource := "./test.db"

	db, err := NewDatabase(sqlDriver, sqlSource)
	if err != nil {
		t.Errorf("NewDatabase error: %v", err)
		t.FailNow()
	}

	table, err := NewTable("test_table", TestTable{})
	if err != nil {
		t.Errorf("NewTable error: %v", err)
		t.FailNow()
	}

	if db.CheckExistTable(table) {
		t.Errorf("table already exist")
		t.FailNow()
	}

	err = db.CreateTable(table)
	if err != nil {
		t.Errorf("db.CreateTable error: %v", err)
		t.FailNow()
	}

	if !db.CheckExistTable(table) {
		t.Errorf("table does not exist")
		t.FailNow()
	}

	err = db.DropTable(table)
	if err != nil {
		t.Errorf("db.DropTable error: %v", err)
		t.FailNow()
	}

	if db.CheckExistTable(table) {
		t.Errorf("table already exist")
		t.FailNow()
	}
}

func TestTruncateTable(t *testing.T) {
	type TestTable struct {
		ParamA int64  `sql:"NAME=paramA, TYPE=INTEGER, PRIMARY_KEY, AUTO_INCREMENT"`
		ParamB string `sql:"NAME=paramB, TYPE=TEXT(32)"`
	}

	sqlDriver := "sqlite"
	sqlSource := "./test.db"

	db, err := NewDatabase(sqlDriver, sqlSource)
	if err != nil {
		t.Errorf("NewDatabase error: %v", err)
		t.FailNow()
	}

	table, err := NewTable("test_table", TestTable{})
	if err != nil {
		t.Errorf("NewTable error: %v", err)
		t.FailNow()
	}

	if db.CheckExistTable(table) {
		t.Errorf("table already exist")
		t.FailNow()
	}

	err = db.CreateTable(table)
	if err != nil {
		t.Errorf("db.CreateTable error: %v", err)
		t.FailNow()
	}

	if !db.CheckExistTable(table) {
		t.Errorf("table does not exist")
		t.FailNow()
	}

	_, err = db.InsertValue(table, []TestTable{
		{ParamB: "a"},
		{ParamB: "b"},
		{ParamB: "c"},
	})
	if err != nil {
		t.Errorf("db.InsertValue error: %v", err)
		t.FailNow()
	}

	count, err := db.GetCount(table)
	if err != nil {
		t.Errorf("db.GetCount error: %v", err)
		t.FailNow()
	}

	if count != 3 {
		t.Errorf("wrong count")
		t.FailNow()
	}

	err = db.TruncateTable(table)
	if err != nil {
		t.Errorf("db.TruncateTable error: %v", err)
		t.FailNow()
	}

	count, err = db.GetCount(table)
	if err != nil {
		t.Errorf("db.GetCount error: %v", err)
		t.FailNow()
	}

	if count != 0 {
		t.Errorf("wrong count")
		t.FailNow()
	}

	err = db.DropTable(table)
	if err != nil {
		t.Errorf("db.DropTable error: %v", err)
		t.FailNow()
	}

	if db.CheckExistTable(table) {
		t.Errorf("table already exist")
		t.FailNow()
	}
}

func TestGetCount(t *testing.T) {
	type TestTable struct {
		ParamA int64  `sql:"NAME=paramA, TYPE=INTEGER, PRIMARY_KEY, AUTO_INCREMENT"`
		ParamB string `sql:"NAME=paramB, TYPE=TEXT(32)"`
	}

	sqlDriver := "sqlite"
	sqlSource := "./test.db"

	db, err := NewDatabase(sqlDriver, sqlSource)
	if err != nil {
		t.Errorf("NewDatabase error: %v", err)
		t.FailNow()
	}

	table, err := NewTable("test_table", TestTable{})
	if err != nil {
		t.Errorf("NewTable error: %v", err)
		t.FailNow()
	}

	if !db.CheckExistTable(table) {
		err = db.CreateTable(table)
		if err != nil {
			t.Errorf("db.CreateTable error: %v", err)
			t.FailNow()
		}
	}

	_, err = db.InsertValue(table, []TestTable{
		{ParamB: "a"},
		{ParamB: "b"},
		{ParamB: "c"},
	})
	if err != nil {
		t.Errorf("db.InsertValue error: %v", err)
		t.FailNow()
	}

	count, err := db.GetCount(table)
	if err != nil {
		t.Errorf("db.GetCount error: %v", err)
		t.FailNow()
	}

	if count != 3 {
		t.Errorf("wrong count")
		t.FailNow()
	}

	err = db.DropTable(table)
	if err != nil {
		t.Errorf("db.DropTable error: %v", err)
		t.FailNow()
	}
}

func TestGetLastId(t *testing.T) {
	type TestTable struct {
		ParamA int64  `sql:"NAME=paramA, TYPE=INTEGER, PRIMARY_KEY, AUTO_INCREMENT"`
		ParamB string `sql:"NAME=paramB, TYPE=TEXT(32)"`
	}

	sqlDriver := "sqlite"
	sqlSource := "./test.db"

	db, err := NewDatabase(sqlDriver, sqlSource)
	if err != nil {
		t.Errorf("NewDatabase error: %v", err)
		t.FailNow()
	}

	table, err := NewTable("test_table", TestTable{})
	if err != nil {
		t.Errorf("NewTable error: %v", err)
		t.FailNow()
	}

	if !db.CheckExistTable(table) {
		err = db.CreateTable(table)
		if err != nil {
			t.Errorf("db.CreateTable error: %v", err)
			t.FailNow()
		}
	}

	_, err = db.InsertValue(table, []TestTable{
		{ParamB: "a"},
		{ParamB: "b"},
		{ParamB: "c"},
	})
	if err != nil {
		t.Errorf("db.InsertValue error: %v", err)
		t.FailNow()
	}

	lastId, err := db.GetLastId(table)
	if err != nil {
		t.Errorf("db.GetLastId error: %v", err)
		t.FailNow()
	}

	if lastId != 3 {
		t.Errorf("wrong lastId")
		t.FailNow()
	}

	err = db.DropTable(table)
	if err != nil {
		t.Errorf("db.DropTable error: %v", err)
		t.FailNow()
	}
}

func TestSelectAll(t *testing.T) {
	type TestTable struct {
		ParamA int64  `sql:"NAME=paramA, TYPE=INTEGER, PRIMARY_KEY, AUTO_INCREMENT"`
		ParamB string `sql:"NAME=paramB, TYPE=TEXT(32)"`
	}

	sqlDriver := "sqlite"
	sqlSource := "./test.db"

	db, err := NewDatabase(sqlDriver, sqlSource)
	if err != nil {
		t.Errorf("NewDatabase error: %v", err)
		t.FailNow()
	}

	table, err := NewTable("test_table", TestTable{})
	if err != nil {
		t.Errorf("NewTable error: %v", err)
		t.FailNow()
	}

	if !db.CheckExistTable(table) {
		err = db.CreateTable(table)
		if err != nil {
			t.Errorf("db.CreateTable error: %v", err)
			t.FailNow()
		}
	}

	_, err = db.InsertValue(table, []TestTable{
		{ParamB: "a"},
		{ParamB: "b"},
		{ParamB: "c"},
	})
	if err != nil {
		t.Errorf("db.InsertValue error: %v", err)
		t.FailNow()
	}

	responseIface, err := db.SelectAll(table)
	if err != nil {
		t.Errorf("db.SelectAll error: %v", err)
		t.FailNow()
	}

	responseArray, ok := responseIface.([]TestTable)
	if !ok {
		t.Errorf("wrong type assertion responseIface to []TestTable")
		t.FailNow()
	}

	if len(responseArray) < 3 {
		t.Errorf("len(responseArray) < 3")
		t.FailNow()
	}

	if responseArray[2].ParamA != 3 {
		t.Errorf("wrong ParamA")
		t.FailNow()
	}

	err = db.DropTable(table)
	if err != nil {
		t.Errorf("db.DropTable error: %v", err)
		t.FailNow()
	}
}

func TestSelectAllWithLimit(t *testing.T) {
	type TestTable struct {
		ParamA int64  `sql:"NAME=paramA, TYPE=INTEGER, PRIMARY_KEY, AUTO_INCREMENT"`
		ParamB string `sql:"NAME=paramB, TYPE=TEXT(32)"`
	}

	sqlDriver := "sqlite"
	sqlSource := "./test.db"

	db, err := NewDatabase(sqlDriver, sqlSource)
	if err != nil {
		t.Errorf("NewDatabase error: %v", err)
		t.FailNow()
	}

	table, err := NewTable("test_table", TestTable{})
	if err != nil {
		t.Errorf("NewTable error: %v", err)
		t.FailNow()
	}

	if !db.CheckExistTable(table) {
		err = db.CreateTable(table)
		if err != nil {
			t.Errorf("db.CreateTable error: %v", err)
			t.FailNow()
		}
	}

	_, err = db.InsertValue(table, []TestTable{
		{ParamB: "a"},
		{ParamB: "b"},
		{ParamB: "c"},
	})
	if err != nil {
		t.Errorf("db.InsertValue error: %v", err)
		t.FailNow()
	}

	responseIface, err := db.SelectAllWithLimit(table, 0, 10)
	if err != nil {
		t.Errorf("db.SelectAll error: %v", err)
		t.FailNow()
	}

	responseArray, ok := responseIface.([]TestTable)
	if !ok {
		t.Errorf("wrong type assertion responseIface to []TestTable")
		t.FailNow()
	}

	if len(responseArray) < 3 {
		t.Errorf("len(responseArray) < 3")
		t.FailNow()
	}

	if responseArray[2].ParamA != 3 {
		t.Errorf("wrong ParamA")
		t.FailNow()
	}

	err = db.DropTable(table)
	if err != nil {
		t.Errorf("db.DropTable error: %v", err)
		t.FailNow()
	}
}

func TestSelectValue(t *testing.T) {
	type TestTable struct {
		ParamA int64  `sql:"NAME=paramA, TYPE=INTEGER, PRIMARY_KEY, AUTO_INCREMENT"`
		ParamB string `sql:"NAME=paramB, TYPE=TEXT(32)"`
	}

	sqlDriver := "sqlite"
	sqlSource := "./test.db"

	db, err := NewDatabase(sqlDriver, sqlSource)
	if err != nil {
		t.Errorf("NewDatabase error: %v", err)
		t.FailNow()
	}

	table, err := NewTable("test_table", TestTable{})
	if err != nil {
		t.Errorf("NewTable error: %v", err)
		t.FailNow()
	}

	if !db.CheckExistTable(table) {
		err = db.CreateTable(table)
		if err != nil {
			t.Errorf("db.CreateTable error: %v", err)
			t.FailNow()
		}
	}

	_, err = db.InsertValue(table, []TestTable{
		{ParamB: "a"},
		{ParamB: "b"},
		{ParamB: "c"},
	})
	if err != nil {
		t.Errorf("db.InsertValue error: %v", err)
		t.FailNow()
	}

	responseIface, err := db.SelectValue(table, "ParamA = 3")
	if err != nil {
		t.Errorf("db.SelectValue error: %v", err)
		t.FailNow()
	}

	responseArray, ok := responseIface.([]TestTable)
	if !ok {
		t.Errorf("wrong type assertion responseIface to []TestTable")
		t.FailNow()
	}

	if responseArray[0].ParamA != 3 {
		t.Errorf("wrong ParamA")
		t.FailNow()
	}

	err = db.DropTable(table)
	if err != nil {
		t.Errorf("db.DropTable error: %v", err)
		t.FailNow()
	}
}

func TestSelectValueSingle(t *testing.T) {
	type TestTable struct {
		ParamA int64  `sql:"NAME=paramA, TYPE=INTEGER, PRIMARY_KEY, AUTO_INCREMENT"`
		ParamB string `sql:"NAME=paramB, TYPE=TEXT(32)"`
	}

	sqlDriver := "sqlite"
	sqlSource := "./test.db"

	db, err := NewDatabase(sqlDriver, sqlSource)
	if err != nil {
		t.Errorf("NewDatabase error: %v", err)
		t.FailNow()
	}

	table, err := NewTable("test_table", TestTable{})
	if err != nil {
		t.Errorf("NewTable error: %v", err)
		t.FailNow()
	}

	if !db.CheckExistTable(table) {
		err = db.CreateTable(table)
		if err != nil {
			t.Errorf("db.CreateTable error: %v", err)
			t.FailNow()
		}
	}

	_, err = db.InsertValue(table, []TestTable{
		{ParamB: "a"},
		{ParamB: "b"},
		{ParamB: "c"},
	})
	if err != nil {
		t.Errorf("db.InsertValue error: %v", err)
		t.FailNow()
	}

	responseIface, err := db.SelectValueSingle(table, "ParamA = 3")
	if err != nil {
		t.Errorf("db.SelectValueSingle error: %v", err)
		t.FailNow()
	}

	response, ok := responseIface.(TestTable)
	if !ok {
		t.Errorf("wrong type assertion responseIface to TestTable")
		t.FailNow()
	}

	if response.ParamA != 3 {
		t.Errorf("wrong ParamA")
		t.FailNow()
	}

	err = db.DropTable(table)
	if err != nil {
		t.Errorf("db.DropTable error: %v", err)
		t.FailNow()
	}
}

func TestSelectValueById(t *testing.T) {
	type TestTable struct {
		ParamA int64  `sql:"NAME=paramA, TYPE=INTEGER, PRIMARY_KEY, AUTO_INCREMENT"`
		ParamB string `sql:"NAME=paramB, TYPE=TEXT(32)"`
	}

	sqlDriver := "sqlite"
	sqlSource := "./test.db"

	db, err := NewDatabase(sqlDriver, sqlSource)
	if err != nil {
		t.Errorf("NewDatabase error: %v", err)
		t.FailNow()
	}

	table, err := NewTable("test_table", TestTable{})
	if err != nil {
		t.Errorf("NewTable error: %v", err)
		t.FailNow()
	}

	if !db.CheckExistTable(table) {
		err = db.CreateTable(table)
		if err != nil {
			t.Errorf("db.CreateTable error: %v", err)
			t.FailNow()
		}
	}

	_, err = db.InsertValue(table, []TestTable{
		{ParamB: "a"},
		{ParamB: "b"},
		{ParamB: "c"},
	})
	if err != nil {
		t.Errorf("db.InsertValue error: %v", err)
		t.FailNow()
	}

	responseIface, err := db.SelectValueById(table, 3)
	if err != nil {
		t.Errorf("db.SelectValueById error: %v", err)
		t.FailNow()
	}

	response, ok := responseIface.(TestTable)
	if !ok {
		t.Errorf("wrong type assertion responseIface to TestTable")
		t.FailNow()
	}

	if response.ParamA != 3 {
		t.Errorf("wrong ParamA")
		t.FailNow()
	}

	err = db.DropTable(table)
	if err != nil {
		t.Errorf("db.DropTable error: %v", err)
		t.FailNow()
	}
}

func TestInsertValue(t *testing.T) {
	type TestTable struct {
		ParamA int64  `sql:"NAME=paramA, TYPE=INTEGER, PRIMARY_KEY, AUTO_INCREMENT"`
		ParamB string `sql:"NAME=paramB, TYPE=TEXT(32)"`
	}

	sqlDriver := "sqlite"
	sqlSource := "./test.db"

	db, err := NewDatabase(sqlDriver, sqlSource)
	if err != nil {
		t.Errorf("NewDatabase error: %v", err)
		t.FailNow()
	}

	table, err := NewTable("test_table", TestTable{})
	if err != nil {
		t.Errorf("NewTable error: %v", err)
		t.FailNow()
	}

	if !db.CheckExistTable(table) {
		err = db.CreateTable(table)
		if err != nil {
			t.Errorf("db.CreateTable error: %v", err)
			t.FailNow()
		}
	}

	_, err = db.InsertValue(table, []TestTable{
		{ParamB: "a"},
		{ParamB: "b"},
		{ParamB: "c"},
	})
	if err != nil {
		t.Errorf("db.InsertValue chunk error: %v", err)
		t.FailNow()
	}

	_, err = db.InsertValue(table, TestTable{
		ParamB: "d",
	})
	if err != nil {
		t.Errorf("db.InsertValue single error: %v", err)
		t.FailNow()
	}

	responseIface, err := db.SelectValueById(table, 4)
	if err != nil {
		t.Errorf("db.SelectValueById error: %v", err)
		t.FailNow()
	}

	response, ok := responseIface.(TestTable)
	if !ok {
		t.Errorf("wrong type assertion responseIface to TestTable")
		t.FailNow()
	}

	if response.ParamA != 4 {
		t.Errorf("wrong ParamA")
		t.FailNow()
	}

	err = db.DropTable(table)
	if err != nil {
		t.Errorf("db.DropTable error: %v", err)
		t.FailNow()
	}
}

func TestReplaceValue(t *testing.T) {
	type TestTable struct {
		ParamA int64  `sql:"NAME=paramA, TYPE=INTEGER, PRIMARY_KEY, AUTO_INCREMENT"`
		ParamB string `sql:"NAME=paramB, TYPE=TEXT(32)"`
	}

	sqlDriver := "sqlite"
	sqlSource := "./test.db"

	db, err := NewDatabase(sqlDriver, sqlSource)
	if err != nil {
		t.Errorf("NewDatabase error: %v", err)
		t.FailNow()
	}

	table, err := NewTable("test_table", TestTable{})
	if err != nil {
		t.Errorf("NewTable error: %v", err)
		t.FailNow()
	}

	if !db.CheckExistTable(table) {
		err = db.CreateTable(table)
		if err != nil {
			t.Errorf("db.CreateTable error: %v", err)
			t.FailNow()
		}
	}

	_, err = db.InsertValue(table, []TestTable{
		{ParamB: "a"},
		{ParamB: "b"},
		{ParamB: "c"},
	})
	if err != nil {
		t.Errorf("db.InsertValue chunk error: %v", err)
		t.FailNow()
	}

	err = db.ReplaceValue(table, []TestTable{
		{ParamA: 1, ParamB: "a1"},
		{ParamA: 2, ParamB: "b1"},
		{ParamA: 3, ParamB: "c1"},
	})
	if err != nil {
		t.Errorf("db.ReplaceValue error: %v", err)
		t.FailNow()
	}

	responseIface, err := db.SelectValueById(table, 3)
	if err != nil {
		t.Errorf("db.SelectValueById error: %v", err)
		t.FailNow()
	}

	response, ok := responseIface.(TestTable)
	if !ok {
		t.Errorf("wrong type assertion responseIface to TestTable")
		t.FailNow()
	}

	if response.ParamA != 3 || response.ParamB != "c1" {
		t.Errorf("wrong ParamA")
		t.FailNow()
	}

	err = db.DropTable(table)
	if err != nil {
		t.Errorf("db.DropTable error: %v", err)
		t.FailNow()
	}
}

func TestUpdateValue(t *testing.T) {
	type TestTable struct {
		ParamA int64  `sql:"NAME=paramA, TYPE=INTEGER, PRIMARY_KEY, AUTO_INCREMENT"`
		ParamB string `sql:"NAME=paramB, TYPE=TEXT(32)"`
	}

	sqlDriver := "sqlite"
	sqlSource := "./test.db"

	db, err := NewDatabase(sqlDriver, sqlSource)
	if err != nil {
		t.Errorf("NewDatabase error: %v", err)
		t.FailNow()
	}

	table, err := NewTable("test_table", TestTable{})
	if err != nil {
		t.Errorf("NewTable error: %v", err)
		t.FailNow()
	}

	if !db.CheckExistTable(table) {
		err = db.CreateTable(table)
		if err != nil {
			t.Errorf("db.CreateTable error: %v", err)
			t.FailNow()
		}
	}

	_, err = db.InsertValue(table, []TestTable{
		{ParamB: "a"},
		{ParamB: "b"},
		{ParamB: "c"},
	})
	if err != nil {
		t.Errorf("db.InsertValue chunk error: %v", err)
		t.FailNow()
	}

	err = db.UpdateValue(table, []TestTable{
		{ParamA: 1, ParamB: "a1"},
		{ParamA: 2, ParamB: "b1"},
		{ParamA: 3, ParamB: "c1"},
	})
	if err != nil {
		t.Errorf("db.UpdateValue error: %v", err)
		t.FailNow()
	}

	responseIface, err := db.SelectValueById(table, 3)
	if err != nil {
		t.Errorf("db.SelectValueById error: %v", err)
		t.FailNow()
	}

	response, ok := responseIface.(TestTable)
	if !ok {
		t.Errorf("wrong type assertion responseIface to TestTable")
		t.FailNow()
	}

	if response.ParamA != 3 || response.ParamB != "c1" {
		t.Errorf("wrong ParamA")
		t.FailNow()
	}

	err = db.DropTable(table)
	if err != nil {
		t.Errorf("db.DropTable error: %v", err)
		t.FailNow()
	}
}

func TestDeleteValue(t *testing.T) {
	type TestTable struct {
		ParamA int64  `sql:"NAME=paramA, TYPE=INTEGER, PRIMARY_KEY, AUTO_INCREMENT"`
		ParamB string `sql:"NAME=paramB, TYPE=TEXT(32)"`
	}

	sqlDriver := "sqlite"
	sqlSource := "./test.db"

	db, err := NewDatabase(sqlDriver, sqlSource)
	if err != nil {
		t.Errorf("NewDatabase error: %v", err)
		t.FailNow()
	}

	table, err := NewTable("test_table", TestTable{})
	if err != nil {
		t.Errorf("NewTable error: %v", err)
		t.FailNow()
	}

	if !db.CheckExistTable(table) {
		err = db.CreateTable(table)
		if err != nil {
			t.Errorf("db.CreateTable error: %v", err)
			t.FailNow()
		}
	}

	_, err = db.InsertValue(table, []TestTable{
		{ParamB: "a"},
		{ParamB: "b"},
		{ParamB: "c"},
	})
	if err != nil {
		t.Errorf("db.InsertValue chunk error: %v", err)
		t.FailNow()
	}

	err = db.DeleteValue(table, []TestTable{
		{ParamA: 1, ParamB: "a1"},
		{ParamA: 2, ParamB: "b1"},
		{ParamA: 3, ParamB: "c1"},
	})
	if err != nil {
		t.Errorf("db.DeleteValue error: %v", err)
		t.FailNow()
	}

	count, err := db.GetCount(table)
	if err != nil {
		t.Errorf("db.SelectValueById error: %v", err)
		t.FailNow()
	}

	if count != 0 {
		t.Errorf("count != 0")
		t.FailNow()
	}

	err = db.DropTable(table)
	if err != nil {
		t.Errorf("db.DropTable error: %v", err)
		t.FailNow()
	}
}

func TestQuery(t *testing.T) {
	type TestTable struct {
		ParamA int64  `sql:"NAME=paramA, TYPE=INTEGER, PRIMARY_KEY, AUTO_INCREMENT"`
		ParamB string `sql:"NAME=paramB, TYPE=TEXT(32)"`
	}

	sqlDriver := "sqlite"
	sqlSource := "./test.db"

	db, err := NewDatabase(sqlDriver, sqlSource)
	if err != nil {
		t.Errorf("NewDatabase error: %v", err)
		t.FailNow()
	}

	table, err := NewTable("test_table", TestTable{})
	if err != nil {
		t.Errorf("NewTable error: %v", err)
		t.FailNow()
	}

	if !db.CheckExistTable(table) {
		err = db.CreateTable(table)
		if err != nil {
			t.Errorf("db.CreateTable error: %v", err)
			t.FailNow()
		}
	}

	_, err = db.InsertValue(table, []TestTable{
		{ParamB: "a"},
		{ParamB: "b"},
		{ParamB: "c"},
	})
	if err != nil {
		t.Errorf("db.InsertValue chunk error: %v", err)
		t.FailNow()
	}

	responseIface, err := db.Query(table, "select * from test_table where paramA = 3")
	if err != nil {
		t.Errorf("db.Query error: %v", err)
		t.FailNow()
	}

	response, ok := responseIface.([]TestTable)
	if !ok {
		t.Errorf("wrong type assertion responseIface to []TestTable")
		t.FailNow()
	}

	if response[0].ParamA != 3 || response[0].ParamB != "c" {
		t.Errorf("wrong item selected")
		t.FailNow()
	}

	err = db.DropTable(table)
	if err != nil {
		t.Errorf("db.DropTable error: %v", err)
		t.FailNow()
	}
}

func TestQuerySingle(t *testing.T) {
	type TestTable struct {
		ParamA int64  `sql:"NAME=paramA, TYPE=INTEGER, PRIMARY_KEY, AUTO_INCREMENT"`
		ParamB string `sql:"NAME=paramB, TYPE=TEXT(32)"`
	}

	sqlDriver := "sqlite"
	sqlSource := "./test.db"

	db, err := NewDatabase(sqlDriver, sqlSource)
	if err != nil {
		t.Errorf("NewDatabase error: %v", err)
		t.FailNow()
	}

	table, err := NewTable("test_table", TestTable{})
	if err != nil {
		t.Errorf("NewTable error: %v", err)
		t.FailNow()
	}

	if !db.CheckExistTable(table) {
		err = db.CreateTable(table)
		if err != nil {
			t.Errorf("db.CreateTable error: %v", err)
			t.FailNow()
		}
	}

	_, err = db.InsertValue(table, []TestTable{
		{ParamB: "a"},
		{ParamB: "b"},
		{ParamB: "c"},
	})
	if err != nil {
		t.Errorf("db.InsertValue chunk error: %v", err)
		t.FailNow()
	}

	responseIface, err := db.QuerySingle(table, "select * from test_table where paramA = 3")
	if err != nil {
		t.Errorf("db.QuerySingle error: %v", err)
		t.FailNow()
	}

	response, ok := responseIface.(TestTable)
	if !ok {
		t.Errorf("wrong type assertion responseIface to TestTable")
		t.FailNow()
	}

	if response.ParamA != 3 || response.ParamB != "c" {
		t.Errorf("wrong item selected")
		t.FailNow()
	}

	err = db.DropTable(table)
	if err != nil {
		t.Errorf("db.DropTable error: %v", err)
		t.FailNow()
	}
}

func TestQueryWithTable(t *testing.T) {
	type TestTable struct {
		ParamA int64  `sql:"NAME=paramA, TYPE=INTEGER, PRIMARY_KEY, AUTO_INCREMENT"`
		ParamB string `sql:"NAME=paramB, TYPE=TEXT(32)"`
	}

	sqlDriver := "sqlite"
	sqlSource := "./test.db"

	db, err := NewDatabase(sqlDriver, sqlSource)
	if err != nil {
		t.Errorf("NewDatabase error: %v", err)
		t.FailNow()
	}

	table, err := NewTable("test_table", TestTable{})
	if err != nil {
		t.Errorf("NewTable error: %v", err)
		t.FailNow()
	}

	if !db.CheckExistTable(table) {
		err = db.CreateTable(table)
		if err != nil {
			t.Errorf("db.CreateTable error: %v", err)
			t.FailNow()
		}
	}

	_, err = db.InsertValue(table, []TestTable{
		{ParamB: "a"},
		{ParamB: "b"},
		{ParamB: "c"},
	})
	if err != nil {
		t.Errorf("db.InsertValue chunk error: %v", err)
		t.FailNow()
	}

	responseIface, err := db.QueryWithTable(table, "select * from test_table where paramA = 3")
	if err != nil {
		t.Errorf("db.QueryWithTable error: %v", err)
		t.FailNow()
	}

	response, ok := responseIface.([]TestTable)
	if !ok {
		t.Errorf("wrong type assertion responseIface to []TestTable")
		t.FailNow()
	}

	if response[0].ParamA != 3 || response[0].ParamB != "c" {
		t.Errorf("wrong item selected")
		t.FailNow()
	}

	err = db.DropTable(table)
	if err != nil {
		t.Errorf("db.DropTable error: %v", err)
		t.FailNow()
	}
}

func TestQuerySingleWithTable(t *testing.T) {
	type TestTable struct {
		ParamA int64  `sql:"NAME=paramA, TYPE=INTEGER, PRIMARY_KEY, AUTO_INCREMENT"`
		ParamB string `sql:"NAME=paramB, TYPE=TEXT(32)"`
	}

	sqlDriver := "sqlite"
	sqlSource := "./test.db"

	db, err := NewDatabase(sqlDriver, sqlSource)
	if err != nil {
		t.Errorf("NewDatabase error: %v", err)
		t.FailNow()
	}

	table, err := NewTable("test_table", TestTable{})
	if err != nil {
		t.Errorf("NewTable error: %v", err)
		t.FailNow()
	}

	if !db.CheckExistTable(table) {
		err = db.CreateTable(table)
		if err != nil {
			t.Errorf("db.CreateTable error: %v", err)
			t.FailNow()
		}
	}

	_, err = db.InsertValue(table, []TestTable{
		{ParamB: "a"},
		{ParamB: "b"},
		{ParamB: "c"},
	})
	if err != nil {
		t.Errorf("db.InsertValue chunk error: %v", err)
		t.FailNow()
	}

	responseIface, err := db.QuerySingleWithTable(table, "select * from test_table where paramA = 3")
	if err != nil {
		t.Errorf("db.QuerySingleWithTable error: %v", err)
		t.FailNow()
	}

	response, ok := responseIface.(TestTable)
	if !ok {
		t.Errorf("wrong type assertion responseIface to TestTable")
		t.FailNow()
	}

	if response.ParamA != 3 || response.ParamB != "c" {
		t.Errorf("wrong item selected")
		t.FailNow()
	}

	err = db.DropTable(table)
	if err != nil {
		t.Errorf("db.DropTable error: %v", err)
		t.FailNow()
	}
}

func TestExec(t *testing.T) {
	type TestTable struct {
		ParamA int64  `sql:"NAME=paramA, TYPE=INTEGER, PRIMARY_KEY, AUTO_INCREMENT"`
		ParamB string `sql:"NAME=paramB, TYPE=TEXT(32)"`
	}

	sqlDriver := "sqlite"
	sqlSource := "./test.db"

	db, err := NewDatabase(sqlDriver, sqlSource)
	if err != nil {
		t.Errorf("NewDatabase error: %v", err)
		t.FailNow()
	}

	table, err := NewTable("test_table", TestTable{})
	if err != nil {
		t.Errorf("NewTable error: %v", err)
		t.FailNow()
	}

	if !db.CheckExistTable(table) {
		err = db.CreateTable(table)
		if err != nil {
			t.Errorf("db.CreateTable error: %v", err)
			t.FailNow()
		}
	}

	_, err = db.InsertValue(table, []TestTable{
		{ParamB: "a"},
		{ParamB: "b"},
		{ParamB: "c"},
	})
	if err != nil {
		t.Errorf("db.InsertValue chunk error: %v", err)
		t.FailNow()
	}

	err = db.Exec(func(db *DataBase, tx *sql.Tx) error {
		rows, err := tx.Query("select * from test_table where paramA = 3")
		if err != nil {
			return err
		}

		for rows.Next() {
			test := TestTable{}
			err = rows.Scan(&test.ParamA, &test.ParamB)
			if err != nil {
				return err
			}

			if test.ParamA != 3 || test.ParamB != "c" {
				t.Errorf("wrong value received")
				t.FailNow()
			}
		}

		return nil
	})
	if err != nil {
		t.Errorf("db.Exec error: %v", err)
		t.FailNow()
	}

	err = db.DropTable(table)
	if err != nil {
		t.Errorf("db.DropTable error: %v", err)
		t.FailNow()
	}
}

func TestExecWithTable(t *testing.T) {
	type TestTable struct {
		ParamA int64  `sql:"NAME=paramA, TYPE=INTEGER, PRIMARY_KEY, AUTO_INCREMENT"`
		ParamB string `sql:"NAME=paramB, TYPE=TEXT(32)"`
	}

	sqlDriver := "sqlite"
	sqlSource := "./test.db"

	db, err := NewDatabase(sqlDriver, sqlSource)
	if err != nil {
		t.Errorf("NewDatabase error: %v", err)
		t.FailNow()
	}

	table, err := NewTable("test_table", TestTable{})
	if err != nil {
		t.Errorf("NewTable error: %v", err)
		t.FailNow()
	}

	if !db.CheckExistTable(table) {
		err = db.CreateTable(table)
		if err != nil {
			t.Errorf("db.CreateTable error: %v", err)
			t.FailNow()
		}
	}

	_, err = db.InsertValue(table, []TestTable{
		{ParamB: "a"},
		{ParamB: "b"},
		{ParamB: "c"},
	})
	if err != nil {
		t.Errorf("db.InsertValue chunk error: %v", err)
		t.FailNow()
	}

	err = db.ExecWithTable(table, func(db *DataBase, tx *sql.Tx, table *Table) error {
		rows, err := tx.Query("select * from test_table where paramA = 3")
		if err != nil {
			return err
		}

		for rows.Next() {
			test := TestTable{}
			err = rows.Scan(&test.ParamA, &test.ParamB)
			if err != nil {
				return err
			}

			if test.ParamA != 3 || test.ParamB != "c" {
				t.Errorf("wrong value received")
				t.FailNow()
			}
		}

		return nil
	})
	if err != nil {
		t.Errorf("db.ExecWithTable error: %v", err)
		t.FailNow()
	}

	err = db.DropTable(table)
	if err != nil {
		t.Errorf("db.DropTable error: %v", err)
		t.FailNow()
	}
}

func TestForEach(t *testing.T) {
	type User struct {
		ID   int64  `sql:"NAME=id, TYPE=INTEGER, PRIMARY_KEY, AUTO_INCREMENT"`
		Name string `sql:"NAME=name, TYPE=TEXT(32)"`
	}

	sqlDriver := "sqlite"
	sqlSource := "./test.db"

	err := os.Remove(sqlSource)
	if err != nil {
		t.Errorf("os.Remove error: %v", err)
		t.FailNow()
	}

	db, err := NewDatabase(sqlDriver, sqlSource)
	if err != nil {
		t.Errorf("NewDatabase err: %v", err)
		t.FailNow()
	}

	// creating a table with the name
	userTable, err := db.NewTable(12, "users", User{})
	if err != nil {
		t.Errorf("db.NewTable err: %v", err)
		t.FailNow()
	}

	if !db.CheckExistTable(userTable) {
		err = db.CreateTable(userTable)
		if err != nil {
			t.Errorf("db.CreateTable err: %v", err)
			t.FailNow()
		}
	}

	_, err = db.InsertValue(userTable, []User{
		{Name: "test1"},
		{Name: "test1"},
		{Name: "test1"},
		{Name: "test1"},
		{Name: "test1"},
		{Name: "test1"},
	})
	if err != nil {
		t.Errorf("db.InsertValue err: %v", err)
		t.FailNow()
	}

	newName := "test2"
	err = db.ForEach(userTable, func(index int64, object interface{}) error {
		user, ok := object.(User)
		if !ok {
			return errors.New("wrong type assert")
		}

		user.Name = newName

		return db.UpdateValue(userTable, user)
	}, 3)

	if err != nil {
		t.Errorf("db.ForEach err: %v", err)
		t.FailNow()
	}

	respIface, err := db.SelectAll(userTable)
	if err != nil {
		t.Errorf("db.SelectAll err: %v", err)
		t.FailNow()
	}

	users, ok := respIface.([]User)
	if !ok {
		t.Errorf("wrong type assert")
		t.FailNow()
	}

	if len(users) != 6 {
		t.Errorf("len(users) != 6")
		t.FailNow()
	}

	for _, v := range users {
		if v.Name != newName {
			t.Errorf("v.Name != newName")
			t.FailNow()
		}
	}
}

// Common test ////////////////////////////////////////////////////////////////

// Test newly created database + migration
func TestNewDatabaseWithMigration(t *testing.T) {
	type User struct {
		ID   int64  `sql:"NAME=id, TYPE=INTEGER, PRIMARY_KEY, AUTO_INCREMENT"`
		Name string `sql:"NAME=name, TYPE=TEXT(32)"`
		// Surname string `sql:"NAME=surname, TYPE=TEXT(32)"`
		// Another string `sql:"NAME=another, TYPE=TEXT(32)"`
	}

	sqlDriver := "sqlite"
	sqlSource := "./test.db"

	err := os.Remove(sqlSource)
	if err != nil {
		t.Errorf("os.Remove error: %v", err)
		t.FailNow()
	}

	db, err := NewDatabase(sqlDriver, sqlSource)
	if err != nil {
		t.Errorf("NewDatabase error: %v", err)
		t.FailNow()
	}

	userTable, err := db.NewTable(11, "users", User{})
	if err != nil {
		t.Errorf("db.NewTable error: %v", err)
		t.FailNow()
	}

	if !db.CheckExistTable(userTable) {
		err = db.CreateTable(userTable)
		if err != nil {
			t.Errorf("db.NewTable error: %v", err)
			t.FailNow()
		}
	}

	lastId, err := db.InsertValue(userTable, User{
		Name: "test1",
		// Surname: "test2",
		// Another: "test3",
	})
	if err != nil {
		t.Errorf("db.InsertValue error: %v", err)
		t.FailNow()
	}

	respIface, err := db.SelectValue(userTable, fmt.Sprintf("ID = %d", lastId))
	if err != nil {
		t.Errorf("db.SelectValue error: %v", err)
		t.FailNow()
	}

	if respIface.([]User)[0].ID != lastId {
		t.Errorf("respIface.(User).ID != lastId")
		t.FailNow()
	}
}

// Test diffrent migration number && changing of migration number in table
func TestNewMigrationNumber(t *testing.T) {
	type User struct {
		ID   int64  `sql:"NAME=id, TYPE=INTEGER, PRIMARY_KEY, AUTO_INCREMENT"`
		Name string `sql:"NAME=name, TYPE=TEXT(32)"`
		// Surname string `sql:"NAME=surname, TYPE=TEXT(32)"`
		// Another string `sql:"NAME=another, TYPE=TEXT(32)"`
	}

	sqlDriver := "sqlite"
	sqlSource := "./test.db"

	err := os.Remove(sqlSource)
	if err != nil {
		t.Errorf("os.Remove error: %v", err)
		t.FailNow()
	}

	db, err := NewDatabase(sqlDriver, sqlSource)
	if err != nil {
		t.Errorf("NewDatabase error: %v", err)
		t.FailNow()
	}

	var num int64 = 1
	userTable, err := db.NewTable(num, "users", User{})
	if err != nil {
		t.Errorf("db.NewTable error: %v", err)
		t.FailNow()
	}

	migrationNumber, err := db.getTableMigrationNumber(userTable)
	if err != nil {
		t.Errorf("db.getTableMigrationNumber error: %v", err)
		t.FailNow()
	}

	if migrationNumber != num {
		t.Errorf("migrationNumber != num (1)")
		t.FailNow()
	}

	num = 2
	userTable, err = db.NewTable(num, "users", User{})
	if err != nil {
		t.Errorf("db.NewTable error: %v", err)
		t.FailNow()
	}

	migrationNumber, err = db.getTableMigrationNumber(userTable)
	if err != nil {
		t.Errorf("db.getTableMigrationNumber error: %v", err)
		t.FailNow()
	}

	if migrationNumber != num {
		t.Errorf("migrationNumber != num (2)")
		t.FailNow()
	}

	num = 2
	userTable, err = db.NewTable(num, "users", User{})
	if err != nil {
		t.Errorf("db.NewTable error: %v", err)
		t.FailNow()
	}

	migrationNumber, err = db.getTableMigrationNumber(userTable)
	if err != nil {
		t.Errorf("db.getTableMigrationNumber error: %v", err)
		t.FailNow()
	}

	if migrationNumber != num {
		t.Errorf("migrationNumber != num (2)")
		t.FailNow()
	}

	num = 1
	_, err = db.NewTable(num, "users", User{})
	if err == nil {
		t.Errorf("err == nil")
		t.FailNow()
	}

	num = 0
	_, err = db.NewTable(num, "users", User{})
	if err != nil {
		t.Errorf("db.NewTable error: %v", err)
		t.FailNow()
	}
}

// Test Up migrations

func TestUpMigration(t *testing.T) {
	name1 := "name1"
	a := struct {
		ID   int64  `sql:"NAME=id, TYPE=INTEGER, PRIMARY_KEY, AUTO_INCREMENT"`
		Name string `sql:"NAME=name, TYPE=TEXT(32)"`
	}{
		Name: name1,
	}

	sqlDriver := "sqlite"
	sqlSource := "./test.db"

	err := os.Remove(sqlSource)
	if err != nil {
		t.Errorf("os.Remove error: %v", err)
		t.FailNow()
	}

	db, err := NewDatabase(sqlDriver, sqlSource)
	if err != nil {
		t.Errorf("NewDatabase error: %v", err)
		t.FailNow()
	}

	var num int64 = 1
	aTable, err := db.NewTable(num, "a", a)
	if err != nil {
		t.Errorf("db.NewTable error: %v", err)
		t.FailNow()
	}

	migrationNumber, err := db.getTableMigrationNumber(aTable)
	if err != nil {
		t.Errorf("db.getTableMigrationNumber error: %v", err)
		t.FailNow()
	}

	if migrationNumber != num {
		t.Errorf("migrationNumber != num (1)")
		t.FailNow()
	}

	lastId, err := db.InsertValue(aTable, a)
	if err != nil {
		t.Errorf("db.InsertValue error: %v", err)
		t.FailNow()
	}

	if lastId != num {
		t.Errorf("lastId != num")
		t.FailNow()
	}

	respIface, err := db.SelectValueById(aTable, lastId)
	if err != nil {
		t.Errorf("db.SelectValueById error: %v", err)
		t.FailNow()
	}

	resp, ok := respIface.(struct {
		ID   int64  `sql:"NAME=id, TYPE=INTEGER, PRIMARY_KEY, AUTO_INCREMENT"`
		Name string `sql:"NAME=name, TYPE=TEXT(32)"`
	})
	if !ok {
		t.Errorf("wrong type assert respIface to anon struct type")
		t.FailNow()
	}

	if resp.Name != name1 {
		t.Errorf("resp.Name != name1")
		t.FailNow()
	}

	name2 := "name2"
	surname2 := "surname2"

	a2 := struct {
		ID      int64  `sql:"NAME=id, TYPE=INTEGER, PRIMARY_KEY, AUTO_INCREMENT"`
		Name    string `sql:"NAME=name, TYPE=TEXT(32)"`
		Surname string `sql:"NAME=surname, TYPE=TEXT(32)"`
	}{
		Name:    name2,
		Surname: surname2,
	}

	num = 2
	aTable, err = db.NewTable(num, "a", a2)
	if err != nil {
		t.Errorf("db.NewTable error: %v", err)
		t.FailNow()
	}

	migrationNumber, err = db.getTableMigrationNumber(aTable)
	if err != nil {
		t.Errorf("db.getTableMigrationNumber error: %v", err)
		t.FailNow()
	}

	if migrationNumber != num {
		t.Errorf("migrationNumber != num (2)")
		t.FailNow()
	}

	lastId, err = db.InsertValue(aTable, a2)
	if err != nil {
		t.Errorf("db.InsertValue error: %v", err)
		t.FailNow()
	}

	if lastId != num {
		t.Errorf("lastId != num")
		t.FailNow()
	}

	respIface, err = db.SelectValueById(aTable, lastId)
	if err != nil {
		t.Errorf("db.SelectValueById error: %v", err)
		t.FailNow()
	}

	resp2, ok := respIface.(struct {
		ID      int64  `sql:"NAME=id, TYPE=INTEGER, PRIMARY_KEY, AUTO_INCREMENT"`
		Name    string `sql:"NAME=name, TYPE=TEXT(32)"`
		Surname string `sql:"NAME=surname, TYPE=TEXT(32)"`
	})
	if !ok {
		t.Errorf("wrong type assert respIface to anon struct type")
		t.FailNow()
	}

	if resp2.Name != name2 || resp2.Surname != surname2 {
		t.Errorf("resp2.Name != name2 || resp2.Surname != surname2")
		t.FailNow()
	}

	name3 := "name3"
	surname3 := "surname3"
	another3 := "another3"

	a3 := struct {
		ID      int64  `sql:"NAME=id, TYPE=INTEGER, PRIMARY_KEY, AUTO_INCREMENT"`
		Name    string `sql:"NAME=name, TYPE=TEXT(32)"`
		Surname string `sql:"NAME=surname, TYPE=TEXT(32)"`
		Another string `sql:"NAME=another, TYPE=TEXT(32)"`
	}{
		Name:    name3,
		Surname: surname3,
		Another: another3,
	}

	num = 3
	aTable, err = db.NewTable(num, "a", a3)
	if err != nil {
		t.Errorf("db.NewTable error: %v", err)
		t.FailNow()
	}

	migrationNumber, err = db.getTableMigrationNumber(aTable)
	if err != nil {
		t.Errorf("db.getTableMigrationNumber error: %v", err)
		t.FailNow()
	}

	if migrationNumber != num {
		t.Errorf("migrationNumber != num (3)")
		t.FailNow()
	}

	lastId, err = db.InsertValue(aTable, a3)
	if err != nil {
		t.Errorf("db.InsertValue error: %v", err)
		t.FailNow()
	}

	if lastId != num {
		t.Errorf("lastId != num")
		t.FailNow()
	}

	respIface, err = db.SelectValueById(aTable, lastId)
	if err != nil {
		t.Errorf("db.SelectValueById error: %v", err)
		t.FailNow()
	}

	resp3, ok := respIface.(struct {
		ID      int64  `sql:"NAME=id, TYPE=INTEGER, PRIMARY_KEY, AUTO_INCREMENT"`
		Name    string `sql:"NAME=name, TYPE=TEXT(32)"`
		Surname string `sql:"NAME=surname, TYPE=TEXT(32)"`
		Another string `sql:"NAME=another, TYPE=TEXT(32)"`
	})
	if !ok {
		t.Errorf("wrong type assert respIface to anon struct type")
		t.FailNow()
	}

	if resp3.Name != name3 || resp3.Surname != surname3 || resp3.Another != another3 {
		t.Errorf("resp3.Name != name3 || resp3.Surname != surname3 || resp3.Another != another3")
		t.FailNow()
	}
}

// Test Down migrations
func TestDownMigration(t *testing.T) {
	sqlDriver := "sqlite"
	sqlSource := "./test.db"

	err := os.Remove(sqlSource)
	if err != nil {
		t.Errorf("os.Remove error: %v", err)
		t.FailNow()
	}

	db, err := NewDatabase(sqlDriver, sqlSource)
	if err != nil {
		t.Errorf("NewDatabase error: %v", err)
		t.FailNow()
	}

	name3 := "name3"
	surname3 := "surname3"
	another3 := "another3"

	a3 := struct {
		ID      int64  `sql:"NAME=id, TYPE=INTEGER, PRIMARY_KEY, AUTO_INCREMENT"`
		Name    string `sql:"NAME=name, TYPE=TEXT(32)"`
		Surname string `sql:"NAME=surname, TYPE=TEXT(32)"`
		Another string `sql:"NAME=another, TYPE=TEXT(32)"`
	}{
		Name:    name3,
		Surname: surname3,
		Another: another3,
	}

	var num int64 = 1
	aTable, err := db.NewTable(num, "a", a3)
	if err != nil {
		t.Errorf("db.NewTable error: %v", err)
		t.FailNow()
	}

	migrationNumber, err := db.getTableMigrationNumber(aTable)
	if err != nil {
		t.Errorf("db.getTableMigrationNumber error: %v", err)
		t.FailNow()
	}

	if migrationNumber != num {
		t.Errorf("migrationNumber != num (3)")
		t.FailNow()
	}

	lastId, err := db.InsertValue(aTable, a3)
	if err != nil {
		t.Errorf("db.InsertValue error: %v", err)
		t.FailNow()
	}

	if lastId != num {
		t.Errorf("lastId != num")
		t.FailNow()
	}

	respIface, err := db.SelectValueById(aTable, lastId)
	if err != nil {
		t.Errorf("db.SelectValueById error: %v", err)
		t.FailNow()
	}

	resp3, ok := respIface.(struct {
		ID      int64  `sql:"NAME=id, TYPE=INTEGER, PRIMARY_KEY, AUTO_INCREMENT"`
		Name    string `sql:"NAME=name, TYPE=TEXT(32)"`
		Surname string `sql:"NAME=surname, TYPE=TEXT(32)"`
		Another string `sql:"NAME=another, TYPE=TEXT(32)"`
	})
	if !ok {
		t.Errorf("wrong type assert respIface to anon struct type")
		t.FailNow()
	}

	if resp3.Name != name3 || resp3.Surname != surname3 || resp3.Another != another3 {
		t.Errorf("resp3.Name != name3 || resp3.Surname != surname3 || resp3.Another != another3")
		t.FailNow()
	}

	name2 := "name2"
	surname2 := "surname2"

	a2 := struct {
		ID      int64  `sql:"NAME=id, TYPE=INTEGER, PRIMARY_KEY, AUTO_INCREMENT"`
		Name    string `sql:"NAME=name, TYPE=TEXT(32)"`
		Surname string `sql:"NAME=surname, TYPE=TEXT(32)"`
	}{
		Name:    name2,
		Surname: surname2,
	}

	num = 2
	aTable, err = db.NewTable(num, "a", a2)
	if err != nil {
		t.Errorf("db.NewTable error: %v", err)
		t.FailNow()
	}

	migrationNumber, err = db.getTableMigrationNumber(aTable)
	if err != nil {
		t.Errorf("db.getTableMigrationNumber error: %v", err)
		t.FailNow()
	}

	if migrationNumber != num {
		t.Errorf("migrationNumber != num (2)")
		t.FailNow()
	}

	lastId, err = db.InsertValue(aTable, a2)
	if err != nil {
		t.Errorf("db.InsertValue error: %v", err)
		t.FailNow()
	}

	if lastId != num {
		t.Errorf("lastId != num")
		t.FailNow()
	}

	respIface, err = db.SelectValueById(aTable, lastId)
	if err != nil {
		t.Errorf("db.SelectValueById error: %v", err)
		t.FailNow()
	}

	resp2, ok := respIface.(struct {
		ID      int64  `sql:"NAME=id, TYPE=INTEGER, PRIMARY_KEY, AUTO_INCREMENT"`
		Name    string `sql:"NAME=name, TYPE=TEXT(32)"`
		Surname string `sql:"NAME=surname, TYPE=TEXT(32)"`
	})
	if !ok {
		t.Errorf("wrong type assert respIface to anon struct type")
		t.FailNow()
	}

	if resp2.Name != name2 || resp2.Surname != surname2 {
		t.Errorf("resp2.Name != name2 || resp2.Surname != surname2")
		t.FailNow()
	}

	name1 := "name1"
	a := struct {
		ID   int64  `sql:"NAME=id, TYPE=INTEGER, PRIMARY_KEY, AUTO_INCREMENT"`
		Name string `sql:"NAME=name, TYPE=TEXT(32)"`
	}{
		Name: name1,
	}

	num = 3
	aTable, err = db.NewTable(num, "a", a)
	if err != nil {
		t.Errorf("db.NewTable error: %v", err)
		t.FailNow()
	}

	migrationNumber, err = db.getTableMigrationNumber(aTable)
	if err != nil {
		t.Errorf("db.getTableMigrationNumber error: %v", err)
		t.FailNow()
	}

	if migrationNumber != num {
		t.Errorf("migrationNumber != num (1)")
		t.FailNow()
	}

	lastId, err = db.InsertValue(aTable, a)
	if err != nil {
		t.Errorf("db.InsertValue error: %v", err)
		t.FailNow()
	}

	if lastId != num {
		t.Errorf("lastId != num")
		t.FailNow()
	}

	respIface, err = db.SelectValueById(aTable, lastId)
	if err != nil {
		t.Errorf("db.SelectValueById error: %v", err)
		t.FailNow()
	}

	resp, ok := respIface.(struct {
		ID   int64  `sql:"NAME=id, TYPE=INTEGER, PRIMARY_KEY, AUTO_INCREMENT"`
		Name string `sql:"NAME=name, TYPE=TEXT(32)"`
	})
	if !ok {
		t.Errorf("wrong type assert respIface to anon struct type")
		t.FailNow()
	}

	if resp.Name != name1 {
		t.Errorf("resp.Name != name1")
		t.FailNow()
	}
}
