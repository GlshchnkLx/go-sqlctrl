package sqlctrl

import (
	"slices"
	"testing"
)

func TestNewTable(t *testing.T) {
	type TestTable struct {
		ParamA int64  `sql:"NAME=paramA, TYPE=INTEGER, PRIMARY_KEY, AUTO_INCREMENT"`
		ParamB string `sql:"NAME=paramB, TYPE=TEXT(32)"`
	}

	tableName := "test_table"

	table, err := NewTable(tableName, TestTable{})
	if err != nil {
		t.Errorf("NewTable error: %v", err)
		t.FailNow()
	}

	if !slices.Contains(table.FieldNameArray, "ParamA") {
		t.Errorf("table.FieldNameArray not Contain ParamA")
	}

	if !slices.Contains(table.FieldNameArray, "ParamB") {
		t.Errorf("table.FieldNameArray not Contain ParamB")
	}

	if table.SqlName != tableName {
		t.Errorf("table.SqlName != tableName")
	}

	a, ok := table.FieldMap["ParamA"]
	if !ok {
		t.Errorf("ParamA is not present in table.FieldMap")
		t.FailNow()
	}

	if a.SqlName != "paramA" {
		t.Errorf("a.SqlName != paramA")
	}

	if a.SqlType != "INTEGER" {
		t.Errorf("a.SqlType != INTEGER")
	}

	if !a.IsAutoIncrement {
		t.Errorf("a.IsAutoIncrement != true")
	}

	if !a.IsPrimaryKey {
		t.Errorf("a.IsPrimaryKey != true")
	}

	b, ok := table.FieldMap["ParamB"]
	if !ok {
		t.Errorf("ParamB is not present in table.FieldMap")
		t.FailNow()
	}

	if b.SqlName != "paramB" {
		t.Errorf("b.SqlName != paramB")
	}

	if b.SqlType != "TEXT(32)" {
		t.Errorf("b.SqlType != TEXT(32)")
	}

	if b.IsAutoIncrement || b.IsNotNull || b.IsPrimaryKey || b.IsUnique {
		t.Errorf("b has wrong field")
	}
}
