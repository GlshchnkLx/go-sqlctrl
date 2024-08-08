package sqlctrl

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
)

//--------------------------------------------------------------------------------//

type Scheme interface {
	SchemeRegister(*Database) error

	Import() error
	Export() error
	Migration(*Table, *SchemeStorageTable, *SchemeStorageTable) error

	RegisterTable(string, interface{}) (*Table, error)
}

//--------------------------------------------------------------------------------//

type SchemeStorageV1 struct {
	SchemeHeader  string `sql:"NAME=scheme_header | NOT_NULL" json:"-"`
	SchemeVersion int64  `sql:"NAME=scheme_version | NOT_NULL" json:"-"`

	LocalTableName  string `sql:"NAME=local_table_name" json:"-"`
	RemoteTableName string `sql:"NAME=remote_table_name | TYPE=VARCHAR(256) | PRIMARY_KEY"`

	LocalFieldName  string `sql:"NAME=local_field_name" json:"-"`
	RemoteFieldName string `sql:"NAME=remote_field_name | TYPE=VARCHAR(256) | PRIMARY_KEY"`

	LocalFieldType  string `sql:"NAME=local_field_type" json:"-"`
	RemoteFieldType string `sql:"NAME=remote_field_type"`

	FieldIndex           int     `sql:"NAME=field_index"`
	FieldIsPrimaryKey    bool    `sql:"NAME=field_is_primarykey"`
	FieldIsAutoIncrement bool    `sql:"NAME=field_is_autoincrement"`
	FieldIsNotNull       bool    `sql:"NAME=field_is_notnull"`
	FieldInUniqueGroup   *string `sql:"NAME=field_in_uniquegroup"`
	FieldValueDefault    *string `sql:"NAME=field_value_default"`
	FieldValueCheck      *string `sql:"NAME=field_value_check"`
}

type SchemeStorageStable SchemeStorageV1

type SchemeStorageTable struct {
	SchemeHeader  string
	SchemeVersion int64

	LocalTableName  string
	RemoteTableName string

	FieldMap map[string]*SchemeStorageStable
}

//--------------------------------------------------------------------------------//

func (fieldUnit *SchemeStorageStable) GetHash() string {
	tableJson, err := json.Marshal(fieldUnit)
	if err != nil {
		return ""
	}

	tableHash := md5.Sum(tableJson)
	return hex.EncodeToString(tableHash[:])
}

func (fieldMap *SchemeStorageTable) GetHash() string {
	tableJson, err := json.Marshal(fieldMap)
	if err != nil {
		return ""
	}

	tableHash := md5.Sum(tableJson)
	return hex.EncodeToString(tableHash[:])
}

//--------------------------------------------------------------------------------//

func SchemeHelperTableToFieldMap(schemeHeader string, schemeVersion int64, table *Table) (*SchemeStorageTable, error) {
	storageTable := SchemeStorageTable{
		SchemeHeader:  schemeHeader,
		SchemeVersion: schemeVersion,

		LocalTableName:  table.GetGoName(),
		RemoteTableName: table.GetSqlName(),

		FieldMap: map[string]*SchemeStorageStable{},
	}

	if table == nil {
		return nil, ErrorTableIsNil
	}

	for _, fieldSqlName := range table.GetSqlFieldNameArray() {
		tableField := table.GetFieldBySqlName(fieldSqlName)

		storageTable.FieldMap[tableField.GetSqlName()] = &SchemeStorageStable{
			SchemeHeader:  schemeHeader,
			SchemeVersion: schemeVersion,

			LocalTableName:  table.GetGoName(),
			RemoteTableName: table.GetSqlName(),

			LocalFieldName:  tableField.GetGoName(),
			RemoteFieldName: tableField.GetSqlName(),

			LocalFieldType:  tableField.GetGoType().String(),
			RemoteFieldType: tableField.GetSqlType(),

			FieldIndex:           tableField.GetSqlIndex(),
			FieldIsPrimaryKey:    tableField.IsPrimaryKey(),
			FieldIsAutoIncrement: tableField.IsAutoIncrement(),
			FieldIsNotNull:       tableField.IsNotNull(),
			FieldInUniqueGroup:   tableField.InUniqueGroup(),
			FieldValueDefault:    tableField.ValueDefault(),
			FieldValueCheck:      tableField.ValueCheck(),
		}
	}
	return &storageTable, nil
}

//--------------------------------------------------------------------------------//
