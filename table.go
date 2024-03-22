package sqlctrl

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"html"
	"reflect"
	"strings"
)

//--------------------------------------------------------------------------------//

type TableField struct {
	GoName string       `json:"goName"`
	GoType reflect.Kind `json:"-"`

	SqlName string `json:"sqlName"`
	SqlType string `json:"sqlType"`

	IsPrimaryKey    bool `json:"isPrimaryKey"`
	IsAutoIncrement bool `json:"isAutoIncrement"`
	IsUnique        bool `json:"isUnique"`
	IsNotNull       bool `json:"isNotNull"`

	ValueDefault *string `json:"valueDefault"`
	ValueCheck   *string `json:"valueCheck"`
}

func (field *TableField) GetHash() string {
	tableJson, err := json.Marshal(field)
	if err != nil {
		return ""
	}

	tableHash := md5.Sum(tableJson)
	return hex.EncodeToString(tableHash[:])
}

// Parses given struct field. Return value is true if operation was successful.
// This method tries to parse struct field tag at first. If NAME or TYPE was
// not found in tag then it retrives appropriate info from field throught
// reflection.
func (field *TableField) ReflectParse(reflectStructField reflect.StructField) bool {
	reflectStructFieldTag := reflectStructField.Tag

	sqlTagString, ok := reflectStructFieldTag.Lookup("sql")
	if !ok {
		return false
	}

	sqlTagSlice := strings.Split(sqlTagString, ",")
	for sqlTagIndex, sqlTagOption := range sqlTagSlice {
		sqlTagOption = strings.Trim(sqlTagOption, " ")
		sqlTagSlice[sqlTagIndex] = "--" + sqlTagOption
	}

	fs := flag.NewFlagSet("", flag.ContinueOnError)
	fs.Usage = func() {}

	flagName := fs.String("NAME", field.GoName, "name")
	flagType := fs.String("TYPE", "", "type")
	flagPrimaryKey := fs.Bool("PRIMARY_KEY", false, "primary_key")
	flagAutoIncrement := fs.Bool("AUTO_INCREMENT", false, "auto_increment")
	flagUnique := fs.Bool("UNIQUE", false, "unique")
	flagNotNull := fs.Bool("NOT_NULL", false, "not_null")
	flagDefault := fs.String("DEFAULT", "", "default")
	flagCheck := fs.String("CHECK", "", "check")

	fs.Parse(sqlTagSlice)

	if *flagName != "" {
		field.SqlName = *flagName
	}

	if *flagType == "" {
		fieldGoType := field.GoType

		if field.GoType == reflect.Ptr {
			fieldGoType = reflectStructField.Type.Elem().Kind()
		}

		switch fieldGoType {
		case reflect.Bool:
			field.SqlType = "INTEGER(1)"
		case reflect.Uint, reflect.Int:
			field.SqlType = "INTEGER(8)"
		case reflect.Uint8, reflect.Int8:
			field.SqlType = "INTEGER(1)"
		case reflect.Uint16, reflect.Int16:
			field.SqlType = "INTEGER(2)"
		case reflect.Uint32, reflect.Int32:
			field.SqlType = "INTEGER(4)"
		case reflect.Uint64, reflect.Int64:
			field.SqlType = "INTEGER(8)"
		case reflect.Float32, reflect.Float64:
			field.SqlType = "REAL"
		case reflect.String:
			field.SqlType = "TEXT(4096)"
		default:
			panic(fieldGoType)
		}
	} else {
		field.SqlType = *flagType
	}

	field.IsPrimaryKey = *flagPrimaryKey
	field.IsAutoIncrement = *flagAutoIncrement
	field.IsUnique = *flagUnique
	field.IsNotNull = *flagNotNull

	if *flagDefault != "" {
		field.ValueDefault = flagDefault
	}

	if *flagCheck != "" {
		field.ValueCheck = flagCheck
	}

	return true
}

//--------------------------------------------------------------------------------//

type Table struct {
	GoName string       `json:"goName"`
	GoType reflect.Type `json:"-"`

	SqlName string `json:"sqlName"`

	FieldNameArray []string               `json:"fieldNameArray"`
	FieldMap       map[string]*TableField `json:"fieldMap"`

	AutoIncrement *TableField `json:"autoIncrement"`
}

func (table *Table) GetHash() string {
	tableJson, err := json.Marshal(table)
	if err != nil {
		return ""
	}

	tableHash := md5.Sum(tableJson)
	return hex.EncodeToString(tableHash[:])
}

func (table *Table) GetStruct(tableStruct interface{}) (tableStructPtr interface{}, fieldArrayPtr []interface{}, err error) {
	tableReflectType := table.GoType
	var tableReflectValue reflect.Value

	if tableStruct == nil {
		tableReflectValue = reflect.New(tableReflectType).Elem()
	} else {
		switch reflect.TypeOf(tableStruct).Kind() {
		case reflect.Ptr:
			tableReflectValue = reflect.ValueOf(tableStruct).Elem()
		case reflect.Struct:
			tableReflectValue = reflect.New(tableReflectType).Elem()
			tableReflectValue.Set(reflect.ValueOf(tableStruct))
		default:
			err = ErrValueMustBeAStructureOrPointer
			return
		}

		if table.GoType != tableReflectValue.Type() {
			err = fmt.Errorf("tables do not match")
			return
		}
	}

	tableStructPtr = tableReflectValue.Addr().Interface()

	for _, fieldName := range table.FieldNameArray {
		fieldReflectValue := tableReflectValue.FieldByName(fieldName)
		fieldArrayPtr = append(fieldArrayPtr, fieldReflectValue.Addr().Interface())
	}

	return
}

//--------------------------------------------------------------------------------//

func (table *Table) convertInterfaceToInterfaceArray(value interface{}) (valueArray []interface{}, err error) {
	valueReflectType := reflect.TypeOf(value)
	valueReflectValue := reflect.ValueOf(value)

	if value == nil {
		err = fmt.Errorf("no value")
		return
	}

	switch valueReflectType.Kind() {
	case reflect.Struct:
		if table.GoType != valueReflectType {
			err = ErrValueDoesNotMatchTableType
			return
		}

		valueArray = append(valueArray, value)
	case reflect.Array, reflect.Slice:
		if valueReflectValue.Len() == 0 {
			err = fmt.Errorf("no value")
			return
		}

		for i := 0; i < valueReflectValue.Len(); i++ {
			if table.GoType != valueReflectValue.Index(i).Type() {
				err = ErrValueDoesNotMatchTableType
				return
			}

			valueArray = append(valueArray, valueReflectValue.Index(i).Interface())
		}
	}

	return
}

//--------------------------------------------------------------------------------//

func (table *Table) sqlCreateTable() (request []string, err error) {
	sqlDeclarationArray := []string{}

	for _, fieldName := range table.FieldNameArray {
		tableField := table.FieldMap[fieldName]

		sqlDeclarationField := fmt.Sprintf("%s %s", tableField.SqlName, tableField.SqlType)

		if tableField.IsPrimaryKey {
			sqlDeclarationField = fmt.Sprintf("%s PRIMARY_KEY", sqlDeclarationField)
		}

		if tableField.IsAutoIncrement {
			sqlDeclarationField = fmt.Sprintf("%s AUTO_INCREMENT", sqlDeclarationField)
		}

		if tableField.IsNotNull {
			sqlDeclarationField = fmt.Sprintf("%s NOT_NULL", sqlDeclarationField)
		}

		if tableField.ValueDefault != nil && *tableField.ValueDefault != "" {
			sqlDeclarationField = fmt.Sprintf("%s DEFAULT %s", sqlDeclarationField, *tableField.ValueDefault)
		}

		sqlDeclarationArray = append(sqlDeclarationArray, sqlDeclarationField)
	}

	for _, fieldName := range table.FieldNameArray {
		tableField := table.FieldMap[fieldName]

		if tableField.IsUnique {
			sqlDeclarationArray = append(sqlDeclarationArray, fmt.Sprintf("CONSTRAINT %s_%s_uq UNIQUE(%s)", table.SqlName, tableField.SqlName, tableField.SqlName))
		}

		if tableField.ValueCheck != nil && *tableField.ValueCheck != "" {
			sqlDeclarationArray = append(sqlDeclarationArray, fmt.Sprintf("CONSTRAINT %s_%s_ck CHECK(%s)", table.SqlName, tableField.SqlName, *tableField.ValueCheck))
		}
	}

	request = append(request, fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s` (%s);", table.SqlName, strings.Join(sqlDeclarationArray, ", ")))

	return
}

func sqlFieldValueToString(goType reflect.Kind, reflectValue reflect.Value) (valueString string, err error) {
	switch goType {
	case reflect.Bool:
		if reflectValue.Bool() {
			valueString = "1"
		} else {
			valueString = "0"
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		valueString = fmt.Sprintf("%d", reflectValue.Uint())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		valueString = fmt.Sprintf("%d", reflectValue.Int())
	case reflect.Float32, reflect.Float64:
		valueString = fmt.Sprintf("%f", reflectValue.Float())
	case reflect.String:
		valueString = fmt.Sprintf("\"%s\"", html.EscapeString(reflectValue.String()))
	case reflect.Ptr:
		if reflectValue.IsNil() {
			valueString = "NULL"
		} else {
			valueString = fmt.Sprintf("\"%v\"", reflectValue.Elem())
		}
	default:
		err = ErrValueDoesNotMatchTableType
	}

	return
}

func (table *Table) sqlInsertValue(valueArray []interface{}) ([]string, error) {
	request := []string{}

	fieldGoNameArray := []string{}
	fieldSqlNameArray := []string{}
	for _, fieldName := range table.FieldNameArray {
		tableField := table.FieldMap[fieldName]

		if !tableField.IsAutoIncrement {
			fieldGoNameArray = append(fieldGoNameArray, tableField.GoName)
			fieldSqlNameArray = append(fieldSqlNameArray, tableField.SqlName)
		}
	}

	for _, valueUnit := range valueArray {
		valueReflectValue := reflect.ValueOf(valueUnit)

		valueFieldArray := []string{}
		for _, fieldGoName := range fieldGoNameArray {
			field := table.FieldMap[fieldGoName]
			fieldValue := valueReflectValue.FieldByName(fieldGoName)

			fieldValueString, err := sqlFieldValueToString(field.GoType, fieldValue)
			if err != nil {
				return nil, err
			}

			valueFieldArray = append(valueFieldArray, fieldValueString)
		}

		request = append(request, fmt.Sprintf("INSERT INTO `%s` (%s) VALUES (%s);", table.SqlName, strings.Join(fieldSqlNameArray, ", "), strings.Join(valueFieldArray, ", ")))
	}

	return request, nil
}

func (table *Table) sqlReplaceValue(valueArray []interface{}) ([]string, error) {
	request := []string{}

	fieldGoNameArray := []string{}
	fieldSqlNameArray := []string{}
	for _, fieldName := range table.FieldNameArray {
		tableField := table.FieldMap[fieldName]

		fieldGoNameArray = append(fieldGoNameArray, tableField.GoName)
		fieldSqlNameArray = append(fieldSqlNameArray, tableField.SqlName)
	}

	for _, valueUnit := range valueArray {
		valueReflectValue := reflect.ValueOf(valueUnit)

		valueFieldArray := []string{}
		for _, fieldGoName := range fieldGoNameArray {
			field := table.FieldMap[fieldGoName]
			fieldValue := valueReflectValue.FieldByName(fieldGoName)

			fieldValueString, err := sqlFieldValueToString(field.GoType, fieldValue)
			if err != nil {
				return nil, err
			}

			valueFieldArray = append(valueFieldArray, fieldValueString)
		}

		request = append(request, fmt.Sprintf("REPLACE INTO `%s` (%s) VALUES (%s);", table.SqlName, strings.Join(fieldSqlNameArray, ", "), strings.Join(valueFieldArray, ", ")))
	}

	return request, nil
}

func (table *Table) sqlUpdateValue(valueArray []interface{}) ([]string, error) {
	request := []string{}

	if table.AutoIncrement == nil {
		return nil, ErrTableDoesNotHaveAutoIncrement
	}

	fieldGoNameArray := []string{}
	fieldSqlNameArray := []string{}
	for _, fieldName := range table.FieldNameArray {
		tableField := table.FieldMap[fieldName]

		fieldGoNameArray = append(fieldGoNameArray, tableField.GoName)
		fieldSqlNameArray = append(fieldSqlNameArray, tableField.SqlName)
	}

	for _, valueUnit := range valueArray {
		valueReflectValue := reflect.ValueOf(valueUnit)

		valueFieldSetArray := []string{}
		for _, fieldGoName := range fieldGoNameArray {
			field := table.FieldMap[fieldGoName]
			fieldValue := valueReflectValue.FieldByName(fieldGoName)

			fieldValueString, err := sqlFieldValueToString(field.GoType, fieldValue)
			if err != nil {
				return nil, err
			}

			valueFieldSetArray = append(valueFieldSetArray, fmt.Sprintf("%s = %s", field.SqlName, fieldValueString))
		}

		request = append(request, fmt.Sprintf("UPDATE `%s` SET %s WHERE %s = %d",
			table.SqlName,
			strings.Join(valueFieldSetArray, ", "),
			table.AutoIncrement.SqlName,
			valueReflectValue.FieldByName(table.AutoIncrement.GoName).Int()),
		)
	}

	return request, nil
}

func (table *Table) sqlDeleteValue(valueArray []interface{}) ([]string, error) {
	request := []string{}

	if table.AutoIncrement == nil {
		return nil, ErrTableDoesNotHaveAutoIncrement
	}

	for _, valueUnit := range valueArray {
		valueReflectType := reflect.TypeOf(valueUnit)
		valueReflectValue := reflect.ValueOf(valueUnit)

		if table.GoType == valueReflectType {
			request = append(request, fmt.Sprintf("DELETE FROM `%s` WHERE %s = %d",
				table.SqlName,
				table.AutoIncrement.SqlName,
				valueReflectValue.FieldByName(table.AutoIncrement.GoName).Int()),
			)
		}
	}

	return request, nil
}

//--------------------------------------------------------------------------------//

// Creates a Table object with specified tableName and tableStruct.
// If tableName is not an empty string then it used as table name for sql queries
// in table methods. A tableStruct object must be a some custom struct object
// otherwise ErrValueMustBeAStructure will be returned. Each field of tableStruct
// is parsed and saved in Table object for future use.
func NewTable(tableName string, tableStruct interface{}) (table *Table, err error) {
	if tableStruct == nil {
		err = ErrInvalidArgument
		return
	}

	tableReflectType := reflect.TypeOf(tableStruct)

	if tableReflectType.Kind() != reflect.Struct {
		err = ErrValueMustBeAStructure
		return
	}

	table = &Table{
		GoName: tableReflectType.Name(),
		GoType: tableReflectType,

		SqlName: tableName,

		FieldNameArray: []string{},
		FieldMap:       map[string]*TableField{},

		AutoIncrement: nil,
	}

	for fieldIndex := 0; fieldIndex < tableReflectType.NumField(); fieldIndex++ {
		fieldReflectField := tableReflectType.Field(fieldIndex)

		field := &TableField{
			GoName: fieldReflectField.Name,
			GoType: fieldReflectField.Type.Kind(),

			SqlName: fieldReflectField.Name,
			SqlType: "",
		}

		if !field.ReflectParse(fieldReflectField) {
			continue
		}

		if field.IsAutoIncrement {
			if table.AutoIncrement == nil {
				table.AutoIncrement = field
			} else {
				field.IsAutoIncrement = false
			}
		}

		table.FieldNameArray = append(table.FieldNameArray, field.GoName)
		table.FieldMap[field.GoName] = field
	}

	return
}

//--------------------------------------------------------------------------------//
