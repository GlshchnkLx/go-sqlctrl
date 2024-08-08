package sqlctrl

import (
	"flag"
	"fmt"
	"html"
	"reflect"
	"strings"
)

//--------------------------------------------------------------------------------//
// TABLE FIELD
//--------------------------------------------------------------------------------//

type TableField struct {
	goIndex int
	goName  string
	goField reflect.StructField
	goType  reflect.Kind

	sqlName string
	sqlType string

	isPrimaryKey    bool
	isAutoIncrement bool
	isNotNull       bool

	inUniqueGroup *string

	valueDefault *string
	valueCheck   *string
}

//--------------------------------------------------------------------------------//

func (field *TableField) GetGoIndex() (goIndex int) {
	return field.goIndex
}

func (field *TableField) GetGoName() (goName string) {
	return field.goName
}

func (field *TableField) GetGoType() (goType reflect.Kind) {
	return field.goType
}

func (field *TableField) GetSqlIndex() (sqlIndex int) {
	return field.goIndex
}

func (field *TableField) GetSqlName() (sqlName string) {
	return field.sqlName
}

func (field *TableField) GetSqlType() (sqlType string) {
	return field.sqlType
}

func (field *TableField) IsPrimaryKey() bool {
	return field.isPrimaryKey
}

func (field *TableField) IsAutoIncrement() bool {
	return field.isAutoIncrement
}

func (field *TableField) IsNotNull() bool {
	return field.isNotNull
}

func (field *TableField) InUniqueGroup() *string {
	return field.inUniqueGroup
}

func (field *TableField) ValueDefault() *string {
	return field.valueDefault
}

func (field *TableField) ValueCheck() *string {
	return field.valueCheck
}

//--------------------------------------------------------------------------------//

func (field *TableField) parseReflect() bool {
	goFieldTag, ok := field.goField.Tag.Lookup("sql")
	if !ok {
		return false
	}

	goFieldTagSlice := strings.Split(goFieldTag, "|")
	for sqlTagIndex, sqlTagOption := range goFieldTagSlice {
		goFieldTagSlice[sqlTagIndex] = "--" + strings.Trim(sqlTagOption, " ")
	}

	fs := flag.NewFlagSet("", flag.ContinueOnError)
	fs.Usage = func() {}

	fs.StringVar(&field.sqlName, "NAME", field.goName, "name")
	fs.StringVar(&field.sqlType, "TYPE", "", "type")
	fs.BoolVar(&field.isPrimaryKey, "PRIMARY_KEY", false, "primary_key")
	fs.BoolVar(&field.isAutoIncrement, "AUTO_INCREMENT", false, "auto_increment")
	fs.BoolVar(&field.isNotNull, "NOT_NULL", false, "not_null")
	var fieldIsUnique bool
	fs.BoolVar(&fieldIsUnique, "UNIQUE", false, "unique")
	field.inUniqueGroup = fs.String("UNIQUE_GROUP", "", "unique_group")
	field.valueDefault = fs.String("DEFAULT", "", "default")
	field.valueCheck = fs.String("CHECK", "", "check")

	fs.Parse(goFieldTagSlice)

	if field.sqlType == "" {
		fieldGoType := field.goType

		if field.goType == reflect.Ptr {
			fieldGoType = field.goField.Type.Elem().Kind()
		}

		switch fieldGoType {
		case reflect.Bool:
			field.sqlType = "INTEGER(1)"
		case reflect.Uint, reflect.Int:
			field.sqlType = "INTEGER(8)"
		case reflect.Uint8, reflect.Int8:
			field.sqlType = "INTEGER(1)"
		case reflect.Uint16, reflect.Int16:
			field.sqlType = "INTEGER(2)"
		case reflect.Uint32, reflect.Int32:
			field.sqlType = "INTEGER(4)"
		case reflect.Uint64, reflect.Int64:
			field.sqlType = "INTEGER(8)"
		case reflect.Float32, reflect.Float64:
			field.sqlType = "REAL"
		case reflect.String:
			field.sqlType = "TEXT(4096)"
		default:
			panic(fieldGoType)
		}
	}

	if len(*field.inUniqueGroup) == 0 {
		if fieldIsUnique {
			field.inUniqueGroup = &field.sqlName
		} else {
			field.inUniqueGroup = nil
		}
	}

	if len(*field.valueDefault) == 0 {
		field.valueDefault = nil
	}
	if len(*field.valueCheck) == 0 {
		field.valueCheck = nil
	}

	return true
}

//--------------------------------------------------------------------------------//

func registerTableField(table *Table, fiedlIndex int) (*TableField, error) {
	var (
		reflectStructField reflect.StructField
		field              *TableField
	)

	if table == nil {
		return nil, ErrorTableFieldMustHaveATable
	}

	reflectStructField = table.goType.Field(fiedlIndex)

	field = &TableField{
		goIndex: len(table.goFieldNameArray),
		goName:  reflectStructField.Name,
		goField: reflectStructField,
		goType:  reflectStructField.Type.Kind(),
	}

	if !field.parseReflect() {
		return nil, nil
	}

	table.goFieldNameArray = append(table.goFieldNameArray, field.goName)
	table.goFieldMap[field.goName] = field

	table.sqlFieldNameArray = append(table.sqlFieldNameArray, field.sqlName)
	table.sqlFieldMap[field.sqlName] = field

	if field.isPrimaryKey {
		table.goPrimaryKeyArray = append(table.goPrimaryKeyArray, field)
	}

	if field.isAutoIncrement {
		if table.goAutoIncrement == nil {
			table.goAutoIncrement = field
		} else {
			field.isAutoIncrement = false
		}
	}

	if field.inUniqueGroup != nil {
		if table.goUniqueMap[*field.inUniqueGroup] == nil {
			table.goUniqueMap[*field.inUniqueGroup] = []*TableField{field}
		} else {
			table.goUniqueMap[*field.inUniqueGroup] = append(table.goUniqueMap[*field.inUniqueGroup], field)
		}
	}

	return field, nil
}

//--------------------------------------------------------------------------------//
// TABLE
//--------------------------------------------------------------------------------//

type Table struct {
	goName string
	goType reflect.Type

	sqlName string

	goFieldNameArray []string
	goFieldMap       map[string]*TableField

	sqlFieldNameArray []string
	sqlFieldMap       map[string]*TableField

	goPrimaryKeyArray []*TableField
	goAutoIncrement   *TableField
	goUniqueMap       map[string][]*TableField
}

//--------------------------------------------------------------------------------//

func (table *Table) GetGoName() (goName string) {
	return table.goName
}

func (table *Table) GetGoType() (goType reflect.Type) {
	return table.goType
}

func (table *Table) GetSqlName() (sqlName string) {
	return table.sqlName
}

func (table *Table) GetGoFieldNameArray() (goFieldNameArray []string) {
	return table.goFieldNameArray
}

func (table *Table) GetSqlFieldNameArray() (sqlFieldNameArray []string) {
	return table.sqlFieldNameArray
}

func (table *Table) GetFieldByIndex(fieldIndex int) (tableField *TableField) {
	if len(table.goFieldNameArray) <= fieldIndex {
		return
	}

	return table.goFieldMap[table.goFieldNameArray[fieldIndex]]
}

func (table *Table) GetFieldByGoName(fieldGoName string) (tableField *TableField) {
	return table.goFieldMap[fieldGoName]
}

func (table *Table) GetFieldBySqlName(fieldSqlName string) (tableField *TableField) {
	return table.sqlFieldMap[fieldSqlName]
}

func (table *Table) GetPrimaryKeyArray() (primaryKeyArray []*TableField) {
	return table.goPrimaryKeyArray
}

func (table *Table) GetAutoIncrement() (autoIncrementArray *TableField) {
	return table.goAutoIncrement
}

func (table *Table) GetUniqueNameArray() (uniqueNameArray []string) {
	for uniqueName := range table.goUniqueMap {
		uniqueNameArray = append(uniqueNameArray, uniqueName)
	}

	return uniqueNameArray
}

func (table *Table) GetUniqueArray(uniqueName string) (uniqueArray []*TableField) {
	uniqueArray = table.goUniqueMap[uniqueName]
	return uniqueArray
}

//--------------------------------------------------------------------------------//

func (table *Table) GetStruct(tableStruct interface{}) (tableStructPtr interface{}, fieldArrayPtr []interface{}, err error) {
	tableReflectType := table.goType
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
			err = ErrorTableReferenceIsUnsupported
			return
		}

		if table.goType != tableReflectValue.Type() {
			err = ErrorTableReferenceIsUncorrected
			return
		}
	}

	tableStructPtr = tableReflectValue.Addr().Interface()

	for _, fieldGoName := range table.goFieldNameArray {
		fieldReflectValue := tableReflectValue.FieldByName(fieldGoName)
		fieldArrayPtr = append(fieldArrayPtr, fieldReflectValue.Addr().Interface())
	}

	return
}

//--------------------------------------------------------------------------------//

func NewTable(tableName string, tableStruct interface{}) (table *Table, err error) {
	if tableStruct == nil {
		err = ErrorTableReferenceIsNil
		return
	}

	tableReflectType := reflect.TypeOf(tableStruct)
	if tableReflectType.Kind() != reflect.Struct {
		err = ErrorTableReferenceIsUnsupported
		return
	}

	table = &Table{
		goName:            tableReflectType.Name(),
		goType:            tableReflectType,
		sqlName:           tableName,
		goFieldNameArray:  []string{},
		goFieldMap:        map[string]*TableField{},
		sqlFieldNameArray: []string{},
		sqlFieldMap:       map[string]*TableField{},
		goPrimaryKeyArray: []*TableField{},
		goAutoIncrement:   nil,
		goUniqueMap:       map[string][]*TableField{},
	}

	for fieldIndex := 0; fieldIndex < tableReflectType.NumField(); fieldIndex++ {
		_, err = registerTableField(table, fieldIndex)
		if err != nil {
			return
		}
	}

	if table.goAutoIncrement != nil {
		if !(len(table.goPrimaryKeyArray) == 1 && table.goAutoIncrement.isPrimaryKey) {
			table.goAutoIncrement.isAutoIncrement = false
			table.goAutoIncrement = nil
		}
	}

	return
}

//--------------------------------------------------------------------------------//
// TOOL
//--------------------------------------------------------------------------------//

func SqlFieldValueToString(goType reflect.Kind, reflectValue reflect.Value) (valueString string, err error) {
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
		valueString = fmt.Sprintf("'%s'", html.EscapeString(reflectValue.String()))
	case reflect.Ptr:
		if reflectValue.IsNil() {
			valueString = "NULL"
		} else {
			valueString = fmt.Sprintf("'%v'", reflectValue.Elem())
		}
	default:
		err = ErrorTableReferenceIsUnsupported
	}

	return
}

//--------------------------------------------------------------------------------//
