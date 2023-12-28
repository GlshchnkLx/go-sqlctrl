package sqlctrl

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
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
}

func (field *TableField) GetHash() string {
	tableJson, err := json.Marshal(field)
	if err != nil {
		return ""
	}

	tableHash := md5.Sum(tableJson)
	return hex.EncodeToString(tableHash[:])
}

//--------------------------------------------------------------------------------//

type Table struct {
	GoName string       `json:"goName"`
	GoType reflect.Type `json:"-"`

	SqlName string `json:"sqlName"`

	FieldNameArray []string               `json:"fieldNameArray"`
	FieldMap       map[string]*TableField `json:"fieldMap"`

	PrimaryKey    []string `json:"primaryKey"`
	AutoIncrement *string  `json:"autoIncrement"`
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
			err = errValueMustBeAStructureOrPointer
			return
		}

		if table.GoType != tableReflectValue.Type() {
			err = fmt.Errorf("tables do not match")
			return
		}
	}

	tableStructPtr = tableReflectValue.Addr().Interface()

	for fieldIndex := 0; fieldIndex < tableReflectType.NumField(); fieldIndex++ {
		fieldReflectValue := tableReflectValue.Field(fieldIndex)
		fieldArrayPtr = append(fieldArrayPtr, fieldReflectValue.Addr().Interface())
	}

	return
}

//--------------------------------------------------------------------------------//

func (table *Table) convertInterfaceToStructArray(value interface{}) (valueArray []interface{}, err error) {
	valueReflectType := reflect.TypeOf(value)
	valueReflectValue := reflect.ValueOf(value)

	if value == nil {
		err = fmt.Errorf("no value")
		return
	}

	switch valueReflectType.Kind() {
	case reflect.Struct:
		if table.GoType != valueReflectType {
			err = errValueDoesNotMatchTableType
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
				fmt.Println(table.GoType, valueReflectType, valueReflectValue.Index(i).Type())
				err = errValueDoesNotMatchTableType
				return
			}

			valueArray = append(valueArray, valueReflectValue.Index(i).Interface())
		}
	}

	return
}

//--------------------------------------------------------------------------------//

func (table *Table) sqlCreateTable() (request []string, err error) {
	fieldDeclarationArray := []string{}

	for _, fieldName := range table.FieldNameArray {
		tableField := table.FieldMap[fieldName]

		fieldDeclaration := fmt.Sprintf("%s %s", tableField.SqlName, tableField.SqlType)

		if tableField.IsPrimaryKey {
			fieldDeclaration = fmt.Sprintf("%s %s", fieldDeclaration, "PRIMARY_KEY")
		}

		if tableField.IsAutoIncrement {
			fieldDeclaration = fmt.Sprintf("%s %s", fieldDeclaration, "AUTO_INCREMENT")
		}

		if tableField.IsUnique {
			fieldDeclaration = fmt.Sprintf("%s %s", fieldDeclaration, "UNIQUE")
		}

		if tableField.IsNotNull {
			fieldDeclaration = fmt.Sprintf("%s %s", fieldDeclaration, "NOT_NULL")
		}

		fieldDeclarationArray = append(fieldDeclarationArray, fieldDeclaration)
	}

	request = append(request, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s);", table.SqlName, strings.Join(fieldDeclarationArray, ", ")))

	return
}

func (table *Table) sqlInsertValue(valueArray []interface{}) (request []string, err error) {
	fieldDeclarationArray := []string{}
	fieldNameArray := []string{}

	for _, fieldName := range table.FieldNameArray {
		tableField := table.FieldMap[fieldName]

		if !tableField.IsAutoIncrement {
			fieldNameArray = append(fieldNameArray, tableField.GoName)
			fieldDeclarationArray = append(fieldDeclarationArray, tableField.SqlName)
		}
	}

	valueDeclarationArray := []string{}

	for _, valueUnit := range valueArray {
		valueReflectType := reflect.TypeOf(valueUnit)
		valueReflectValue := reflect.ValueOf(valueUnit)

		if table.GoType == valueReflectType {
			valueDeclaration := []string{}

			for _, fieldName := range fieldNameArray {
				tableField := table.FieldMap[fieldName]
				value := valueReflectValue.FieldByName(fieldName)
				var valueString string

				switch tableField.GoType {
				case reflect.Bool:
					if value.Bool() {
						valueString = "0"
					} else {
						valueString = "1"
					}
				case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					valueString = fmt.Sprintf("%d", value.Uint())
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					valueString = fmt.Sprintf("%d", value.Int())
				case reflect.Float32, reflect.Float64:
					valueString = fmt.Sprintf("%f", value.Float())
				case reflect.String:
					valueString = fmt.Sprintf("\"%s\"", value.String())
				default:
					panic(tableField.GoType)
				}

				valueDeclaration = append(valueDeclaration, valueString)
			}

			valueDeclarationArray = append(valueDeclarationArray, fmt.Sprintf("(%s)", strings.Join(valueDeclaration, ", ")))
		}
	}

	request = append(request, fmt.Sprintf("INSERT INTO %s (%s) VALUES %s;", table.SqlName, strings.Join(fieldDeclarationArray, ", "), strings.Join(valueDeclarationArray, ", ")))

	return
}

func (table *Table) sqlUpdateValue(valueArray []interface{}) (request []string, err error) {
	fieldNameArray := []string{}

	for _, fieldName := range table.FieldNameArray {
		tableField := table.FieldMap[fieldName]

		if !tableField.IsAutoIncrement {
			fieldNameArray = append(fieldNameArray, tableField.GoName)
		}
	}

	valueDeclarationArray := []string{}

	for _, valueUnit := range valueArray {
		valueReflectType := reflect.TypeOf(valueUnit)
		valueReflectValue := reflect.ValueOf(valueUnit)

		if table.GoType == valueReflectType {
			valueDeclaration := []string{}

			for _, fieldName := range fieldNameArray {
				tableField := table.FieldMap[fieldName]
				value := valueReflectValue.FieldByName(fieldName)
				var valueString string

				switch tableField.GoType {
				case reflect.Bool:
					if value.Bool() {
						valueString = "0"
					} else {
						valueString = "1"
					}
				case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					valueString = fmt.Sprintf("%d", value.Uint())
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					valueString = fmt.Sprintf("%d", value.Int())
				case reflect.Float32, reflect.Float64:
					valueString = fmt.Sprintf("%f", value.Float())
				case reflect.String:
					valueString = fmt.Sprintf("\"%s\"", value.String())
				default:
					panic(tableField.GoType)
				}

				valueDeclaration = append(valueDeclaration, fmt.Sprintf("%s = %s", tableField.SqlName, valueString))
			}

			valueDeclarationArray = append(valueDeclarationArray, fmt.Sprintf("UPDATE %s SET %s WHERE %s = %d",
				table.SqlName,
				strings.Join(valueDeclaration, ", "),
				table.FieldMap[*table.AutoIncrement].SqlName,
				valueReflectValue.FieldByName(*table.AutoIncrement).Int()),
			)
		}
	}

	request = append(request, valueDeclarationArray...)

	return
}

func (table *Table) sqlDeleteValue(valueArray []interface{}) (request []string, err error) {
	valueDeclarationArray := []string{}

	for _, valueUnit := range valueArray {
		valueReflectType := reflect.TypeOf(valueUnit)
		valueReflectValue := reflect.ValueOf(valueUnit)

		if table.GoType == valueReflectType {
			valueDeclarationArray = append(valueDeclarationArray, fmt.Sprintf("DELETE FROM %s WHERE %s = %d",
				table.SqlName,
				table.FieldMap[*table.AutoIncrement].SqlName,
				valueReflectValue.FieldByName(*table.AutoIncrement).Int()),
			)
		}
	}

	request = append(request, valueDeclarationArray...)

	return
}

//--------------------------------------------------------------------------------//

func NewTable(tableName string, tableStruct interface{}) (table *Table, err error) {
	tableReflectType := reflect.TypeOf(tableStruct)

	if tableReflectType.Kind() != reflect.Struct {
		err = errValueMustBeAStructure
		return
	}

	table = &Table{
		GoName: tableReflectType.Name(),
		GoType: tableReflectType,

		SqlName: tableName,

		FieldNameArray: []string{},
		FieldMap:       map[string]*TableField{},

		PrimaryKey:    []string{},
		AutoIncrement: nil,
	}

	for fieldIndex := 0; fieldIndex < tableReflectType.NumField(); fieldIndex++ {
		fieldReflectField := tableReflectType.Field(fieldIndex)

		fieldReflectTag, ok := fieldReflectField.Tag.Lookup("sql")
		if !ok {
			continue
		}
		fieldReflectTagSlice := strings.Split(fieldReflectTag, ",")

		for tagIndex, tagValue := range fieldReflectTagSlice {
			fieldReflectTagSlice[tagIndex] = strings.Trim(tagValue, " ")

		}

		field := &TableField{
			GoName: fieldReflectField.Name,
			GoType: fieldReflectField.Type.Kind(),

			SqlName: fieldReflectField.Name,
			SqlType: "TEXT(4096)",
		}

		if len(fieldReflectTagSlice) > 0 {
			field.SqlName = fieldReflectTagSlice[0]
		}

		if len(fieldReflectTagSlice) > 1 {
			field.SqlType = fieldReflectTagSlice[1]
		} else {
			switch field.GoType {
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
				panic(field.GoType)
			}
		}

		if len(fieldReflectTagSlice) > 2 {
			for tagIndex := 2; tagIndex < len(fieldReflectTagSlice); tagIndex++ {
				tagValue := fieldReflectTagSlice[tagIndex]

				switch tagValue {
				case "PRIMARY_KEY":
					field.IsPrimaryKey = true
				case "AUTO_INCREMENT":
					field.IsAutoIncrement = true
				case "UNIQUE":
					field.IsUnique = true
				case "NOT_NULL":
					field.IsNotNull = true
				}
			}
		}

		if field.IsPrimaryKey {
			table.PrimaryKey = append(table.PrimaryKey, field.GoName)
		}

		if field.IsAutoIncrement {
			if table.AutoIncrement == nil {
				table.AutoIncrement = &field.GoName
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
