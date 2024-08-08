package sqlctrl

import (
	"fmt"
	"reflect"
	"strings"
)

// --------------------------------------------------------------------------------//

type BuilderInsert struct {
	sqlDialect  string
	insertTable *Table
	insertValue []interface{}
}

// --------------------------------------------------------------------------------//

func (builder *BuilderInsert) SqlDialect(sqlDialect string) *BuilderInsert {
	builder.sqlDialect = sqlDialect
	return builder
}

func (builder *BuilderInsert) Insert(insertTable *Table) *BuilderInsert {
	builder.insertTable = insertTable
	return builder
}

func (builder *BuilderInsert) Value(insertValue ...interface{}) *BuilderInsert {
	if insertValue == nil {
		insertValue = []interface{}{}
	}

	builder.insertValue = insertValue
	return builder
}

// --------------------------------------------------------------------------------//

func (builder *BuilderInsert) Build() (result string, option []interface{}, err error) {
	builderInsert := []string{"INSERT INTO"}

	if builder.insertTable == nil {
		err = ErrorBuilderMustHaveATable
		return
	}

	builderInsert = append(builderInsert, fmt.Sprintf("`%s`", builder.insertTable.GetSqlName()))

	fieldGoNameArray := []string{}
	fieldSqlNameArray := []string{}
	for _, fieldGoName := range builder.insertTable.GetGoFieldNameArray() {
		tableField := builder.insertTable.GetFieldByGoName(fieldGoName)

		if !tableField.IsAutoIncrement() {
			fieldGoNameArray = append(fieldGoNameArray, tableField.GetGoName())
			fieldSqlNameArray = append(fieldSqlNameArray, tableField.GetSqlName())
		}
	}

	builderValueArray := []string{}
	for _, valueUnit := range builder.insertValue {
		valueReflectValue := reflect.ValueOf(valueUnit)

		if valueReflectValue.Type() != builder.insertTable.GetGoType() {
			return "", nil, ErroroBuilderTableHasUnsupportedReferense
		}

		valueFieldArray := []string{}
		for _, fieldGoName := range fieldGoNameArray {
			tableField := builder.insertTable.GetFieldByGoName(fieldGoName)
			fieldValue := valueReflectValue.FieldByName(fieldGoName)

			fieldValueString, err := SqlFieldValueToString(tableField.GetGoType(), fieldValue)
			if err != nil {
				return "", nil, err
			}

			valueFieldArray = append(valueFieldArray, fieldValueString)
		}

		builderValueArray = append(builderValueArray, fmt.Sprintf("(%s)", strings.Join(valueFieldArray, ", ")))
	}

	builderInsert = append(builderInsert, fmt.Sprintf("(%s)", strings.Join(fieldSqlNameArray, ", ")), " VALUES ", strings.Join(builderValueArray, ", "))
	result = strings.Join(builderInsert, " ")
	return
}

// --------------------------------------------------------------------------------//

func NewBuilderInsert(insertTable *Table) *BuilderInsert {
	insertBuilder := &BuilderInsert{
		sqlDialect:  "",
		insertTable: nil,
		insertValue: []interface{}{},
	}

	return insertBuilder.Insert(insertTable)
}

// --------------------------------------------------------------------------------//
