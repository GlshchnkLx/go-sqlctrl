package sqlctrl

import (
	"fmt"
	"reflect"
	"strings"
)

// --------------------------------------------------------------------------------//

type BuilderReplace struct {
	sqlDialect   string
	replaceTable *Table
	replaceValue []interface{}
}

// --------------------------------------------------------------------------------//

func (builder *BuilderReplace) SqlDialect(sqlDialect string) *BuilderReplace {
	builder.sqlDialect = sqlDialect
	return builder
}

func (builder *BuilderReplace) Replace(replaceTable *Table) *BuilderReplace {
	builder.replaceTable = replaceTable
	return builder
}

func (builder *BuilderReplace) Value(replaceValue ...interface{}) *BuilderReplace {
	if replaceValue == nil {
		replaceValue = []interface{}{}
	}

	builder.replaceValue = replaceValue
	return builder
}

// --------------------------------------------------------------------------------//

func (builder *BuilderReplace) Build() (result string, option []interface{}, err error) {
	builderReplace := []string{"REPLACE INTO"}

	if builder.replaceTable == nil {
		err = ErrorBuilderMustHaveATable
		return
	}

	builderReplace = append(builderReplace, fmt.Sprintf("`%s`", builder.replaceTable.GetSqlName()))

	fieldSqlNameArray := []string{}
	for _, fieldGoName := range builder.replaceTable.GetGoFieldNameArray() {
		tableField := builder.replaceTable.GetFieldByGoName(fieldGoName)

		fieldSqlNameArray = append(fieldSqlNameArray, tableField.GetSqlName())
	}

	builderValueArray := []string{}
	for _, valueUnit := range builder.replaceValue {
		valueReflectValue := reflect.ValueOf(valueUnit)

		if valueReflectValue.Type() != builder.replaceTable.GetGoType() {
			return "", nil, ErroroBuilderTableHasUnsupportedReferense
		}

		valueFieldArray := []string{}
		for _, fieldGoName := range builder.replaceTable.GetGoFieldNameArray() {
			tableField := builder.replaceTable.GetFieldByGoName(fieldGoName)
			fieldValue := valueReflectValue.FieldByName(fieldGoName)

			fieldValueString, err := SqlFieldValueToString(tableField.GetGoType(), fieldValue)
			if err != nil {
				return "", nil, err
			}

			valueFieldArray = append(valueFieldArray, fieldValueString)
		}

		builderValueArray = append(builderValueArray, fmt.Sprintf("(%s)", strings.Join(valueFieldArray, ", ")))
	}

	builderReplace = append(builderReplace, fmt.Sprintf("(%s)", strings.Join(fieldSqlNameArray, ", ")), " VALUES ", strings.Join(builderValueArray, ", "))
	result = strings.Join(builderReplace, " ")
	return
}

// --------------------------------------------------------------------------------//

func NewBuilderReplace(replaceTable *Table) *BuilderReplace {
	replaceBuilder := &BuilderReplace{
		sqlDialect:   "",
		replaceTable: nil,
		replaceValue: []interface{}{},
	}

	return replaceBuilder.Replace(replaceTable)
}

// --------------------------------------------------------------------------------//
