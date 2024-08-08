package sqlctrl

import (
	"fmt"
	"reflect"
	"strings"
)

//--------------------------------------------------------------------------------//

type BuilderSelect struct {
	distinct         bool
	selectTable      *Table
	fromSelectArray  []*BuilderSelect
	fromTableArray   []*Table
	whereStringArray []string
}

//--------------------------------------------------------------------------------//

func (builder *BuilderSelect) GetResponseTable() *Table {
	return builder.selectTable
}

//--------------------------------------------------------------------------------//

func (builder *BuilderSelect) Distinct(value bool) *BuilderSelect {
	builder.distinct = value
	return builder
}

func (builder *BuilderSelect) Select(selectTable *Table) *BuilderSelect {
	builder.selectTable = selectTable
	return builder
}

func (builder *BuilderSelect) FromSelect(fromSelectArray ...*BuilderSelect) *BuilderSelect {
	builder.fromSelectArray = append(builder.fromSelectArray, fromSelectArray...)
	return builder
}

func (builder *BuilderSelect) FromTable(fromTableArray ...*Table) *BuilderSelect {
	builder.fromTableArray = append(builder.fromTableArray, fromTableArray...)
	return builder
}

func (builder *BuilderSelect) Where(whereStringArray ...string) *BuilderSelect {
	builder.whereStringArray = append(builder.whereStringArray, whereStringArray...)
	return builder
}

//--------------------------------------------------------------------------------//

func (builder *BuilderSelect) GetType() reflect.Type {
	return builder.selectTable.GetGoType()
}

func (builder *BuilderSelect) GetStruct(tableStruct interface{}) (tableStructPtr interface{}, fieldArrayPtr []interface{}, err error) {
	return builder.selectTable.GetStruct(tableStruct)
}

func (builder *BuilderSelect) Build() (result string, option []interface{}, err error) {
	builderSelect := []string{"SELECT"}

	if builder.distinct {
		builderSelect = append(builderSelect, "DISTINCT")
	}

	if builder.selectTable == nil {
		err = ErrorBuilderMustHaveATable
		return
	}

	builderSelect = append(builderSelect, strings.Join(builder.selectTable.GetSqlFieldNameArray(), ", "))

	builderSelect = append(builderSelect, "FROM")

	if len(builder.fromSelectArray) == 0 && len(builder.fromTableArray) == 0 && len(builder.selectTable.GetSqlName()) == 0 {
		err = ErrorBuilderMustHaveATableWithName
		return
	}

	if len(builder.fromSelectArray) == 0 && len(builder.fromTableArray) == 0 {
		builderSelect = append(builderSelect, fmt.Sprintf("`%s`", builder.selectTable.GetSqlName()))
	} else {
		builderSelectFrom := []string{}

		for _, fromSelect := range builder.fromSelectArray {
			if fromSelect.selectTable == nil || len(fromSelect.selectTable.GetSqlName()) == 0 {
				err = ErrorBuilderMustHaveATableWithName
				return
			}

			subSelectResult, subSelectOption, subSelectError := fromSelect.Build()
			if subSelectError != nil {
				err = subSelectError
				return
			}

			option = append(option, subSelectOption...)
			builderSelectFrom = append(builderSelectFrom, fmt.Sprintf("(%s) AS `%s`", subSelectResult, fromSelect.selectTable.GetSqlName()))
		}

		for _, fromTable := range builder.fromTableArray {
			if len(fromTable.GetSqlName()) == 0 {
				err = ErrorBuilderMustHaveATableWithName
				return
			}

			builderSelectFrom = append(builderSelectFrom, fmt.Sprintf("`%s`", fromTable.GetSqlName()))
		}

		builderSelect = append(builderSelect, strings.Join(builderSelectFrom, ", "))
	}

	if len(builder.whereStringArray) > 0 {
		builderSelect = append(builderSelect, "WHERE", strings.Join(builder.whereStringArray, " AND "))
	}

	result = strings.Join(builderSelect, " ")
	return
}

//--------------------------------------------------------------------------------//

func NewBuilderSelect(selectTable *Table) (selectBuilder *BuilderSelect) {
	selectBuilder = &BuilderSelect{
		distinct:         false,
		selectTable:      nil,
		fromSelectArray:  []*BuilderSelect{},
		fromTableArray:   []*Table{},
		whereStringArray: []string{},
	}

	return selectBuilder.Select(selectTable)
}

//--------------------------------------------------------------------------------//
