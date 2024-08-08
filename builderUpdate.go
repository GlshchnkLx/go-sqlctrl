package sqlctrl

import (
	"fmt"
	"strings"
)

// --------------------------------------------------------------------------------//

type BuilderUpdate struct {
	sqlDialect       string
	updateTable      *Table
	setStringArray   []string
	whereStringArray []string
}

// --------------------------------------------------------------------------------//

func (builder *BuilderUpdate) SqlDialect(sqlDialect string) *BuilderUpdate {
	builder.sqlDialect = sqlDialect
	return builder
}

func (builder *BuilderUpdate) Update(updateTable *Table) *BuilderUpdate {
	builder.updateTable = updateTable
	return builder
}

func (builder *BuilderUpdate) Set(setStringArray ...string) *BuilderUpdate {
	builder.setStringArray = append(builder.setStringArray, setStringArray...)
	return builder
}

func (builder *BuilderUpdate) Where(whereStringArray ...string) *BuilderUpdate {
	builder.whereStringArray = append(builder.whereStringArray, whereStringArray...)
	return builder
}

// --------------------------------------------------------------------------------//

func (builder *BuilderUpdate) Build() (result string, option []interface{}, err error) {
	builderUpdate := []string{"UPDATE"}

	if builder.updateTable == nil {
		err = ErrorBuilderMustHaveATable
		return
	}

	builderUpdate = append(builderUpdate, fmt.Sprintf("`%s`", builder.updateTable.GetSqlName()))

	if len(builder.setStringArray) > 0 {
		builderUpdate = append(builderUpdate, "SET", strings.Join(builder.setStringArray, ", "))
	}

	if len(builder.whereStringArray) > 0 {
		builderUpdate = append(builderUpdate, "WHERE", strings.Join(builder.whereStringArray, " AND "))
	}

	result = strings.Join(builderUpdate, " ")
	return
}

// --------------------------------------------------------------------------------//

func NewBuilderUpdate(updateTable *Table) *BuilderUpdate {
	updateBuilder := &BuilderUpdate{
		sqlDialect:       "",
		updateTable:      nil,
		setStringArray:   []string{},
		whereStringArray: []string{},
	}

	return updateBuilder.Update(updateTable)
}

// --------------------------------------------------------------------------------//
