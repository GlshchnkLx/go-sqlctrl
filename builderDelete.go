package sqlctrl

import (
	"fmt"
	"strings"
)

//--------------------------------------------------------------------------------//

type BuilderDelete struct {
	sqlDialect       string
	deleteTable      *Table
	whereStringArray []string
}

//--------------------------------------------------------------------------------//

func (builder *BuilderDelete) SqlDialect(sqlDialect string) *BuilderDelete {
	builder.sqlDialect = sqlDialect
	return builder
}

func (builder *BuilderDelete) Delete(deleteTable *Table) *BuilderDelete {
	builder.deleteTable = deleteTable
	return builder
}

func (builder *BuilderDelete) Where(whereStringArray ...string) *BuilderDelete {
	builder.whereStringArray = append(builder.whereStringArray, whereStringArray...)
	return builder
}

// --------------------------------------------------------------------------------//

func (builder *BuilderDelete) Build() (result string, option []interface{}, err error) {
	builderUpdate := []string{"DELETE FROM"}

	if builder.deleteTable == nil {
		err = ErrorBuilderMustHaveATable
		return
	}

	builderUpdate = append(builderUpdate, fmt.Sprintf("`%s`", builder.deleteTable.GetSqlName()))

	if len(builder.whereStringArray) > 0 {
		builderUpdate = append(builderUpdate, "WHERE", strings.Join(builder.whereStringArray, " AND "))
	}

	result = strings.Join(builderUpdate, " ")
	return
}

// --------------------------------------------------------------------------------//

func NewBuilderDelete(deleteTable *Table) *BuilderDelete {
	builderDelete := &BuilderDelete{
		sqlDialect:       "",
		deleteTable:      nil,
		whereStringArray: []string{},
	}

	return builderDelete.Delete(deleteTable)
}

// --------------------------------------------------------------------------------//
