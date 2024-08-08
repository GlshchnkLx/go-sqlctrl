package sqlctrl

import (
	"fmt"
	"strings"
)

//--------------------------------------------------------------------------------//

type BuilderCreate struct {
	sqlDialect  string
	ifNotExists bool
	createTable *Table
	createName  *string
}

//--------------------------------------------------------------------------------//

func (builder *BuilderCreate) SetDialect(sqlDialect string) {
	builder.sqlDialect = sqlDialect
}

func (builder *BuilderCreate) IfNotExists(value bool) *BuilderCreate {
	builder.ifNotExists = value
	return builder
}

func (builder *BuilderCreate) Create(createTable *Table) *BuilderCreate {
	builder.createTable = createTable

	if createTable != nil {
		builder.CreateName(createTable.GetSqlName())
	}

	return builder
}

func (builder *BuilderCreate) CreateName(createName string) *BuilderCreate {
	builder.createName = &createName
	return builder
}

//--------------------------------------------------------------------------------//

func (builder *BuilderCreate) Build() (result string, option []interface{}, err error) {
	builderCreateTable := []string{"CREATE TABLE"}
	builderCreateTableDefine := []string{}

	if builder.createTable == nil {
		err = ErrorBuilderMustHaveATable
		return
	}

	if builder.ifNotExists {
		builderCreateTable = append(builderCreateTable, "IF NOT EXISTS")
	}

	builderCreateTable = append(builderCreateTable, fmt.Sprintf("`%s`", *builder.createName))

	for _, fieldGoName := range builder.createTable.GetGoFieldNameArray() {
		tableField := builder.createTable.GetFieldByGoName(fieldGoName)
		builderTableDefine := []string{tableField.GetSqlName()}

		if tableField.IsPrimaryKey() && tableField.IsAutoIncrement() {
			builderTableDefine = append(builderTableDefine, "INTEGER")

			switch builder.sqlDialect {
			case "sqlite":
				builderTableDefine = append(builderTableDefine, "PRIMARY KEY")
			case "mysql":
				builderTableDefine = append(builderTableDefine, "PRIMARY KEY")
			default:
				builderTableDefine = append(builderTableDefine, "PRIMARY_KEY")
			}
		} else {
			builderTableDefine = append(builderTableDefine, tableField.GetSqlType())
		}

		if tableField.IsAutoIncrement() {
			switch builder.sqlDialect {
			case "sqlite":
				builderTableDefine = append(builderTableDefine, "AUTOINCREMENT")
			case "mysql":
				builderTableDefine = append(builderTableDefine, "AUTO_INCREMENT")
			default:
				builderTableDefine = append(builderTableDefine, "AUTO_INCREMENT")
			}
		}

		if tableField.IsNotNull() {
			switch builder.sqlDialect {
			case "sqlite":
				builderTableDefine = append(builderTableDefine, "NOT NULL")
			case "mysql":
				builderTableDefine = append(builderTableDefine, "NOT NULL")
			default:
				builderTableDefine = append(builderTableDefine, "NOT_NULL")
			}
		}

		if tableField.ValueDefault() != nil {
			builderTableDefine = append(builderTableDefine, fmt.Sprintf("DEFAULT %s", *tableField.ValueDefault()))
		}

		builderCreateTableDefine = append(builderCreateTableDefine, strings.Join(builderTableDefine, " "))
	}

	for _, fieldGoName := range builder.createTable.GetGoFieldNameArray() {
		tableField := builder.createTable.GetFieldByGoName(fieldGoName)

		if tableField.ValueCheck() != nil {
			builderCreateTableDefine = append(builderCreateTableDefine, fmt.Sprintf("CONSTRAINT %s_%s_ck CHECK(%s)", *builder.createName, tableField.GetSqlName(), *tableField.ValueCheck()))
		}
	}

	if builder.createTable.GetAutoIncrement() == nil {
		primaryFieldArray := []string{}
		for _, tableField := range builder.createTable.GetPrimaryKeyArray() {
			primaryFieldArray = append(primaryFieldArray, tableField.GetSqlName())
		}

		if len(primaryFieldArray) > 1 {
			switch builder.sqlDialect {
			case "sqlite":
				builderCreateTableDefine = append(builderCreateTableDefine, fmt.Sprintf("CONSTRAINT %s_pk PRIMARY KEY(%s)", *builder.createName, strings.Join(primaryFieldArray, ", ")))
			case "mysql":
				builderCreateTableDefine = append(builderCreateTableDefine, fmt.Sprintf("CONSTRAINT %s_pk PRIMARY KEY(%s)", *builder.createName, strings.Join(primaryFieldArray, ", ")))
			default:
				builderCreateTableDefine = append(builderCreateTableDefine, fmt.Sprintf("CONSTRAINT %s_pk PRIMARY_KEY(%s)", *builder.createName, strings.Join(primaryFieldArray, ", ")))
			}
		}
	}

	for _, uniqueName := range builder.createTable.GetUniqueNameArray() {
		uniqueFieldArray := []string{}

		for _, tableField := range builder.createTable.GetUniqueArray(uniqueName) {
			uniqueFieldArray = append(uniqueFieldArray, tableField.GetSqlName())
		}

		builderCreateTableDefine = append(builderCreateTableDefine, fmt.Sprintf("CONSTRAINT %s_%s_uq UNIQUE(%s)", *builder.createName, uniqueName, strings.Join(uniqueFieldArray, ", ")))
	}

	builderCreateTable = append(builderCreateTable, fmt.Sprintf("(%s)", strings.Join(builderCreateTableDefine, ", ")))
	result = strings.Join(builderCreateTable, " ")
	return
}

//--------------------------------------------------------------------------------//

func NewBuilderCreate(createTable *Table) *BuilderCreate {
	createBuilder := &BuilderCreate{
		sqlDialect:  "",
		ifNotExists: true,
		createTable: nil,
		createName:  nil,
	}

	return createBuilder.Create(createTable)
}

//--------------------------------------------------------------------------------//
