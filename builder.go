package sqlctrl

//--------------------------------------------------------------------------------//

type Builder interface {
	Build() (string, []interface{}, error)
}

type BuilderWithDialect interface {
	Builder
	SetDialect(string)
}

type BuilderWithResponse interface {
	Build() (string, []interface{}, error)
	GetResponseTable() *Table
}

//--------------------------------------------------------------------------------//
