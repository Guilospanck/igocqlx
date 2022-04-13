package igocqlx

import (
	"reflect"

	"github.com/scylladb/gocqlx/v2"
)

type IIterx interface {
	Unsafe() IIterx
	StructOnly() IIterx
	Get(dest interface{}) error
	scanAny(dest interface{}) bool
	Select(dest interface{}) error
	scanAll(dest interface{}) bool
	isScannable(t reflect.Type) bool
	scan(value reflect.Value) bool
	StructScan(dest interface{}) bool
	structScan(value reflect.Value) bool
	fieldsByTraversal(value reflect.Value, traversals [][]int, values []interface{}) error
	Scan(dest ...interface{}) bool
	Close() error
	checkErrAndNotFound() error

	MapScan(m map[string]interface{}) bool
}

type Iterx struct {
	*gocqlx.Iterx
}
