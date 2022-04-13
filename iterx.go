package igocqlx

import (
	"github.com/scylladb/gocqlx/v2"
)

type IIterx interface {
	Unsafe() IIterx
	StructOnly() IIterx
	Get(dest interface{}) error
	Select(dest interface{}) error
	StructScan(dest interface{}) bool
	Scan(dest ...interface{}) bool
	Close() error

	MapScan(m map[string]interface{}) bool
}

type Iterx struct {
	I *gocqlx.Iterx
}

func (i *Iterx) Unsafe() IIterx {
	iterx := i.I.Unsafe()

	i.I = iterx

	return i
}

func (i *Iterx) StructOnly() IIterx {
	iterx := i.I.StructOnly()

	i.I = iterx

	return i
}

func (i *Iterx) Get(dest interface{}) error {
	return i.I.Get(dest)
}

func (i *Iterx) Select(dest interface{}) error {
	return i.I.Select(dest)
}

func (i *Iterx) StructScan(dest interface{}) bool {
	return i.I.StructScan(dest)
}

func (i *Iterx) Scan(dest ...interface{}) bool {
	return i.I.Scan(dest)
}

func (i *Iterx) Close() error {
	return i.I.Close()
}

func (i *Iterx) MapScan(m map[string]interface{}) bool {
	return i.I.MapScan(m)
}
