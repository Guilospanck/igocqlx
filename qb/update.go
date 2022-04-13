package igocqlxqb

import (
	"bytes"
	"context"
	"time"

	"github.com/Guilospanck/igocqlx"
	"github.com/scylladb/gocqlx/v2/qb"
)

type value interface {
	// writeCql writes the bytes for this value to the buffer and returns the
	// list of names of parameters which need substitution.
	writeCql(cql *bytes.Buffer) (names []string)
}

type IUpdateBuilder interface {
	ToCql() (stmt string, names []string)
	Query(session *igocqlx.Session) igocqlx.IQueryx
	QueryContext(ctx context.Context, session *igocqlx.Session) igocqlx.IQueryx
	Table(table string) IUpdateBuilder
	TTL(d time.Duration) IUpdateBuilder
	TTLNamed(name string) IUpdateBuilder
	Timestamp(t time.Time) IUpdateBuilder
	TimestampNamed(name string) IUpdateBuilder
	Timeout(d time.Duration) IUpdateBuilder
	TimeoutNamed(name string) IUpdateBuilder
	Set(columns ...string) IUpdateBuilder
	SetNamed(column, name string) IUpdateBuilder
	SetLit(column, literal string) IUpdateBuilder
	SetFunc(column string, fn *qb.Func) IUpdateBuilder
	SetTuple(column string, count int) IUpdateBuilder
	Add(column string) IUpdateBuilder
	AddNamed(column, name string) IUpdateBuilder
	AddLit(column, literal string) IUpdateBuilder
	AddFunc(column string, fn *qb.Func) IUpdateBuilder
	Remove(column string) IUpdateBuilder
	RemoveNamed(column, name string) IUpdateBuilder
	RemoveLit(column, literal string) IUpdateBuilder
	RemoveFunc(column string, fn *qb.Func) IUpdateBuilder
	Where(w ...qb.Cmp) IUpdateBuilder
	If(w ...qb.Cmp) IUpdateBuilder
	Existing() IUpdateBuilder
}

type UpdateBuilder struct {
	UB *qb.UpdateBuilder
}

func (ub *UpdateBuilder) ToCql() (stmt string, names []string) {
	return ub.UB.ToCql()
}

func (ub *UpdateBuilder) Query(session *igocqlx.Session) igocqlx.IQueryx {
	gocqlxSession := session.S

	queryx := ub.UB.Query(*gocqlxSession)

	return &igocqlx.Queryx{
		Q: queryx,
	}
}

func (ub *UpdateBuilder) QueryContext(ctx context.Context, session *igocqlx.Session) igocqlx.IQueryx {
	gocqlxSession := session.S

	queryx := ub.UB.QueryContext(ctx, *gocqlxSession)

	return &igocqlx.Queryx{
		Q: queryx,
	}
}

func (ub *UpdateBuilder) Table(table string) IUpdateBuilder {
	updateBuilderx := ub.UB.Table(table)

	ub.UB = updateBuilderx

	return ub
}

func (ub *UpdateBuilder) TTL(d time.Duration) IUpdateBuilder {
	updateBuilderx := ub.UB.TTL(d)

	ub.UB = updateBuilderx

	return ub
}

func (ub *UpdateBuilder) TTLNamed(name string) IUpdateBuilder {
	updateBuilderx := ub.UB.TTLNamed(name)

	ub.UB = updateBuilderx

	return ub
}

func (ub *UpdateBuilder) Timestamp(t time.Time) IUpdateBuilder {
	updateBuilderx := ub.UB.Timestamp(t)

	ub.UB = updateBuilderx

	return ub
}

func (ub *UpdateBuilder) TimestampNamed(name string) IUpdateBuilder {
	updateBuilderx := ub.UB.TimestampNamed(name)

	ub.UB = updateBuilderx

	return ub
}

func (ub *UpdateBuilder) Timeout(d time.Duration) IUpdateBuilder {
	updateBuilderx := ub.UB.Timeout(d)

	ub.UB = updateBuilderx

	return ub
}

func (ub *UpdateBuilder) TimeoutNamed(name string) IUpdateBuilder {
	updateBuilderx := ub.UB.TimeoutNamed(name)

	ub.UB = updateBuilderx

	return ub
}

func (ub *UpdateBuilder) Set(columns ...string) IUpdateBuilder {
	updateBuilderx := ub.UB.Set(columns...)

	ub.UB = updateBuilderx

	return ub
}

func (ub *UpdateBuilder) SetNamed(column, name string) IUpdateBuilder {
	updateBuilderx := ub.UB.SetNamed(column, name)

	ub.UB = updateBuilderx

	return ub
}

func (ub *UpdateBuilder) SetLit(column, literal string) IUpdateBuilder {
	updateBuilderx := ub.UB.SetLit(column, literal)

	ub.UB = updateBuilderx

	return ub
}

func (ub *UpdateBuilder) SetFunc(column string, fn *qb.Func) IUpdateBuilder {
	updateBuilderx := ub.UB.SetFunc(column, fn)

	ub.UB = updateBuilderx

	return ub
}

func (ub *UpdateBuilder) SetTuple(column string, count int) IUpdateBuilder {
	updateBuilderx := ub.UB.SetTuple(column, count)

	ub.UB = updateBuilderx

	return ub
}

func (ub *UpdateBuilder) Add(column string) IUpdateBuilder {
	updateBuilderx := ub.UB.Add(column)

	ub.UB = updateBuilderx

	return ub
}

func (ub *UpdateBuilder) AddNamed(column, name string) IUpdateBuilder {
	updateBuilderx := ub.UB.AddNamed(column, name)

	ub.UB = updateBuilderx

	return ub
}

func (ub *UpdateBuilder) AddLit(column, literal string) IUpdateBuilder {
	updateBuilderx := ub.UB.AddLit(column, literal)

	ub.UB = updateBuilderx

	return ub
}

func (ub *UpdateBuilder) AddFunc(column string, fn *qb.Func) IUpdateBuilder {
	updateBuilderx := ub.UB.AddFunc(column, fn)

	ub.UB = updateBuilderx

	return ub
}

func (ub *UpdateBuilder) Remove(column string) IUpdateBuilder {
	updateBuilderx := ub.UB.Remove(column)

	ub.UB = updateBuilderx

	return ub
}

func (ub *UpdateBuilder) RemoveNamed(column, name string) IUpdateBuilder {
	updateBuilderx := ub.UB.RemoveNamed(column, name)

	ub.UB = updateBuilderx

	return ub
}

func (ub *UpdateBuilder) RemoveLit(column, literal string) IUpdateBuilder {
	updateBuilderx := ub.UB.RemoveLit(column, literal)

	ub.UB = updateBuilderx

	return ub
}

func (ub *UpdateBuilder) RemoveFunc(column string, fn *qb.Func) IUpdateBuilder {
	updateBuilderx := ub.UB.RemoveFunc(column, fn)

	ub.UB = updateBuilderx

	return ub
}

func (ub *UpdateBuilder) Where(w ...qb.Cmp) IUpdateBuilder {
	updateBuilderx := ub.UB.Where(w...)

	ub.UB = updateBuilderx

	return ub
}

func (ub *UpdateBuilder) If(w ...qb.Cmp) IUpdateBuilder {
	updateBuilderx := ub.UB.If(w...)

	ub.UB = updateBuilderx

	return ub
}

func (ub *UpdateBuilder) Existing() IUpdateBuilder {
	updateBuilderx := ub.UB.Existing()

	ub.UB = updateBuilderx

	return ub
}
