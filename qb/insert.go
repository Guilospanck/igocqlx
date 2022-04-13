package igocqlxqb

import (
	"context"
	"time"

	"github.com/Guilospanck/igocqlx"
	"github.com/scylladb/gocqlx/v2/qb"
)

type IInsertBuilder interface {
	ToCql() (stmt string, names []string)
	Query(session *igocqlx.Session) igocqlx.IQueryx
	QueryContext(ctx context.Context, session *igocqlx.Session) igocqlx.IQueryx
	Into(table string) IInsertBuilder
	Json() IInsertBuilder
	Columns(columns ...string) IInsertBuilder
	NamedColumn(column, name string) IInsertBuilder
	LitColumn(column, literal string) IInsertBuilder
	FuncColumn(column string, fn *qb.Func) IInsertBuilder
	TupleColumn(column string, count int) IInsertBuilder
	Unique() IInsertBuilder
	TTL(d time.Duration) IInsertBuilder
	TTLNamed(name string) IInsertBuilder
	Timestamp(t time.Time) IInsertBuilder
	TimestampNamed(name string) IInsertBuilder
	Timeout(d time.Duration) IInsertBuilder
	TimeoutNamed(name string) IInsertBuilder
}

type InsertBuilder struct {
	IB *qb.InsertBuilder
}

func (ib *InsertBuilder) ToCql() (stmt string, names []string) {
	return ib.IB.ToCql()
}

func (ib *InsertBuilder) Query(session *igocqlx.Session) igocqlx.IQueryx {
	gocqlxSession := session.S

	queryx := ib.IB.Query(*gocqlxSession)

	return &igocqlx.Queryx{
		Q: queryx,
	}
}

func (ib *InsertBuilder) QueryContext(ctx context.Context, session *igocqlx.Session) igocqlx.IQueryx {
	gocqlxSession := session.S

	queryx := ib.IB.QueryContext(ctx, *gocqlxSession)

	return &igocqlx.Queryx{
		Q: queryx,
	}
}

func (ib *InsertBuilder) Into(table string) IInsertBuilder {
	deleteBuilderx := ib.IB.Into(table)

	ib.IB = deleteBuilderx

	return ib
}

func (ib *InsertBuilder) Json() IInsertBuilder {
	deleteBuilderx := ib.IB.Json()

	ib.IB = deleteBuilderx

	return ib
}

func (ib *InsertBuilder) Columns(columns ...string) IInsertBuilder {
	deleteBuilderx := ib.IB.Columns(columns...)

	ib.IB = deleteBuilderx

	return ib
}

func (ib *InsertBuilder) NamedColumn(column, name string) IInsertBuilder {
	deleteBuilderx := ib.IB.NamedColumn(column, name)

	ib.IB = deleteBuilderx

	return ib
}

func (ib *InsertBuilder) LitColumn(column, literal string) IInsertBuilder {
	deleteBuilderx := ib.IB.LitColumn(column, literal)

	ib.IB = deleteBuilderx

	return ib
}

func (ib *InsertBuilder) FuncColumn(column string, fn *qb.Func) IInsertBuilder {
	deleteBuilderx := ib.IB.FuncColumn(column, fn)

	ib.IB = deleteBuilderx

	return ib
}

func (ib *InsertBuilder) TupleColumn(column string, count int) IInsertBuilder {
	deleteBuilderx := ib.IB.TupleColumn(column, count)

	ib.IB = deleteBuilderx

	return ib
}

func (ib *InsertBuilder) Unique() IInsertBuilder {
	deleteBuilderx := ib.IB.Unique()

	ib.IB = deleteBuilderx

	return ib
}

func (ib *InsertBuilder) TTL(d time.Duration) IInsertBuilder {
	deleteBuilderx := ib.IB.TTL(d)

	ib.IB = deleteBuilderx

	return ib
}

func (ib *InsertBuilder) TTLNamed(name string) IInsertBuilder {
	deleteBuilderx := ib.IB.TTLNamed(name)

	ib.IB = deleteBuilderx

	return ib
}

func (ib *InsertBuilder) Timestamp(t time.Time) IInsertBuilder {
	deleteBuilderx := ib.IB.Timestamp(t)

	ib.IB = deleteBuilderx

	return ib
}

func (ib *InsertBuilder) TimestampNamed(name string) IInsertBuilder {
	deleteBuilderx := ib.IB.TimestampNamed(name)

	ib.IB = deleteBuilderx

	return ib
}

func (ib *InsertBuilder) Timeout(d time.Duration) IInsertBuilder {
	deleteBuilderx := ib.IB.Timeout(d)

	ib.IB = deleteBuilderx

	return ib
}

func (ib *InsertBuilder) TimeoutNamed(name string) IInsertBuilder {
	deleteBuilderx := ib.IB.TimeoutNamed(name)

	ib.IB = deleteBuilderx

	return ib
}
