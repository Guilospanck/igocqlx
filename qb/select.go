package igocqlxqb

import (
	"context"
	"time"

	"github.com/Guilospanck/igocqlx"
	"github.com/scylladb/gocqlx/v2/qb"
)

type ISelectBuilder interface {
	ToCql() (stmt string, names []string)
	Query(session *igocqlx.Session) igocqlx.IQueryx
	QueryContext(ctx context.Context, session *igocqlx.Session) igocqlx.IQueryx
	From(table string) ISelectBuilder
	Json() ISelectBuilder
	Columns(columns ...string) ISelectBuilder
	Distinct(columns ...string) ISelectBuilder
	Timeout(d time.Duration) ISelectBuilder
	TimeoutNamed(name string) ISelectBuilder
	Where(w ...qb.Cmp) ISelectBuilder
	GroupBy(columns ...string) ISelectBuilder
	OrderBy(column string, o qb.Order) ISelectBuilder
	Limit(limit uint) ISelectBuilder
	LimitNamed(name string) ISelectBuilder
	LimitPerPartition(limit uint) ISelectBuilder
	LimitPerPartitionNamed(name string) ISelectBuilder
	AllowFiltering() ISelectBuilder
	BypassCache() ISelectBuilder
	Count(column string) ISelectBuilder
	CountAll() ISelectBuilder
	Min(column string) ISelectBuilder
	Max(column string) ISelectBuilder
	Avg(column string) ISelectBuilder
	Sum(column string) ISelectBuilder
}

type SelectBuilder struct {
	SB *qb.SelectBuilder
}

func (sb *SelectBuilder) ToCql() (stmt string, names []string) {
	return sb.SB.ToCql()
}

func (sb *SelectBuilder) Query(session *igocqlx.Session) igocqlx.IQueryx {
	gocqlxSession := session.S

	queryx := sb.SB.Query(*gocqlxSession)

	return &igocqlx.Queryx{
		Q: queryx,
	}
}

func (sb *SelectBuilder) QueryContext(ctx context.Context, session *igocqlx.Session) igocqlx.IQueryx {
	gocqlxSession := session.S

	queryx := sb.SB.QueryContext(ctx, *gocqlxSession)

	return &igocqlx.Queryx{
		Q: queryx,
	}
}

func (sb *SelectBuilder) From(table string) ISelectBuilder {
	selectBuilderx := sb.SB.From(table)

	sb.SB = selectBuilderx

	return sb
}

func (sb *SelectBuilder) Json() ISelectBuilder {
	selectBuilderx := sb.SB.Json()

	sb.SB = selectBuilderx

	return sb
}

func (sb *SelectBuilder) Columns(columns ...string) ISelectBuilder {
	selectBuilderx := sb.SB.Columns(columns...)

	sb.SB = selectBuilderx

	return sb
}

func (sb *SelectBuilder) Distinct(columns ...string) ISelectBuilder {
	selectBuilderx := sb.SB.Distinct(columns...)

	sb.SB = selectBuilderx

	return sb
}

func (sb *SelectBuilder) Timeout(d time.Duration) ISelectBuilder {
	selectBuilderx := sb.SB.Timeout(d)

	sb.SB = selectBuilderx

	return sb
}

func (sb *SelectBuilder) TimeoutNamed(name string) ISelectBuilder {
	selectBuilderx := sb.SB.TimeoutNamed(name)

	sb.SB = selectBuilderx

	return sb
}

func (sb *SelectBuilder) Where(w ...qb.Cmp) ISelectBuilder {
	selectBuilderx := sb.SB.Where(w...)

	sb.SB = selectBuilderx

	return sb
}

func (sb *SelectBuilder) GroupBy(columns ...string) ISelectBuilder {
	selectBuilderx := sb.SB.GroupBy(columns...)

	sb.SB = selectBuilderx

	return sb
}

func (sb *SelectBuilder) OrderBy(column string, o qb.Order) ISelectBuilder {
	selectBuilderx := sb.SB.OrderBy(column, o)

	sb.SB = selectBuilderx

	return sb
}

func (sb *SelectBuilder) Limit(limit uint) ISelectBuilder {
	selectBuilderx := sb.SB.Limit(limit)

	sb.SB = selectBuilderx

	return sb
}

func (sb *SelectBuilder) LimitNamed(name string) ISelectBuilder {
	selectBuilderx := sb.SB.LimitNamed(name)

	sb.SB = selectBuilderx

	return sb
}

func (sb *SelectBuilder) LimitPerPartition(limit uint) ISelectBuilder {
	selectBuilderx := sb.SB.LimitPerPartition(limit)

	sb.SB = selectBuilderx

	return sb
}

func (sb *SelectBuilder) LimitPerPartitionNamed(name string) ISelectBuilder {
	selectBuilderx := sb.SB.LimitPerPartitionNamed(name)

	sb.SB = selectBuilderx

	return sb
}

func (sb *SelectBuilder) AllowFiltering() ISelectBuilder {
	selectBuilderx := sb.SB.AllowFiltering()

	sb.SB = selectBuilderx

	return sb
}

func (sb *SelectBuilder) BypassCache() ISelectBuilder {
	selectBuilderx := sb.SB.BypassCache()

	sb.SB = selectBuilderx

	return sb
}

func (sb *SelectBuilder) Count(column string) ISelectBuilder {
	selectBuilderx := sb.SB.Columns(column)

	sb.SB = selectBuilderx

	return sb
}

func (sb *SelectBuilder) CountAll() ISelectBuilder {
	selectBuilderx := sb.SB.CountAll()

	sb.SB = selectBuilderx

	return sb
}

func (sb *SelectBuilder) Min(column string) ISelectBuilder {
	selectBuilderx := sb.SB.Min(column)

	sb.SB = selectBuilderx

	return sb
}

func (sb *SelectBuilder) Max(column string) ISelectBuilder {
	selectBuilderx := sb.SB.Max(column)

	sb.SB = selectBuilderx

	return sb
}

func (sb *SelectBuilder) Avg(column string) ISelectBuilder {
	selectBuilderx := sb.SB.Avg(column)

	sb.SB = selectBuilderx

	return sb
}

func (sb *SelectBuilder) Sum(column string) ISelectBuilder {
	selectBuilderx := sb.SB.Sum(column)

	sb.SB = selectBuilderx

	return sb
}
