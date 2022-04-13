package table

import (
	"context"

	"github.com/Guilospanck/igocqlx"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/gocqlx/v2/table"
)

type ITable interface {
	Metadata() table.Metadata
	PrimaryKeyCmp() []qb.Cmp
	Name() string
	Get(columns ...string) (stmt string, names []string)
	GetQuery(session igocqlx.ISessionx, columns ...string) igocqlx.IQueryx
	GetQueryContext(ctx context.Context, session igocqlx.ISessionx, columns ...string) igocqlx.IQueryx
	Select(columns ...string) (stmt string, names []string)
	SelectQuery(session igocqlx.ISessionx, columns ...string) igocqlx.IQueryx
	SelectQueryContext(ctx context.Context, session igocqlx.ISessionx, columns ...string) igocqlx.IQueryx
	SelectBuilder(columns ...string) *qb.SelectBuilder
	SelectAll() (stmt string, names []string)
	Insert() (stmt string, names []string)
	InsertQuery(session igocqlx.ISessionx) igocqlx.IQueryx
	InsertQueryContext(ctx context.Context, session igocqlx.ISessionx) igocqlx.IQueryx
	InsertBuilder() *qb.InsertBuilder
	Update(columns ...string) (stmt string, names []string)
	UpdateQuery(session igocqlx.ISessionx, columns ...string) igocqlx.IQueryx
	UpdateQueryContext(ctx context.Context, session igocqlx.ISessionx, columns ...string) igocqlx.IQueryx
	UpdateBuilder(columns ...string) *qb.UpdateBuilder
	Delete(columns ...string) (stmt string, names []string)
	DeleteQuery(session igocqlx.ISessionx, columns ...string) igocqlx.IQueryx
	DeleteQueryContext(ctx context.Context, session igocqlx.ISessionx, columns ...string) igocqlx.IQueryx
	DeleteBuilder(columns ...string) *qb.DeleteBuilder
}

type Table struct {
	*table.Table
}

type Metadata struct {
	*table.Metadata
}

func New(m table.Metadata) *table.Table {
	return table.New(m)
}
