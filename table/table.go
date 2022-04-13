package igocqlxtable

import (
	"context"

	"github.com/Guilospanck/igocqlx"
	"github.com/Guilospanck/igocqlx/qb"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/gocqlx/v2/table"
)

type ITable interface {
	Metadata() Metadata
	PrimaryKeyCmp() []qb.Cmp
	Name() string
	Get(columns ...string) (stmt string, names []string)
	GetQuery(session *igocqlx.Session, columns ...string) igocqlx.IQueryx
	GetQueryContext(ctx context.Context, session *igocqlx.Session, columns ...string) igocqlx.IQueryx
	Select(columns ...string) (stmt string, names []string)
	SelectQuery(session *igocqlx.Session, columns ...string) igocqlx.IQueryx
	SelectQueryContext(ctx context.Context, session *igocqlx.Session, columns ...string) igocqlx.IQueryx
	SelectBuilder(columns ...string) igocqlxqb.ISelectBuilder
	SelectAll() (stmt string, names []string)
	Insert() (stmt string, names []string)
	InsertQuery(session *igocqlx.Session) igocqlx.IQueryx
	InsertQueryContext(ctx context.Context, session *igocqlx.Session) igocqlx.IQueryx
	InsertBuilder() igocqlxqb.IInsertBuilder
	Update(columns ...string) (stmt string, names []string)
	UpdateQuery(session *igocqlx.Session, columns ...string) igocqlx.IQueryx
	UpdateQueryContext(ctx context.Context, session *igocqlx.Session, columns ...string) igocqlx.IQueryx
	UpdateBuilder(columns ...string) igocqlxqb.IUpdateBuilder
	Delete(columns ...string) (stmt string, names []string)
	DeleteQuery(session *igocqlx.Session, columns ...string) igocqlx.IQueryx
	DeleteQueryContext(ctx context.Context, session *igocqlx.Session, columns ...string) igocqlx.IQueryx
	DeleteBuilder(columns ...string) igocqlxqb.IDeleteBuilder
}

type Metadata struct {
	M *table.Metadata
}

type Table struct {
	T *table.Table
}

func (t *Table) Metadata() Metadata {
	gocqlxmetadata := t.T.Metadata()

	return Metadata{
		M: &gocqlxmetadata,
	}
}

func (t *Table) PrimaryKeyCmp() []qb.Cmp {
	return t.T.PrimaryKeyCmp()
}

func (t *Table) Name() string {
	return t.T.Name()
}

func (t *Table) Get(columns ...string) (stmt string, names []string) {
	return t.T.Get(columns...)
}

func (t *Table) GetQuery(session *igocqlx.Session, columns ...string) igocqlx.IQueryx {
	gocqlxSession := session.S

	queryx := t.T.GetQuery(*gocqlxSession, columns...)

	return &igocqlx.Queryx{
		Q: queryx,
	}
}

func (t *Table) GetQueryContext(ctx context.Context, session *igocqlx.Session, columns ...string) igocqlx.IQueryx {
	gocqlxSession := session.S

	queryx := t.T.GetQueryContext(ctx, *gocqlxSession, columns...)

	return &igocqlx.Queryx{
		Q: queryx,
	}
}

func (t *Table) Select(columns ...string) (stmt string, names []string) {
	return t.T.Select(columns...)
}

func (t *Table) SelectQuery(session *igocqlx.Session, columns ...string) igocqlx.IQueryx {
	gocqlxSession := session.S

	queryx := t.T.SelectQuery(*gocqlxSession, columns...)

	return &igocqlx.Queryx{
		Q: queryx,
	}
}

func (t *Table) SelectQueryContext(ctx context.Context, session *igocqlx.Session, columns ...string) igocqlx.IQueryx {
	gocqlxSession := session.S

	queryx := t.T.SelectQueryContext(ctx, *gocqlxSession, columns...)

	return &igocqlx.Queryx{
		Q: queryx,
	}
}

func (t *Table) SelectBuilder(columns ...string) igocqlxqb.ISelectBuilder {
	selectBuilderx := t.T.SelectBuilder(columns...)

	return &igocqlxqb.SelectBuilder{
		SB: selectBuilderx,
	}
}

func (t *Table) SelectAll() (stmt string, names []string) {
	return t.T.SelectAll()
}

func (t *Table) Insert() (stmt string, names []string) {
	return t.T.Insert()
}

func (t *Table) InsertQuery(session *igocqlx.Session) igocqlx.IQueryx {
	gocqlxSession := session.S

	queryx := t.T.InsertQuery(*gocqlxSession)

	return &igocqlx.Queryx{
		Q: queryx,
	}
}

func (t *Table) InsertQueryContext(ctx context.Context, session *igocqlx.Session) igocqlx.IQueryx {
	gocqlxSession := session.S

	queryx := t.T.InsertQueryContext(ctx, *gocqlxSession)

	return &igocqlx.Queryx{
		Q: queryx,
	}
}

func (t *Table) InsertBuilder() igocqlxqb.IInsertBuilder {
	insertBuilderx := t.T.InsertBuilder()

	return &igocqlxqb.InsertBuilder{
		IB: insertBuilderx,
	}
}

func (t *Table) Update(columns ...string) (stmt string, names []string) {
	return t.T.Update(columns...)
}

func (t *Table) UpdateQuery(session *igocqlx.Session, columns ...string) igocqlx.IQueryx {
	gocqlxSession := session.S

	queryx := t.T.UpdateQuery(*gocqlxSession, columns...)

	return &igocqlx.Queryx{
		Q: queryx,
	}
}

func (t *Table) UpdateQueryContext(ctx context.Context, session *igocqlx.Session, columns ...string) igocqlx.IQueryx {
	gocqlxSession := session.S

	queryx := t.T.UpdateQueryContext(ctx, *gocqlxSession, columns...)

	return &igocqlx.Queryx{
		Q: queryx,
	}
}

func (t *Table) UpdateBuilder(columns ...string) igocqlxqb.IUpdateBuilder {
	updateBuilderx := t.T.UpdateBuilder()

	return &igocqlxqb.UpdateBuilder{
		UB: updateBuilderx,
	}
}

func (t *Table) Delete(columns ...string) (stmt string, names []string) {
	return t.T.Delete(columns...)
}

func (t *Table) DeleteQuery(session *igocqlx.Session, columns ...string) igocqlx.IQueryx {
	gocqlxSession := session.S

	queryx := t.T.DeleteQuery(*gocqlxSession, columns...)

	return &igocqlx.Queryx{
		Q: queryx,
	}
}

func (t *Table) DeleteQueryContext(ctx context.Context, session *igocqlx.Session, columns ...string) igocqlx.IQueryx {
	gocqlxSession := session.S

	queryx := t.T.DeleteQueryContext(ctx, *gocqlxSession, columns...)

	return &igocqlx.Queryx{
		Q: queryx,
	}
}

func (t *Table) DeleteBuilder(columns ...string) igocqlxqb.IDeleteBuilder {
	deleteBuilderx := t.T.DeleteBuilder()

	return &igocqlxqb.DeleteBuilder{
		DB: deleteBuilderx,
	}
}

func New(m table.Metadata) ITable {
	gocqlxTable := table.New(m)
	return &Table{
		T: gocqlxTable,
	}
}
