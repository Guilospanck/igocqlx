package igocqlxqb

import (
	"context"
	"time"

	"github.com/Guilospanck/igocqlx"
	"github.com/scylladb/gocqlx/v2/qb"
)

type IDeleteBuilder interface {
	ToCql() (stmt string, names []string)
	Query(session *igocqlx.Session) igocqlx.IQueryx
	QueryContext(ctx context.Context, session *igocqlx.Session) igocqlx.IQueryx
	From(table string) IDeleteBuilder
	Columns(columns ...string) IDeleteBuilder
	Timestamp(t time.Time) IDeleteBuilder
	TimestampNamed(name string) IDeleteBuilder
	Timeout(d time.Duration) IDeleteBuilder
	TimeoutNamed(name string) IDeleteBuilder
	Where(w ...qb.Cmp) IDeleteBuilder
	If(w ...qb.Cmp) IDeleteBuilder
	Existing() IDeleteBuilder
}

type DeleteBuilder struct {
	DB *qb.DeleteBuilder
}

func (db *DeleteBuilder) ToCql() (stmt string, names []string) {
	return db.DB.ToCql()
}

func (db *DeleteBuilder) Query(session *igocqlx.Session) igocqlx.IQueryx {
	gocqlxSession := session.S

	queryx := db.DB.Query(*gocqlxSession)

	return &igocqlx.Queryx{
		Q: queryx,
	}
}

func (db *DeleteBuilder) QueryContext(ctx context.Context, session *igocqlx.Session) igocqlx.IQueryx {
	gocqlxSession := session.S

	queryx := db.DB.QueryContext(ctx, *gocqlxSession)

	return &igocqlx.Queryx{
		Q: queryx,
	}
}

func (db *DeleteBuilder) From(table string) IDeleteBuilder {
	deleteBuilderx := db.DB.From(table)

	db.DB = deleteBuilderx

	return db
}

func (db *DeleteBuilder) Columns(columns ...string) IDeleteBuilder {
	deleteBuilderx := db.DB.Columns(columns...)

	db.DB = deleteBuilderx

	return db
}

func (db *DeleteBuilder) Timestamp(t time.Time) IDeleteBuilder {
	deleteBuilderx := db.DB.Timestamp(t)

	db.DB = deleteBuilderx

	return db
}

func (db *DeleteBuilder) TimestampNamed(name string) IDeleteBuilder {
	deleteBuilderx := db.DB.TimestampNamed(name)

	db.DB = deleteBuilderx

	return db
}

func (db *DeleteBuilder) Timeout(d time.Duration) IDeleteBuilder {
	deleteBuilderx := db.DB.Timeout(d)

	db.DB = deleteBuilderx

	return db
}

func (db *DeleteBuilder) TimeoutNamed(name string) IDeleteBuilder {
	deleteBuilderx := db.DB.TimeoutNamed(name)

	db.DB = deleteBuilderx

	return db
}

func (db *DeleteBuilder) Where(w ...qb.Cmp) IDeleteBuilder {
	deleteBuilderx := db.DB.Where(w...)

	db.DB = deleteBuilderx

	return db
}

func (db *DeleteBuilder) If(w ...qb.Cmp) IDeleteBuilder {
	deleteBuilderx := db.DB.If(w...)

	db.DB = deleteBuilderx

	return db
}

func (db *DeleteBuilder) Existing() IDeleteBuilder {
	deleteBuilderx := db.DB.Existing()

	db.DB = deleteBuilderx

	return db
}
