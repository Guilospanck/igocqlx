package igocqlx

import (
	"context"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/v2"
)

type ISessionx interface {
	ContextQuery(ctx context.Context, stmt string, names []string) IQueryx
	Query(stmt string, names []string) IQueryx
	ExecStmt(stmt string) error
	AwaitSchemaAgreement(ctx context.Context) error
	Close()
}

type Session struct {
	*gocqlx.Session
}

func NewSession(session *gocql.Session) gocqlx.Session {
	return gocqlx.NewSession(session)
}

func WrapSession(session *gocql.Session, err error) (gocqlx.Session, error) {
	return gocqlx.WrapSession(session, err)
}
