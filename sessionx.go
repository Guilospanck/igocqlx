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
	S *gocqlx.Session
}

func (s *Session) ContextQuery(ctx context.Context, stmt string, names []string) IQueryx {
	queryx := s.S.ContextQuery(ctx, stmt, names)

	return &Queryx{
		Q: queryx,
	}
}

func (s *Session) Query(stmt string, names []string) IQueryx {
	queryx := s.S.Query(stmt, names)

	return &Queryx{
		Q: queryx,
	}
}

func (s *Session) ExecStmt(stmt string) error {
	return s.S.ExecStmt(stmt)
}

func (s *Session) AwaitSchemaAgreement(ctx context.Context) error {
	return s.S.AwaitSchemaAgreement(ctx)
}

func (s *Session) Close() {
	s.S.Close()
}

func NewSession(session *gocql.Session) ISessionx {
	gocqlxSession := gocqlx.NewSession(session)
	return &Session{
		&gocqlxSession,
	}
}

func WrapSession(session *gocql.Session, err error) (ISessionx, error) {
	gocqlxSession, wrapErr := gocqlx.WrapSession(session, err)

	return &Session{
		&gocqlxSession,
	}, wrapErr
}
