package igocqlx

import (
	"context"
	"fmt"
	"testing"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/v2"
	"github.com/stretchr/testify/assert"
)

func Test_Sessionx_ContextQuery(t *testing.T) {
	t.Run("Should return right ContextQuery", func(t *testing.T) {
		// arrange
		sut := MakeSessionSut()
		session := sut.Session
		ctx := context.Background()
		stmt := `INSERT INTO tracking_data (first_name,last_name,location) VALUES (?,?,?) `
		names := []string{"first_name", "last_name", "location"}

		expectedQuery := session.S.ContextQuery(ctx, stmt, names)

		// act
		resultQuery := session.ContextQuery(ctx, stmt, names)

		// assert
		assert.Equal(t, expectedQuery, resultQuery.(*Queryx).Q)
	})
}

func Test_Sessionx_Query(t *testing.T) {
	t.Run("Should return right Query", func(t *testing.T) {
		// arrange
		sut := MakeSessionSut()
		session := sut.Session
		stmt := `INSERT INTO tracking_data (first_name,last_name,location) VALUES (?,?,?) `
		names := []string{"first_name", "last_name", "location"}

		expectedQuery := session.S.Query(stmt, names)

		// act
		resultQuery := session.Query(stmt, names)

		// assert
		assert.Equal(t, expectedQuery, resultQuery.(*Queryx).Q)
	})
}

func Test_Sessionx_Close(t *testing.T) {
	t.Run("Should return right Close", func(t *testing.T) {
		// arrange
		sut := MakeSessionSut()
		session := sut.Session

		// act
		session.Close()

		// assert
		assert.NotPanics(t, session.Close)
	})
}

func Test_Sessionx_NewSession(t *testing.T) {
	t.Run("Should return right NewSession", func(t *testing.T) {
		// arrange
		sut := MakeSessionSut()
		session := sut.Session

		expectedSession := gocqlx.NewSession(session.S.Session)

		// act
		resultSession := NewSession(session.S.Session)

		// assert
		assert.Equal(t, &expectedSession, resultSession.(*Session).S)
	})
}

func Test_Sessionx_WrapSession(t *testing.T) {
	t.Run("Should return right WrapSession", func(t *testing.T) {
		// arrange
		sut := MakeSessionSut()
		session := sut.Session
		err := fmt.Errorf("test")

		expectedSession, expectedErr := gocqlx.WrapSession(session.S.Session, err)

		// act
		resultSession, resultErr := WrapSession(session.S.Session, err)

		// assert
		assert.Equal(t, &expectedSession, resultSession.(*Session).S)
		assert.Equal(t, expectedErr, resultErr)
	})
}

/* ============= SUT helpers ============ */

type sessionSut struct {
	Session *Session
}

func MakeSessionSut() sessionSut {
	gocqlxsession := MakeGocqlxSessionSut()

	return sessionSut{
		Session: &Session{
			S: gocqlxsession.Sut,
		},
	}
}

type sessionGocqlxSut struct {
	Sut *gocqlx.Session
}

func MakeGocqlxSessionSut() sessionGocqlxSut {

	sut := &gocqlx.Session{
		Session: &gocql.Session{},
	}

	return sessionGocqlxSut{
		Sut: sut,
	}
}
