package igocqlx

import (
	"testing"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/v2"
	"github.com/stretchr/testify/assert"
)

func Test_Iterx_Unsafe(t *testing.T) {
	t.Run("Should return right Unsafe", func(t *testing.T) {
		// arrange
		sut := MakeIterxSut()
		iter := sut.Iterx

		expectedIterx := iter.I.Unsafe()

		// act
		resultIterx := iter.Unsafe()

		// assert
		assert.Equal(t, expectedIterx, resultIterx.(*Iterx).I)
	})
}

func Test_Iterx_StructOnly(t *testing.T) {
	t.Run("Should return right StructOnly", func(t *testing.T) {
		// arrange
		sut := MakeIterxSut()
		iter := sut.Iterx

		expectedIterx := iter.I.StructOnly()

		// act
		resultIterx := iter.StructOnly()

		// assert
		assert.Equal(t, expectedIterx, resultIterx.(*Iterx).I)
	})
}

func Test_Iterx_Get(t *testing.T) {
	t.Run("Should return right Get", func(t *testing.T) {
		// arrange
		sut := MakeIterxSut()
		iter := sut.Iterx

		expectedErr := iter.I.Get(nil)

		// act
		resultErr := iter.Get(nil)

		// assert
		assert.Equal(t, expectedErr, resultErr)
	})
}

func Test_Iterx_Select(t *testing.T) {
	t.Run("Should return right Select", func(t *testing.T) {
		// arrange
		sut := MakeIterxSut()
		iter := sut.Iterx

		expectedErr := iter.I.Select(nil)

		// act
		resultErr := iter.Select(nil)

		// assert
		assert.Equal(t, expectedErr, resultErr)
	})
}

func Test_Iterx_StructScan(t *testing.T) {
	t.Run("Should return right StructScan", func(t *testing.T) {
		// arrange
		sut := MakeIterxSut()
		iter := sut.Iterx

		expectedErr := iter.I.StructScan(nil)

		// act
		resultErr := iter.StructScan(nil)

		// assert
		assert.Equal(t, expectedErr, resultErr)
	})
}

func Test_Iterx_Scan(t *testing.T) {
	t.Run("Should return right Scan", func(t *testing.T) {
		// arrange
		sut := MakeIterxSut()
		iter := sut.Iterx

		expectedErr := iter.I.Scan(nil)

		// act
		resultErr := iter.Scan(nil)

		// assert
		assert.Equal(t, expectedErr, resultErr)
	})
}

func Test_Iterx_Close(t *testing.T) {
	t.Run("Should return right Close", func(t *testing.T) {
		// arrange
		sut := MakeIterxSut()
		iter := sut.Iterx

		expectedErr := iter.I.Close()

		// act
		resultErr := iter.Close()

		// assert
		assert.Equal(t, expectedErr, resultErr)
	})
}

func Test_Iterx_MapScan(t *testing.T) {
	t.Run("Should return right MapScan", func(t *testing.T) {
		// arrange
		sut := MakeIterxSut()
		iter := sut.Iterx

		expectedErr := iter.I.MapScan(nil)

		// act
		resultErr := iter.MapScan(nil)

		// assert
		assert.Equal(t, expectedErr, resultErr)
	})
}

/* ============= SUT helpers ============ */

type iterxSut struct {
	Iterx *Iterx
}

func MakeIterxSut() iterxSut {
	gocqlxIterx := MakeGocqlxIterxSut()

	return iterxSut{
		Iterx: &Iterx{
			I: gocqlxIterx.Sut,
		},
	}
}

type iterGocqlxSut struct {
	Sut *gocqlx.Iterx
}

func MakeGocqlxIterxSut() iterGocqlxSut {

	sut := &gocqlx.Iterx{
		Iter:   &gocql.Iter{},
		Mapper: gocqlx.DefaultMapper,
	}

	return iterGocqlxSut{
		Sut: sut,
	}
}
