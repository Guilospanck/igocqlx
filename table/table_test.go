package igocqlxtable

import (
	"testing"

	"github.com/scylladb/gocqlx/v2/table"
	"github.com/stretchr/testify/assert"
)

func Test_Metadata(t *testing.T) {
	t.Run("", func(t *testing.T) {
		// arrange
		name := "mutant_data"
		columns := []string{"first_name", "last_name", "address", "picture_location"}
		partKey := []string{"first_name", "last_name"}

		metadata := Metadata{
			&table.Metadata{
				Name:    name,
				Columns: columns,
				PartKey: partKey,
			},
		}

		assert.Equal(t, metadata.Name, name)
		assert.Equal(t, metadata.Columns, columns)
		assert.Equal(t, metadata.PartKey, partKey)
	})
}

func Test_Table(t *testing.T) {
	t.Run("", func(t *testing.T) {
		// arrange
		name := "mutant_data"
		columns := []string{"first_name", "last_name", "address", "picture_location"}
		partKey := []string{"first_name", "last_name"}

		metadata := Metadata{
			&table.Metadata{
				Name:    name,
				Columns: columns,
				PartKey: partKey,
			},
		}

		tableTest := New(*metadata.Metadata)

		assert.Equal(t, tableTest.Metadata().Name, name)
		assert.Equal(t, tableTest.Metadata().Columns, columns)
		assert.Equal(t, tableTest.Metadata().PartKey, partKey)
	})
}
