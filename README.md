# igocqlx [![Go Report Card](https://goreportcard.com/badge/github.com/Guilospanck/igocqlx)](https://goreportcard.com/report/github.com/Guilospanck/igocqlx) [![CircleCI](https://circleci.com/gh/Guilospanck/igocqlx/tree/main.svg?style=svg)](https://circleci.com/gh/Guilospanck/igocqlx/tree/main) [![codecov](https://codecov.io/gh/Guilospanck/igocqlx/branch/main/graph/badge.svg?token=WTY8VZTGD0)](https://codecov.io/gh/Guilospanck/igocqlx)
Gocqlx with interfaces.

`igocqlx` has (almost, to be continued) functions as `gocqlx` but provides interfaces to wrap functions from the package.
This is designed to make it easier to mock calls.

You can check it out [`gocqlxmock`](https://github.com/Guilospanck/gocqlxmock), a simple package to mock your `igocqlx` calls.

## Installation
```bash
go get github.com/Guilospanck/igocqlx
```

## How to use
The best way to teach how to use something is by examples. Here you can see examples showing the differences between a normal `gocqlx` and a `igocqlx` calls.

### tracking_data_model.go
```go
package models

import "github.com/scylladb/gocqlx/v2/table"

type TrackingDataTable struct {
	Table *table.Table
}

func NewTrackingDataTable() *TrackingDataTable {
	trackingDataMetadata := table.Metadata{
		Name: "tracking_data",
		Columns: []string{
			"first_name", "last_name", "timestamp", "heat",
			"location", "speed", "telepathy_powers",
		},
		PartKey: []string{"first_name", "last_name"},
		SortKey: []string{"timestamp"},
	}

	trackingDataTable := table.New(trackingDataMetadata)

	return &TrackingDataTable{
		Table: trackingDataTable,
	}
}
```

```go
// using igocqlx

package models

import (
	igocqlxtable "github.com/Guilospanck/igocqlx/table"
	"github.com/scylladb/gocqlx/v2/table"
)

type TrackingDataTable struct {
	Table igocqlxtable.ITable
}

func NewTrackingDataTable() *TrackingDataTable {
	trackingDataMetadata := igocqlxtable.Metadata{
		M: &table.Metadata{
			Name: "tracking_data",
			Columns: []string{
				"first_name", "last_name", "timestamp", "heat",
				"location", "speed", "telepathy_powers",
			},
			PartKey: []string{"first_name", "last_name"},
			SortKey: []string{"timestamp"},
		},
	}

	trackingDataTable := igocqlxtable.New(*trackingDataMetadata.M)

	return &TrackingDataTable{
		Table: trackingDataTable,
	}
}
```

### connection.go
```go
// using gocqlx

import (
  "github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/v2"
)

func (conn *scyllaDBConnection) createSession(cluster *gocql.ClusterConfig) (*gocqlx.Session, error) {
	session, err := gocqlx.WrapSession(cluster.CreateSession())
	if err != nil {
		conn.logger.Error("An error occurred while creating DB session: ", zap.Error(err))
		return nil, err
	}

	return &session, nil
}
```

```go
// using igocqlx
import (
	"github.com/Guilospanck/igocqlx"
	"github.com/gocql/gocql"
)

func (conn *scyllaDBConnection) createSession(cluster *gocql.ClusterConfig) (igocqlx.ISessionx, error) {
	session, err := igocqlx.WrapSession(cluster.CreateSession())
	if err != nil {
		conn.logger.Error(fmt.Sprintf("An error occurred while creating DB session: %s", err.Error()))
		return nil, err
	}

	return session, nil
}
```
