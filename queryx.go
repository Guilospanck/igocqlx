package igocqlx

import (
	"context"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/v2"
)

type IQueryx interface {
	WithBindTransformer(tr gocqlx.Transformer) IQueryx
	BindStruct(arg interface{}) IQueryx
	BindStructMap(arg0 interface{}, arg1 map[string]interface{}) IQueryx
	bindStructArgs(arg0 interface{}, arg1 map[string]interface{}) ([]interface{}, error)
	BindMap(arg map[string]interface{}) IQueryx
	bindMapArgs(arg map[string]interface{}) ([]interface{}, error)
	Bind(v ...interface{}) IQueryx
	Err() error
	Exec() error
	ExecRelease() error
	ExecCAS() (applied bool, err error)
	ExecCASRelease() (bool, error)
	Get(dest interface{}) error
	GetRelease(dest interface{}) error
	GetCAS(dest interface{}) (applied bool, err error)
	GetCASRelease(dest interface{}) (bool, error)
	Select(dest interface{}) error
	SelectRelease(dest interface{}) error
	Iter() IIterx

	Consistency(c gocql.Consistency) IQueryx
	CustomPayload(customPayload map[string][]byte) IQueryx
	Trace(trace gocql.Tracer) IQueryx
	Observer(observer gocql.QueryObserver) IQueryx
	PageSize(n int) IQueryx
	DefaultTimestamp(enable bool) IQueryx
	WithTimestamp(timestamp int64) IQueryx
	RoutingKey(routingKey []byte) IQueryx
	WithContext(ctx context.Context) IQueryx
	Prefetch(p float64) IQueryx
	RetryPolicy(r gocql.RetryPolicy) IQueryx
	SetSpeculativeExecutionPolicy(sp gocql.SpeculativeExecutionPolicy) IQueryx
	Idempotent(value bool) IQueryx
	SerialConsistency(cons gocql.SerialConsistency) IQueryx
	PageState(state []byte) IQueryx
	NoSkipMetadata() IQueryx

	Release() error
	Scan(dest ...interface{}) error
}

type Queryx struct {
	*gocqlx.Queryx
}
