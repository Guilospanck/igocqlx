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
	BindMap(arg map[string]interface{}) IQueryx
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

	Release()
	Scan(dest ...interface{}) error
}

type Queryx struct {
	Q *gocqlx.Queryx
}

func (q *Queryx) WithBindTransformer(tr gocqlx.Transformer) IQueryx {
	queryx := q.Q.WithBindTransformer(tr)

	q.Q = queryx

	return q
}

func (q *Queryx) BindStruct(arg interface{}) IQueryx {
	queryx := q.Q.BindStruct(arg)

	q.Q = queryx

	return q
}

func (q *Queryx) BindStructMap(arg0 interface{}, arg1 map[string]interface{}) IQueryx {
	queryx := q.Q.BindStructMap(arg0, arg1)

	q.Q = queryx

	return q
}

func (q *Queryx) BindMap(arg map[string]interface{}) IQueryx {
	queryx := q.Q.BindMap(arg)

	q.Q = queryx

	return q
}

func (q *Queryx) Bind(v ...interface{}) IQueryx {
	queryx := q.Q.Bind(v)

	q.Q = queryx

	return q
}

func (q *Queryx) Err() error {
	return q.Q.Err()
}

func (q *Queryx) Exec() error {
	return q.Q.Exec()
}

func (q *Queryx) ExecRelease() error {
	return q.Q.ExecRelease()
}

func (q *Queryx) ExecCAS() (applied bool, err error) {
	return q.Q.ExecCAS()
}

func (q *Queryx) ExecCASRelease() (bool, error) {
	return q.Q.ExecCASRelease()
}

func (q *Queryx) Get(dest interface{}) error {
	return q.Q.Get(dest)
}

func (q *Queryx) GetRelease(dest interface{}) error {
	return q.Q.GetRelease(dest)
}

func (q *Queryx) GetCAS(dest interface{}) (applied bool, err error) {
	return q.Q.GetCAS(dest)
}

func (q *Queryx) GetCASRelease(dest interface{}) (bool, error) {
	return q.Q.GetCASRelease(dest)
}

func (q *Queryx) Select(dest interface{}) error {
	return q.Q.Select(dest)
}

func (q *Queryx) SelectRelease(dest interface{}) error {
	return q.Q.SelectRelease(dest)
}

func (q *Queryx) Iter() IIterx {
	iterx := q.Q.Iter()

	return &Iterx{
		I: iterx,
	}
}

func (q *Queryx) Consistency(c gocql.Consistency) IQueryx {
	queryx := q.Q.Consistency(c)

	q.Q = queryx

	return q
}

func (q *Queryx) CustomPayload(customPayload map[string][]byte) IQueryx {
	queryx := q.Q.CustomPayload(customPayload)

	q.Q = queryx

	return q
}

func (q *Queryx) Trace(trace gocql.Tracer) IQueryx {
	queryx := q.Q.Trace(trace)

	q.Q = queryx

	return q
}

func (q *Queryx) Observer(observer gocql.QueryObserver) IQueryx {
	queryx := q.Q.Observer(observer)

	q.Q = queryx

	return q
}

func (q *Queryx) PageSize(n int) IQueryx {
	queryx := q.Q.PageSize(n)

	q.Q = queryx

	return q
}

func (q *Queryx) DefaultTimestamp(enable bool) IQueryx {
	queryx := q.Q.DefaultTimestamp(enable)

	q.Q = queryx

	return q
}

func (q *Queryx) WithTimestamp(timestamp int64) IQueryx {
	queryx := q.Q.WithTimestamp(timestamp)

	q.Q = queryx

	return q
}

func (q *Queryx) RoutingKey(routingKey []byte) IQueryx {
	queryx := q.Q.RoutingKey(routingKey)

	q.Q = queryx

	return q
}

func (q *Queryx) WithContext(ctx context.Context) IQueryx {
	queryx := q.Q.WithContext(ctx)

	q.Q = queryx

	return q
}

func (q *Queryx) Prefetch(p float64) IQueryx {
	queryx := q.Q.Prefetch(p)

	q.Q = queryx

	return q
}

func (q *Queryx) RetryPolicy(r gocql.RetryPolicy) IQueryx {
	queryx := q.Q.RetryPolicy(r)

	q.Q = queryx

	return q
}

func (q *Queryx) SetSpeculativeExecutionPolicy(sp gocql.SpeculativeExecutionPolicy) IQueryx {
	queryx := q.Q.SetSpeculativeExecutionPolicy(sp)

	q.Q = queryx

	return q
}

func (q *Queryx) Idempotent(value bool) IQueryx {
	queryx := q.Q.Idempotent(value)

	q.Q = queryx

	return q
}

func (q *Queryx) SerialConsistency(cons gocql.SerialConsistency) IQueryx {
	queryx := q.Q.SerialConsistency(cons)

	q.Q = queryx

	return q
}

func (q *Queryx) PageState(state []byte) IQueryx {
	queryx := q.Q.PageState(state)

	q.Q = queryx

	return q
}

func (q *Queryx) NoSkipMetadata() IQueryx {
	queryx := q.Q.NoSkipMetadata()

	q.Q = queryx

	return q
}

func (q *Queryx) Release() {
	q.Q.Release()
}

func (q *Queryx) Scan(dest ...interface{}) error {
	return q.Q.Scan(dest...)
}
