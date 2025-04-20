package wal_e

import (
	"context"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

type Handler func(*Log)

type WALController struct {
	ctx                       context.Context
	cancel                    context.CancelFunc
	config                    *Config
	masterDbConn              *pgx.Conn
	replicationConn           *pgconn.PgConn
	lastLSN                   pglogrepl.LSN
	lastEmptyBatchPkmSentTime time.Time
	handlers                  []Handler
	ConsumerHealth            ConsumerHealth
	relationCache             map[uint32]RelationData

	// metrics function
	WalStandyStatusUpdateCounter func(context.Context, string, string)
	ReplicaLagMetricFunc         func(context.Context, int64)
	RecoverFromPanic             func() func()
}

type Config struct {
	ReplicationSlot       string
	Publications          string
	WalConsumerHealthPort int
	ExtraPluginArgs       []string
}

type Log struct {
	Ctx           context.Context
	handlerIndex  int
	walController *WALController
	RawMsg        pgproto3.BackendMessage
	Wal           *Wal
}

type Column string
type Operation string
type Table string

const (
	Insert Operation = "INSERT"
	Update Operation = "UPDATE"
	Delete Operation = "DELETE"
)

type RelationData struct {
	Relation    string
	Columns     []string
	ColumnTypes map[string]uint32
}

type TableAction struct {
	Operation    Operation
	TableName    Table
	Values       map[string]any
	OldValues    map[string]any
	DateModified time.Time
}

type LogicalMessage struct {
	Prefix  string
	Content []byte
}
type Wal struct {
	TableAction    *TableAction
	LogicalMessage *LogicalMessage
	LSN            pglogrepl.LSN
}

func (r *Log) Next() {
	r.handlerIndex++
	if r.handlerIndex < len(r.walController.handlers) {
		r.walController.handlers[r.handlerIndex](r)
	}
}

func (r *Log) Start() {
	r.walController.handlers[r.handlerIndex](r)
}
