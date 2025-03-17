package wale

import (
	"context"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
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
	consumerHealth            ConsumerHealth

	// metrics function
	WalStandyStatusUpdateCounter func(context.Context, string, string)
}

type Config struct {
	ReplicationSlot       string
	Publications          string
	WalConsumerHealthPort int
}

type Log struct {
	ctx           context.Context
	handlerIndex  int
	walController *WALController
	rawMsg        pgproto3.BackendMessage
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
