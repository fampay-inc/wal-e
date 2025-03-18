package wal_e

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func NewWalConsumer(ctx context.Context, masterDBUri string, config *Config) (*WALController, error) {
	masterDBConn, err := pgx.Connect(ctx, masterDBUri)
	if err != nil {
		return nil, err
	}

	replicationURI := masterDBUri + "?replication=database"
	replicationConn, err := pgconn.Connect(ctx, replicationURI)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(ctx)
	walController := &WALController{
		ctx:             ctx,
		cancel:          cancel,
		config:          config,
		masterDbConn:    masterDBConn,
		replicationConn: replicationConn,
	}
	return walController, nil
}
