package wal_e

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgproto3"
)

func (wc *WALController) AddHandlers(handlers ...Handler) {
	wc.handlers = append(wc.handlers, handlers...)
}

func (wc *WALController) NewLog(ctx context.Context, rawMsg pgproto3.BackendMessage) *Log {
	return &Log{
		Ctx:           ctx,
		handlerIndex:  0,
		walController: wc,
		RawMsg:        rawMsg,
	}
}

func (wc *WALController) InitConsumer() error {
	var strLSN string
	err := wc.masterDbConn.QueryRow(wc.ctx, "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = $1;", wc.config.ReplicationSlot).Scan(&strLSN)
	if err != nil {
		return err
	}
	wc.lastLSN, err = pglogrepl.ParseLSN(strLSN)
	if err != nil {
		return err
	}

	// Start replication
	err = pglogrepl.StartReplication(wc.ctx, wc.replicationConn, wc.config.ReplicationSlot, wc.lastLSN, pglogrepl.StartReplicationOptions{
		PluginArgs: []string{
			"proto_version '1'",
			"publication_names '" + wc.config.Publications + "'",
		},
	})
	if err != nil {
		return err
	}
	return nil
}

func (wc *WALController) SendStandbyStatusUpdate() error {
	err := pglogrepl.SendStandbyStatusUpdate(wc.ctx, wc.replicationConn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: wc.lastLSN,
		WALFlushPosition: wc.lastLSN,
		WALApplyPosition: wc.lastLSN,
	})
	if err != nil {
		wc.ConsumerHealth.SetHealth(false)
		wc.WalStandyStatusUpdateCounter(wc.ctx, "error", err.Error())
		return err
	}
	wc.ConsumerHealth.SetHealth(true)
	return nil
}

func (wc *WALController) GetReplicationLag() {
	ticker := time.NewTicker(5 * time.Second)
	for {
		select {
		case <-wc.ctx.Done():
			return
		case <-ticker.C:
			// get server wal write position
			var walWritePos string
			wc.masterDbConn.QueryRow(wc.ctx, "SELECT pg_current_wal_insert_lsn()").Scan(&walWritePos)
			walWriteLSN, err := pglogrepl.ParseLSN(walWritePos)
			if err != nil {
				continue
			}
			var replicationLag = walWriteLSN - wc.lastLSN
			wc.ReplicaLagMetricFunc(wc.ctx, int64(replicationLag))
		}
	}
}

func (wc *WALController) Consume(wg *sync.WaitGroup) error {
	defer wg.Done()
	for {
		select {
		case <-wc.ctx.Done():
			wc.ConsumerHealth.Shutdown()
			wc.replicationConn.Close(wc.ctx)
			wc.masterDbConn.Close(wc.ctx)
			return nil
		default:
			newCtx := wc.ctx
			rawMsg, err := wc.replicationConn.ReceiveMessage(newCtx)
			if err != nil {
				wc.ConsumerHealth.SetHealth(false)
				return err
			}
			req := wc.NewLog(newCtx, rawMsg)
			req.Start()
		}
	}
}

func (wc *WALController) StopConsumer() {
	wc.cancel()
}

func (wc *WALController) SendPeriodicStandbyStatusUpdate() {
	ticker := time.NewTicker(10 * time.Second)
	for {
		select {
		case <-wc.ctx.Done():
			return
		case <-ticker.C:
			wc.SendStandbyStatusUpdate()
		}
	}
}

func (wc *WALController) ProcessWalLog(req *Log) {
	fmt.Printf("%+v\n", req.RawMsg)
	switch msg := req.RawMsg.(type) {
	case *pgproto3.CopyData:
		walLog := wc.processCopyData(msg.Data)
		if walLog != nil {
			req.Wal = walLog
		} else {
			fmt.Println("< WAL is NIL >")
		}
		// if walLog != nil {
		// 	err := wc.processWalLog(req.ctx, walLog)
		// 	if err != nil {
		// 		errdom.RaiseToSentry(req.ctx, err)
		// 	}
		// 	updatedAtOfWal, err := wc.ivUc.GetUpdatedAtOfWal(req.ctx, walLog)
		// 	if err != nil {
		// 		errdom.RaiseToSentry(req.ctx, err)
		// 	}
		// 	emitMetrics(currentTime, updatedAtOfWal, err)
		// 	metrics.WalTableLogCounter.WithLabelValues(string(walLog.TableName), string(walLog.Operation)).Inc()
		// } else {
		// 	metrics.WalProcessedCounter.WithLabelValues("non_data", "success", "").Inc()
		// 	metrics.WalProcessingLatency.WithLabelValues("non_data").Observe(time.Since(currentTime).Seconds())
		// }
		fmt.Println("> Processed CopyData")
		fmt.Printf("%+v\n", req.Wal)
		wc.ConsumerHealth.SetHealth(true)
		req.Next()
	case *pgproto3.ErrorResponse:
		fmt.Printf("This is error, %+v\n", msg)
		// wc.logger.Errorf("Error response from server: %v", msg)

	default:
		fmt.Printf("Unknown message type, %+v\n", msg)
		// wc.logger.Errorf("Unknown message type: %v", msg)
	}
}
