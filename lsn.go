package wal_e

import (
	"time"

	"github.com/jackc/pglogrepl"
)

// StartLSNAckWorker starts a worker that listens for LSN to be acknowledged
// pass a channel where lsn will be sent, this channel receives the lsn and
// sends status update to the database in a certain time interval.
func (wc *WALController) StartLSNAckWorker(lsnChan <-chan pglogrepl.LSN) {
	var lsn pglogrepl.LSN
	ticker := time.NewTicker(time.Second * 2)
	for {
		select {
		case receivedLSN := <-lsnChan:
			lsn = receivedLSN
		case <-ticker.C:
			wc.SendStatusUpdate(lsn)
		case <-wc.ctx.Done():
			wc.SendStatusUpdate(lsn)
			return
		}
	}
}
