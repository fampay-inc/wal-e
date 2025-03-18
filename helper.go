package wal_e

import (
	"fmt"
	"time"

	"github.com/jackc/pglogrepl"
)

func (wc *WALController) processCopyData(msgData []byte) *Wal {
	defer wc.RecoverFromPanic()
	// Parse the message
	fmt.Printf(">> msgData %v\n", msgData[0])
	switch msgData[0] {
	case pglogrepl.XLogDataByteID:
		xld, err := pglogrepl.ParseXLogData(msgData[1:])
		if err != nil {
			// wc.logger.Fatalf("Failed to parse XLogData: %v", err)
			panic(err)
		}
		logicalMsg, err := pglogrepl.Parse(xld.WALData)
		if err != nil {
			// wc.logger.Fatalf("Failed to parse logical message: %v", err)
			panic(err)
		}
		switch msg := logicalMsg.(type) {
		case *pglogrepl.RelationMessage:
			wc.processRelationMessage(msg)
		case *pglogrepl.InsertMessage:
			return wc.processInsertMessage(msg)
		case *pglogrepl.UpdateMessage:
			return wc.processUpdateMessage(msg)
		case *pglogrepl.DeleteMessage:
			return wc.processDeleteMessage(msg)
		}
		wc.lastLSN = xld.ServerWALEnd
	case pglogrepl.PrimaryKeepaliveMessageByteID:
		pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msgData[1:])
		if err != nil {
			// wc.logger.Fatalf("Failed to parse PrimaryKeepaliveMessage: %v", err)
			panic(err)
		}
		if pkm.ReplyRequested && time.Now().After(wc.lastEmptyBatchPkmSentTime.Add(10*time.Second)) {
			wc.lastEmptyBatchPkmSentTime = time.Now()
			wc.SendStandbyStatusUpdate()
		}
		wc.lastLSN = pkm.ServerWALEnd
	default:
		// wc.logger.Fatalf("Unknown message type: %v", msgData[0])
		panic(fmt.Errorf("Unknown message type: %v", msgData[0]))
	}
	return nil
}

func (wc *WALController) processRelationMessage(msg *pglogrepl.RelationMessage) {
	columns := make([]Column, len(msg.Columns))
	for i, col := range msg.Columns {
		columns[i] = Column(col.Name)
	}
	wc.relationCache[msg.RelationID] = RelationData{
		Columns:  columns,
		Relation: msg.RelationName,
	}
}

func (wc *WALController) processInsertMessage(msg *pglogrepl.InsertMessage) *Wal {
	relationColumns := wc.relationCache[msg.RelationID].Columns
	var walLog = Wal{
		Values: make(map[Column][]byte),
	}
	for i := 0; i < len(relationColumns); i++ {
		walLog.Values[relationColumns[i]] = msg.Tuple.Columns[i].Data
	}
	walLog.Operation = Insert
	walLog.TableName = Table(wc.relationCache[msg.RelationID].Relation)
	return &walLog
}

func (wc *WALController) processUpdateMessage(msg *pglogrepl.UpdateMessage) *Wal {
	relationColumns := wc.relationCache[msg.RelationID].Columns
	var walLog = Wal{
		Values: make(map[Column][]byte),
	}
	for i := 0; i < len(relationColumns); i++ {
		walLog.Values[relationColumns[i]] = msg.NewTuple.Columns[i].Data
	}
	walLog.Operation = Update
	walLog.TableName = Table(wc.relationCache[msg.RelationID].Relation)
	return &walLog
}

func (wc *WALController) processDeleteMessage(msg *pglogrepl.DeleteMessage) *Wal {
	relationColumns := wc.relationCache[msg.RelationID].Columns
	var walLog = Wal{
		Values: make(map[Column][]byte),
	}
	for i := 0; i < len(relationColumns); i++ {
		walLog.Values[relationColumns[i]] = msg.OldTuple.Columns[i].Data
	}
	walLog.Operation = Delete
	walLog.TableName = Table(wc.relationCache[msg.RelationID].Relation)
	return &walLog
}

// func (wc *WALController) processWalLog(ctx context.Context, walLog *waldom.WALLog) error {
// err := wc.ivUc.ProcessWalLog(ctx, walLog)
// if err != nil {
// 	wc.logger.Errorf("Error processing WAL log: %v", err)
// 	return err
// }
// wc.logger.Debugf("Processed WAL log, operation=%s, table=%s", walLog.Operation, walLog.TableName)
// return nil
// }
