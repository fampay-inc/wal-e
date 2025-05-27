package wal_e

import (
	"fmt"
	"time"

	"github.com/jackc/pglogrepl"
)

func (wc *WALController) processCopyData(msgData []byte) *Wal {
	defer wc.RecoverFromPanic()
	// Parse the message
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
			return wc.processInsertMessage(msg, xld)
		case *pglogrepl.UpdateMessage:
			return wc.processUpdateMessage(msg, xld)
		case *pglogrepl.DeleteMessage:
			return wc.processDeleteMessage(msg, xld)
		case *pglogrepl.LogicalDecodingMessage:
			return wc.processLogicalMessage(msg, xld)
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

func processOID(oid uint32, data []byte) (any, error) {
	if len(data) == 0 {
		return nil, nil
	}

	convertFunc, ok := OidMap[oid]
	if !ok {
		return string(data), nil
	}

	return convertFunc(data)
}

func (wc *WALController) processRelationMessage(msg *pglogrepl.RelationMessage) {
	columns := make([]string, len(msg.Columns))
	columnType := make(map[string]uint32)
	for i, col := range msg.Columns {
		columns[i] = col.Name
		columnType[col.Name] = col.DataType
	}
	wc.relationCache[msg.RelationID] = RelationData{
		Columns:     columns,
		Relation:    msg.RelationName,
		ColumnTypes: columnType,
	}
}

func (wc *WALController) processInsertMessage(msg *pglogrepl.InsertMessage, walData pglogrepl.XLogData) *Wal {
	relationColumns := wc.relationCache[msg.RelationID].Columns
	var tableAction = TableAction{
		Values: make(map[string]any),
	}
	for i := range len(relationColumns) {
		oid := wc.relationCache[msg.RelationID].ColumnTypes[relationColumns[i]]
		processedValue, err := processOID(oid, msg.Tuple.Columns[i].Data)
		if err != nil {
			fmt.Println(err)
		}
		tableAction.Values[relationColumns[i]] = processedValue
	}
	tableAction.Operation = Insert
	tableAction.TableName = Table(wc.relationCache[msg.RelationID].Relation)
	return &Wal{
		TableAction: &tableAction,
		LSN:         walData.ServerWALEnd,
	}
}

func (wc *WALController) processUpdateMessage(msg *pglogrepl.UpdateMessage, walData pglogrepl.XLogData) *Wal {
	relationColumns := wc.relationCache[msg.RelationID].Columns
	var tableAction = &TableAction{
		Values:    make(map[string]any),
		OldValues: make(map[string]any),
	}
	for i := range len(relationColumns) {
		oid := wc.relationCache[msg.RelationID].ColumnTypes[relationColumns[i]]
		processedValue, err := processOID(oid, msg.NewTuple.Columns[i].Data)
		if err != nil {
			fmt.Println(err)
		}
		tableAction.Values[relationColumns[i]] = processedValue
	}
	if msg.OldTuple != nil {
		for i := range len(relationColumns) {
			oid := wc.relationCache[msg.RelationID].ColumnTypes[relationColumns[i]]
			processedValue, err := processOID(oid, msg.OldTuple.Columns[i].Data)
			if err != nil {
				fmt.Println(err)
			}
			tableAction.OldValues[relationColumns[i]] = processedValue
		}
	}
	tableAction.Operation = Update
	tableAction.TableName = Table(wc.relationCache[msg.RelationID].Relation)
	return &Wal{
		TableAction: tableAction,
		LSN:         walData.ServerWALEnd,
	}
}

func (wc *WALController) processDeleteMessage(msg *pglogrepl.DeleteMessage, walData pglogrepl.XLogData) *Wal {
	relationColumns := wc.relationCache[msg.RelationID].Columns
	var tableAction = &TableAction{
		Values:    make(map[string]any),
		OldValues: make(map[string]any),
	}
	for i := range len(relationColumns) {
		oid := wc.relationCache[msg.RelationID].ColumnTypes[relationColumns[i]]
		processedValue, err := processOID(oid, msg.OldTuple.Columns[i].Data)
		if err != nil {
			fmt.Println(err)
		}
		tableAction.Values[relationColumns[i]] = processedValue
	}
	for i := range len(relationColumns) {
		oid := wc.relationCache[msg.RelationID].ColumnTypes[relationColumns[i]]
		processedValue, err := processOID(oid, msg.OldTuple.Columns[i].Data)
		if err != nil {
			fmt.Println(err)
		}
		tableAction.OldValues[relationColumns[i]] = processedValue
	}
	tableAction.Operation = Delete
	tableAction.TableName = Table(wc.relationCache[msg.RelationID].Relation)
	return &Wal{
		TableAction: tableAction,
		LSN:         walData.ServerWALEnd,
	}
}

func (wc *WALController) processLogicalMessage(msg *pglogrepl.LogicalDecodingMessage, walData pglogrepl.XLogData) *Wal {
	var logicalMessage = &LogicalMessage{
		Prefix:  msg.Prefix,
		Content: msg.Content,
	}
	return &Wal{
		LogicalMessage: logicalMessage,
		LSN:            walData.ServerWALEnd,
	}
}
