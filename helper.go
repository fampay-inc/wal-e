package wal_e

import (
	"encoding/json"
	"fmt"
	"strconv"
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

func processOID(oid uint32, data []byte) (any, error) {
	if len(data) == 0 {
		return nil, nil
	}
	strValue := string(data)
	switch oid {
	case 16:
		return strValue == "t" || strValue == "true" || strValue == "True", nil
	case 17:
		return data, nil
	case 20: // BIGINT
		return strconv.ParseInt(strValue, 10, 64)

	case 21, 23: // SMALLINT, INTEGER
		return strconv.Atoi(strValue)

	case 25, 1043: // TEXT, VARCHAR
		return strValue, nil

	case 700: // FLOAT4 (float32)
		f, err := strconv.ParseFloat(strValue, 32)
		return float32(f), err

	case 701: // FLOAT8 (float64)
		return strconv.ParseFloat(strValue, 64)

	case 1082: // DATE
		return time.Parse("2006-01-02", strValue)

	case 1083, 1114: // TIME, TIMESTAMP (without timezone)
		return time.Parse("2006-01-02 15:04:05", strValue)

	case 1184: // TIMESTAMPTZ
		return time.Parse("2006-01-02 15:04:05.999999-07", strValue)

	case 1700: // NUMERIC
		return strconv.ParseFloat(strValue, 64)

	case 3802: // JSONB
		var jsonData any
		if err := json.Unmarshal(data, &jsonData); err != nil {
			return nil, err
		}
		return jsonData, nil
	default: // Unknown type
		return strValue, nil
	}
}

func (wc *WALController) processRelationMessage(msg *pglogrepl.RelationMessage) {
	columns := make([]Column, len(msg.Columns))
	columnType := make(map[Column]uint32)
	for i, col := range msg.Columns {
		columns[i] = Column(col.Name)
		columnType[Column(col.Name)] = col.DataType
	}
	wc.relationCache[msg.RelationID] = RelationData{
		Columns:     columns,
		Relation:    msg.RelationName,
		ColumnTypes: columnType,
	}
}

func (wc *WALController) processInsertMessage(msg *pglogrepl.InsertMessage) *Wal {
	relationColumns := wc.relationCache[msg.RelationID].Columns
	var walLog = Wal{
		Values: make(map[Column]any),
	}
	for i := range len(relationColumns) {
		oid := wc.relationCache[msg.RelationID].ColumnTypes[relationColumns[i]]
		processedValue, err := processOID(oid, msg.Tuple.Columns[i].Data)
		if err != nil {
			fmt.Println(err)
		}
		walLog.Values[relationColumns[i]] = processedValue
	}
	walLog.Operation = Insert
	walLog.TableName = Table(wc.relationCache[msg.RelationID].Relation)
	return &walLog
}

func (wc *WALController) processUpdateMessage(msg *pglogrepl.UpdateMessage) *Wal {
	relationColumns := wc.relationCache[msg.RelationID].Columns
	var walLog = Wal{
		Values: make(map[Column]any),
	}
	for i := range len(relationColumns) {
		oid := wc.relationCache[msg.RelationID].ColumnTypes[relationColumns[i]]
		processedValue, err := processOID(oid, msg.NewTuple.Columns[i].Data)
		if err != nil {
			fmt.Println(err)
		}
		walLog.Values[relationColumns[i]] = processedValue
	}
	walLog.Operation = Update
	walLog.TableName = Table(wc.relationCache[msg.RelationID].Relation)
	return &walLog
}

func (wc *WALController) processDeleteMessage(msg *pglogrepl.DeleteMessage) *Wal {
	relationColumns := wc.relationCache[msg.RelationID].Columns
	var walLog = Wal{
		Values: make(map[Column]any),
	}
	for i := range len(relationColumns) {
		oid := wc.relationCache[msg.RelationID].ColumnTypes[relationColumns[i]]
		processedValue, err := processOID(oid, msg.OldTuple.Columns[i].Data)
		if err != nil {
			fmt.Println(err)
		}
		walLog.Values[relationColumns[i]] = processedValue
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
