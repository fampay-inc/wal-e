package wal_e

import (
	"encoding/json"
	"strconv"
	"time"
)

var OidMap = map[uint32]func([]byte) (any, error){
	// bool
	16: convertToBool,

	// bytes
	17: convertToBytes,

	// int
	20: convertToInt64,
	21: convertToInt64,
	23: convertToInt64,

	// string
	25:   convertToString,
	1042: convertToString,
	1043: convertToString,
	790:  convertToString,

	// float
	700: convertToFloat32,
	701: convertToFloat64,

	// Date and time
	1082: convertToDate,
	1083: convertToTime,
	1114: convertToTimestampWOTZ,
	1184: convertToTimestampWTZ,
	1266: convertToTimeWTZ,

	// NUMERIC
	1700: convertToFloat64,

	// JSON, JSONB
	3802: convertToJSON,
	114:  convertToJSON,

	// UUID
	2950: convertToString,
}

func convertToBool(data []byte) (any, error) {
	strValue := string(data)
	return strValue == "t" || strValue == "true" || strValue == "True", nil
}

func convertToString(data []byte) (any, error) {
	return string(data), nil
}

func convertToBytes(data []byte) (any, error) {
	return data, nil
}

func convertToInt64(data []byte) (any, error) {
	return strconv.ParseInt(string(data), 10, 64)
}

func convertToFloat32(data []byte) (any, error) {
	return strconv.ParseFloat(string(data), 32)
}

func convertToFloat64(data []byte) (any, error) {
	return strconv.ParseFloat(string(data), 64)
}

func convertToDate(data []byte) (any, error) {
	return time.Parse("2006-01-02", string(data))
}

func convertToTime(data []byte) (any, error) {
	return time.Parse("15:04:05", string(data))
}

func convertToTimestampWOTZ(data []byte) (any, error) {
	return time.Parse("2006-01-02 15:04:05", string(data))
}

func convertToTimestampWTZ(data []byte) (any, error) {
	return time.Parse("2006-01-02 15:04:05.000000 -0700 MST", string(data))
}

func convertToTimeWTZ(data []byte) (any, error) {
	return time.Parse("15:04:05.999999Z07:00", string(data))
}

func convertToJSON(data []byte) (any, error) {
	var jsonData any
	if err := json.Unmarshal(data, &jsonData); err != nil {
		return nil, err
	}
	return jsonData, nil
}
