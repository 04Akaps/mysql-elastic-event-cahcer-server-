package repository

import (
	"errors"
	"strconv"
	"time"
)

func convertToInt64(value interface{}) (int64, error) {
	if idInt64, ok := value.(int64); ok {
		return idInt64, nil
	} else if idStr, ok := value.(string); ok {
		if idParsed, err := strconv.ParseInt(idStr, 10, 64); err != nil {
			return 0, err
		} else {
			return idParsed, nil
		}
	} else {
		return 0, errors.New("UnExpected Id Format")
	}
}

func convertToInt32(value interface{}) (int32, error) {
	if age, ok := value.(int32); ok {
		return age, nil
	} else {
		return 0, errors.New("convertToInt32 type Format")
	}
}

func convertToString(value interface{}) (string, error) {
	if name, ok := value.(string); ok {
		return name, nil
	} else {
		return "", errors.New("convertToString type Format")
	}
}

func convertTimeToUnix(value interface{}) (int64, error) {
	switch v := value.(type) {
	case time.Time:
		return v.Unix(), nil
	default:
		return 0, errors.New("convertTimeToUnix type Format") // Or handle error
	}
}
