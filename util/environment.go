package util

import (
	"os"
	"strconv"
)

func GetEnvStr(key string, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

func GetEnvInt(key string, defaultValue int) int {
	value := os.Getenv(key)
	ret, err := strconv.Atoi(value)
	if err != nil {
		return defaultValue
	}
	return ret
}
