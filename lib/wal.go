package main

import (
	"fmt"
	"os"
	"sync"
)

type WriteAheadLog struct {
	logFilePath string
	file        *os.File
	byteOffset  int64
	mu          sync.Mutex // To ensure thread-safe writes
}

func NewWriteAheadLog(logFileName string) (*WriteAheadLog, error) {
	file, err := os.OpenFile(logFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY|os.O_SYNC, 0644)
	if err != nil {
		return nil, fmt.Errorf("error opening log file: %w", err)
	}

	return &WriteAheadLog{
		logFilePath: logFileName,
		file:        file,
		byteOffset:  0,
	}, nil
}

func (wal *WriteAheadLog) WriteInsertLog(key, value string) {
	wal.writeLog("INSERT", key, value)
}

func (wal *WriteAheadLog) WriteUpdateLog(key, value string) {
	wal.writeLog("UPDATE", key, value)
}

func (wal *WriteAheadLog) WriteDeleteLog(key string) {
	wal.writeLog("DELETE", key, "TOMBSTONE")
}

func (wal *WriteAheadLog) ClearLog() {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	// Truncate the file to clear its contents
	if err := wal.file.Truncate(0); err != nil {
		fmt.Println("Error clearing log:", err)
		return
	}

	// Reset the file offset
	if _, err := wal.file.Seek(0, 0); err != nil {
		fmt.Println("Error seeking to beginning of log file:", err)
		return
	}

	wal.byteOffset = 0
}

func (wal *WriteAheadLog) Close() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	if err := wal.file.Close(); err != nil {
		return fmt.Errorf("error closing log file: %w", err)
	}
	return nil
}

func (wal *WriteAheadLog) writeLog(operation, key, value string) {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	logEntry := fmt.Sprintf("%d, %s: %s: %s\n", wal.byteOffset, operation, key, value)
	bytesWritten, err := wal.file.WriteString(logEntry)
	if err != nil {
		fmt.Printf("Error writing %s log: %v\n", operation, err)
		return
	}

	wal.byteOffset += int64(bytesWritten)

	// Ensure data is flushed to disk
	if err := wal.fsync(); err != nil {
		fmt.Printf("Error syncing log file: %v\n", err)
	}
}

func (wal *WriteAheadLog) fsync() error {
	return wal.file.Sync()
}
