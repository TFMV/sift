package storage

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/TFMV/sift/pkg/config"
	"github.com/TFMV/sift/pkg/schema/generated/sift/schema"
	_ "github.com/marcboeker/go-duckdb"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
	"go.uber.org/zap"
)

// Storage handles storing and retrieving log events
type Storage struct {
	config    *config.Config
	logger    *zap.Logger
	db        *sql.DB
	mutex     sync.RWMutex
	flushChan chan struct{}
	stopChan  chan struct{}
	wg        sync.WaitGroup
}

// NewStorage creates a new Storage instance
func NewStorage(cfg *config.Config, logger *zap.Logger) (*Storage, error) {
	storage := &Storage{
		config:    cfg,
		logger:    logger,
		flushChan: make(chan struct{}),
		stopChan:  make(chan struct{}),
	}

	// Create database directory if it doesn't exist
	dbDir := filepath.Dir(cfg.Storage.DuckDB.Path)
	if err := os.MkdirAll(dbDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create database directory: %w", err)
	}

	// Open DuckDB connection
	var err error
	var connString string

	if cfg.Storage.DuckDB.MemoryDB {
		connString = "duckdb::memory:"
	} else {
		connString = cfg.Storage.DuckDB.Path
	}

	storage.db, err = sql.Open("duckdb", connString)
	if err != nil {
		return nil, fmt.Errorf("failed to open DuckDB: %w", err)
	}

	// Initialize tables
	if err := storage.initTables(); err != nil {
		storage.db.Close()
		return nil, fmt.Errorf("failed to initialize tables: %w", err)
	}

	// Start background tasks
	storage.startBackgroundTasks()

	return storage, nil
}

// Close closes the storage
func (s *Storage) Close() error {
	// Signal to stop background tasks
	close(s.stopChan)
	s.wg.Wait()

	// Close DuckDB connection
	if err := s.db.Close(); err != nil {
		s.logger.Error("Failed to close DuckDB connection", zap.Error(err))
	}

	return nil
}

// StoreLogEvent stores a log event from a Flatbuffer
func (s *Storage) StoreLogEvent(data []byte) error {
	// Deserialize Flatbuffer
	logEvent := schema.GetRootAsLogEvent(data, 0)

	// Extract fields from the Flatbuffer
	timestamp := time.Unix(0, logEvent.Timestamp())
	service := string(logEvent.Service())
	instanceID := string(logEvent.InstanceId())
	traceID := string(logEvent.TraceId())
	spanID := string(logEvent.SpanId())
	level := logEvent.Level()
	message := string(logEvent.Message())

	// Extract file, line, function
	file := string(logEvent.File())
	line := logEvent.Line()
	function := string(logEvent.Function())

	// Extract error details if present
	errorType := string(logEvent.ErrorType())
	errorStack := string(logEvent.ErrorStack())

	// Extract attributes as JSON
	attributes := "[]"
	if logEvent.AttributesLength() > 0 {
		// Simple JSON array construction for attributes
		attributesJSON := "["
		for i := 0; i < logEvent.AttributesLength(); i++ {
			attr := new(schema.Attribute)
			if logEvent.Attributes(attr, i) {
				if i > 0 {
					attributesJSON += ","
				}
				key := string(attr.Key())
				value := string(attr.Value())
				attributesJSON += fmt.Sprintf(`{"key":"%s","value":"%s"}`, key, value)
			}
		}
		attributesJSON += "]"
		attributes = attributesJSON
	}

	// Acquire write lock
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Insert into DuckDB with actual values
	_, err := s.db.Exec(`
		INSERT INTO log_events (
			timestamp, service, instance_id, trace_id, span_id, 
			level, message, attributes, file, line, function,
			error_type, error_stack
		) VALUES (
			?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
		)
	`,
		timestamp, service, instanceID, traceID, spanID,
		int(level), message, attributes, file, line, function,
		errorType, errorStack)

	if err != nil {
		return fmt.Errorf("failed to insert log event: %w", err)
	}

	return nil
}

// QueryLogs queries logs with the given filter
func (s *Storage) QueryLogs(filter map[string]interface{}, limit int, offset int) ([]map[string]interface{}, error) {
	// Acquire read lock
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	// Build query with filters
	query := "SELECT * FROM log_events"

	// Add filters (very basic implementation)
	if len(filter) > 0 {
		query += " WHERE "
		first := true

		for key, value := range filter {
			if !first {
				query += " AND "
			}
			query += fmt.Sprintf("%s = '%v'", key, value)
			first = false
		}
	}

	// Add limit and offset
	query += fmt.Sprintf(" LIMIT %d OFFSET %d", limit, offset)

	// Execute query
	rows, err := s.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	// Build results
	var results []map[string]interface{}

	for rows.Next() {
		// Create a slice of interface{} to hold the values
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))

		// Initialize pointers
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		// Scan the row into the values
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		// Create a map for the row
		row := make(map[string]interface{})
		for i, col := range columns {
			row[col] = values[i]
		}

		results = append(results, row)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

// TriggerFlush triggers a flush of data to long-term storage
func (s *Storage) TriggerFlush() {
	select {
	case s.flushChan <- struct{}{}:
		// Successfully triggered
	default:
		// Channel is full, ignore
	}
}

// initTables initializes the database tables
func (s *Storage) initTables() error {
	// Create sequence for log_events table
	_, err := s.db.Exec(`CREATE SEQUENCE IF NOT EXISTS log_events_id_seq START 1`)
	if err != nil {
		return fmt.Errorf("failed to create sequence: %w", err)
	}

	// Create log_events table with compression
	_, err = s.db.Exec(`
		CREATE TABLE IF NOT EXISTS log_events (
			id BIGINT PRIMARY KEY DEFAULT nextval('log_events_id_seq'),
			timestamp TIMESTAMP,
			service VARCHAR(255),
			instance_id VARCHAR(255),
			trace_id VARCHAR(32),
			span_id VARCHAR(16),
			level INTEGER,
			message TEXT,
			attributes JSON,
			file VARCHAR(255),
			line INTEGER,
			function VARCHAR(255),
			error_type VARCHAR(255),
			error_stack TEXT
		) WITH (compression = 'zstd')
	`)

	if err != nil {
		return err
	}

	// Create indexes
	_, err = s.db.Exec(`
		CREATE INDEX IF NOT EXISTS idx_log_events_timestamp ON log_events (timestamp);
		CREATE INDEX IF NOT EXISTS idx_log_events_service ON log_events (service);
		CREATE INDEX IF NOT EXISTS idx_log_events_level ON log_events (level);
		CREATE INDEX IF NOT EXISTS idx_log_events_trace_id ON log_events (trace_id);
	`)

	return err
}

// startBackgroundTasks starts background tasks like flushing data to Parquet
func (s *Storage) startBackgroundTasks() {
	// Start periodic flusher
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		ticker := time.NewTicker(time.Duration(s.config.Storage.Parquet.RotationIntervalHr) * time.Hour)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if err := s.flushToParquet(); err != nil {
					s.logger.Error("Failed to flush to Parquet", zap.Error(err))
				}
			case <-s.flushChan:
				if err := s.flushToParquet(); err != nil {
					s.logger.Error("Failed to flush to Parquet", zap.Error(err))
				}
			case <-s.stopChan:
				// Final flush before stopping
				if err := s.flushToParquet(); err != nil {
					s.logger.Error("Failed to flush to Parquet during shutdown", zap.Error(err))
				}
				return
			}
		}
	}()
}

// LogEventParquet represents a log event for Parquet serialization
type LogEventParquet struct {
	ID         int64  `parquet:"name=id, type=INT64"`
	Timestamp  int64  `parquet:"name=timestamp, type=INT64"`
	Service    string `parquet:"name=service, type=BYTE_ARRAY, convertedtype=UTF8"`
	InstanceID string `parquet:"name=instance_id, type=BYTE_ARRAY, convertedtype=UTF8"`
	TraceID    string `parquet:"name=trace_id, type=BYTE_ARRAY, convertedtype=UTF8"`
	SpanID     string `parquet:"name=span_id, type=BYTE_ARRAY, convertedtype=UTF8"`
	Level      int32  `parquet:"name=level, type=INT32"`
	Message    string `parquet:"name=message, type=BYTE_ARRAY, convertedtype=UTF8"`
	Attributes string `parquet:"name=attributes, type=BYTE_ARRAY, convertedtype=UTF8"`
	File       string `parquet:"name=file, type=BYTE_ARRAY, convertedtype=UTF8"`
	Line       int32  `parquet:"name=line, type=INT32"`
	Function   string `parquet:"name=function, type=BYTE_ARRAY, convertedtype=UTF8"`
	ErrorType  string `parquet:"name=error_type, type=BYTE_ARRAY, convertedtype=UTF8"`
	ErrorStack string `parquet:"name=error_stack, type=BYTE_ARRAY, convertedtype=UTF8"`
}

// flushToParquet flushes data from DuckDB to Parquet
func (s *Storage) flushToParquet() error {
	s.logger.Info("Flushing data to Parquet")

	// Acquire write lock to prevent concurrent reads/writes during flush
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Get timestamp of oldest records to flush
	cutoffTime := time.Now().Add(-time.Duration(s.config.Storage.Parquet.RotationIntervalHr) * time.Hour)

	// Query records to flush
	rows, err := s.db.Query(`
		SELECT 
			id, timestamp, service, instance_id, trace_id, span_id, 
			level, message, attributes, file, line, function,
			error_type, error_stack
		FROM log_events 
		WHERE timestamp < ?
		ORDER BY timestamp
	`, cutoffTime)

	if err != nil {
		return fmt.Errorf("failed to query logs for flushing: %w", err)
	}
	defer rows.Close()

	// Generate Parquet file path
	parquetDir := s.config.Storage.Parquet.LocalPath
	if err := os.MkdirAll(parquetDir, 0755); err != nil {
		return fmt.Errorf("failed to create Parquet directory: %w", err)
	}

	parquetFile := filepath.Join(
		parquetDir,
		fmt.Sprintf("logs_%s.parquet", time.Now().Format("20060102_150405")),
	)

	// Create Parquet file using the local file source
	fw, err := local.NewLocalFileWriter(parquetFile)
	if err != nil {
		return fmt.Errorf("failed to create Parquet file writer: %w", err)
	}
	defer fw.Close()

	// Create Parquet writer
	pw, err := writer.NewParquetWriter(fw, new(LogEventParquet), 4)
	if err != nil {
		return fmt.Errorf("failed to create Parquet writer: %w", err)
	}

	// Set compression
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	// Write records to Parquet file
	rowCount := 0
	for rows.Next() {
		var (
			id         int64
			timestamp  time.Time
			service    string
			instanceID string
			traceID    string
			spanID     string
			level      int32
			message    string
			attributes string
			file       string
			line       int32
			function   string
			errorType  string
			errorStack string
		)

		if err := rows.Scan(
			&id, &timestamp, &service, &instanceID, &traceID, &spanID,
			&level, &message, &attributes, &file, &line, &function,
			&errorType, &errorStack,
		); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		// Create Parquet record
		record := LogEventParquet{
			ID:         id,
			Timestamp:  timestamp.UnixNano(),
			Service:    service,
			InstanceID: instanceID,
			TraceID:    traceID,
			SpanID:     spanID,
			Level:      level,
			Message:    message,
			Attributes: attributes,
			File:       file,
			Line:       line,
			Function:   function,
			ErrorType:  errorType,
			ErrorStack: errorStack,
		}

		// Write record to Parquet file
		if err := pw.Write(record); err != nil {
			return fmt.Errorf("failed to write record to Parquet: %w", err)
		}

		rowCount++
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating rows: %w", err)
	}

	// Close Parquet writer
	if err := pw.WriteStop(); err != nil {
		return fmt.Errorf("failed to close Parquet writer: %w", err)
	}

	s.logger.Info("Flushed data to Parquet",
		zap.String("file", parquetFile),
		zap.Int("rows", rowCount))

	// Delete flushed records from DuckDB if we successfully wrote them to Parquet
	if rowCount > 0 {
		result, err := s.db.Exec(`
			DELETE FROM log_events 
			WHERE timestamp < ?
		`, cutoffTime)

		if err != nil {
			return fmt.Errorf("failed to delete flushed logs: %w", err)
		}

		deletedRows, _ := result.RowsAffected()
		s.logger.Info("Deleted flushed logs from DuckDB", zap.Int64("rows", deletedRows))
	} else {
		s.logger.Info("No logs to flush")
	}

	return nil
}
