package storage

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/TFMV/sift/pkg/config"
	_ "github.com/marcboeker/go-duckdb"
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
	// Deserialize Flatbuffer and insert into database
	// This is just a placeholder implementation
	// In a real implementation, we would use the generated Flatbuffers code

	// Acquire write lock
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Insert into DuckDB
	// In a real implementation, we would extract fields from the Flatbuffer
	_, err := s.db.Exec(`
		INSERT INTO log_events (
			timestamp, service, level, message
		) VALUES (
			current_timestamp, 'placeholder', 'INFO', 'placeholder'
		)
	`)

	return err
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
	// Create log_events table
	_, err := s.db.Exec(`
		CREATE TABLE IF NOT EXISTS log_events (
			id BIGINT PRIMARY KEY AUTO_INCREMENT,
			timestamp TIMESTAMP,
			service VARCHAR(255),
			instance_id VARCHAR(255),
			trace_id VARCHAR(32),
			span_id VARCHAR(16),
			level VARCHAR(10),
			message TEXT,
			attributes JSON,
			file VARCHAR(255),
			line INTEGER,
			function VARCHAR(255),
			error_type VARCHAR(255),
			error_stack TEXT
		)
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
				// Final flush on shutdown
				if err := s.flushToParquet(); err != nil {
					s.logger.Error("Failed to flush to Parquet on shutdown", zap.Error(err))
				}
				return
			}
		}
	}()
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
		SELECT * FROM log_events 
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

	// Create Parquet file
	file, err := os.Create(parquetFile)
	if err != nil {
		return fmt.Errorf("failed to create Parquet file: %w", err)
	}
	defer file.Close()

	// In a real implementation, we would use the Parquet library to write the file
	// This is just a placeholder
	s.logger.Info("Flushed data to Parquet", zap.String("file", parquetFile))

	// Delete flushed records from DuckDB
	_, err = s.db.Exec(`
		DELETE FROM log_events 
		WHERE timestamp < ?
	`, cutoffTime)

	if err != nil {
		return fmt.Errorf("failed to delete flushed logs: %w", err)
	}

	return nil
}
