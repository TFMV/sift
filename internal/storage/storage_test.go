package storage

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/TFMV/sift/pkg/config"
	"github.com/TFMV/sift/pkg/schema/generated/sift/schema"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

func TestNewStorage(t *testing.T) {
	// Skip if DuckDB is not available
	if os.Getenv("SKIP_DUCKDB_TESTS") == "1" {
		t.Skip("Skipping test that requires DuckDB")
	}

	// Create a temporary directory for the test
	tempDir, err := os.MkdirTemp("", "sift-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create test config
	cfg := &config.Config{
		Storage: struct {
			DuckDB struct {
				Path     string `mapstructure:"path"`
				MemoryDB bool   `mapstructure:"memory_db"`
			} `mapstructure:"duckdb"`
			Parquet struct {
				S3Bucket           string `mapstructure:"s3_bucket"`
				S3Region           string `mapstructure:"s3_region"`
				S3Endpoint         string `mapstructure:"s3_endpoint"`
				RotationIntervalHr int    `mapstructure:"rotation_interval_hr"`
				RetentionDays      int    `mapstructure:"retention_days"`
				UseLocalFS         bool   `mapstructure:"use_local_fs"`
				LocalPath          string `mapstructure:"local_path"`
			} `mapstructure:"parquet"`
		}{
			DuckDB: struct {
				Path     string `mapstructure:"path"`
				MemoryDB bool   `mapstructure:"memory_db"`
			}{
				Path:     filepath.Join(tempDir, "test.db"),
				MemoryDB: true, // Use in-memory DB for faster tests
			},
			Parquet: struct {
				S3Bucket           string `mapstructure:"s3_bucket"`
				S3Region           string `mapstructure:"s3_region"`
				S3Endpoint         string `mapstructure:"s3_endpoint"`
				RotationIntervalHr int    `mapstructure:"rotation_interval_hr"`
				RetentionDays      int    `mapstructure:"retention_days"`
				UseLocalFS         bool   `mapstructure:"use_local_fs"`
				LocalPath          string `mapstructure:"local_path"`
			}{
				RotationIntervalHr: 1,
				RetentionDays:      1,
				UseLocalFS:         true,
				LocalPath:          filepath.Join(tempDir, "parquet"),
			},
		},
	}

	// Create logger
	logger := zaptest.NewLogger(t)

	// Create storage
	storage, err := NewStorage(cfg, logger)
	if err != nil {
		t.Skipf("Skipping test due to DuckDB error: %v", err)
		return
	}
	require.NotNil(t, storage)

	// Close storage
	err = storage.Close()
	require.NoError(t, err)
}

func TestStoreLogEvent(t *testing.T) {
	// Skip if DuckDB is not available
	if os.Getenv("SKIP_DUCKDB_TESTS") == "1" {
		t.Skip("Skipping test that requires DuckDB")
	}

	// Create a temporary directory for the test
	tempDir, err := os.MkdirTemp("", "sift-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create test config with in-memory DB for faster tests
	cfg := &config.Config{
		Storage: struct {
			DuckDB struct {
				Path     string `mapstructure:"path"`
				MemoryDB bool   `mapstructure:"memory_db"`
			} `mapstructure:"duckdb"`
			Parquet struct {
				S3Bucket           string `mapstructure:"s3_bucket"`
				S3Region           string `mapstructure:"s3_region"`
				S3Endpoint         string `mapstructure:"s3_endpoint"`
				RotationIntervalHr int    `mapstructure:"rotation_interval_hr"`
				RetentionDays      int    `mapstructure:"retention_days"`
				UseLocalFS         bool   `mapstructure:"use_local_fs"`
				LocalPath          string `mapstructure:"local_path"`
			} `mapstructure:"parquet"`
		}{
			DuckDB: struct {
				Path     string `mapstructure:"path"`
				MemoryDB bool   `mapstructure:"memory_db"`
			}{
				Path:     filepath.Join(tempDir, "test.db"),
				MemoryDB: true,
			},
			Parquet: struct {
				S3Bucket           string `mapstructure:"s3_bucket"`
				S3Region           string `mapstructure:"s3_region"`
				S3Endpoint         string `mapstructure:"s3_endpoint"`
				RotationIntervalHr int    `mapstructure:"rotation_interval_hr"`
				RetentionDays      int    `mapstructure:"retention_days"`
				UseLocalFS         bool   `mapstructure:"use_local_fs"`
				LocalPath          string `mapstructure:"local_path"`
			}{
				RotationIntervalHr: 1,
				RetentionDays:      1,
				UseLocalFS:         true,
				LocalPath:          filepath.Join(tempDir, "parquet"),
			},
		},
	}

	// Create logger
	logger := zaptest.NewLogger(t)

	// Create storage
	storage, err := NewStorage(cfg, logger)
	if err != nil {
		t.Skipf("Skipping test due to DuckDB error: %v", err)
		return
	}
	defer storage.Close()

	// Create a test log event using Flatbuffers
	builder := flatbuffers.NewBuilder(1024)

	// Create message
	message := builder.CreateString("Test log message")

	// Create service
	service := builder.CreateString("test-service")

	// Create instance ID
	instanceID := builder.CreateString("test-instance-1")

	// Create trace ID
	traceID := builder.CreateString("abcdef1234567890")

	// Create span ID
	spanID := builder.CreateString("span123456")

	// Create file
	file := builder.CreateString("main.go")

	// Create function
	function := builder.CreateString("main")

	// Create error type
	errorType := builder.CreateString("")

	// Create error stack
	errorStack := builder.CreateString("")

	// Start building LogEvent
	schema.LogEventStart(builder)
	schema.LogEventAddTimestamp(builder, time.Now().UnixNano())
	schema.LogEventAddService(builder, service)
	schema.LogEventAddInstanceId(builder, instanceID)
	schema.LogEventAddTraceId(builder, traceID)
	schema.LogEventAddSpanId(builder, spanID)
	schema.LogEventAddLevel(builder, schema.LogLevelINFO)
	schema.LogEventAddMessage(builder, message)
	schema.LogEventAddFile(builder, file)
	schema.LogEventAddLine(builder, 42)
	schema.LogEventAddFunction(builder, function)
	schema.LogEventAddErrorType(builder, errorType)
	schema.LogEventAddErrorStack(builder, errorStack)

	logEvent := schema.LogEventEnd(builder)
	builder.Finish(logEvent)

	// Get the bytes
	data := builder.FinishedBytes()

	// Store the log event
	err = storage.StoreLogEvent(data)
	require.NoError(t, err)

	// Query the log event
	logs, err := storage.QueryLogs(map[string]interface{}{
		"service": "test-service",
	}, 10, 0)
	require.NoError(t, err)
	require.Len(t, logs, 1)

	// Verify the log event
	assert.Equal(t, "test-service", logs[0]["service"])
	assert.Equal(t, "Test log message", logs[0]["message"])
	assert.Equal(t, "test-instance-1", logs[0]["instance_id"])
	assert.Equal(t, "abcdef1234567890", logs[0]["trace_id"])
	assert.Equal(t, "span123456", logs[0]["span_id"])
	assert.Equal(t, int32(2), logs[0]["level"]) // INFO level is 2
	assert.Equal(t, "main.go", logs[0]["file"])
	assert.Equal(t, int32(42), logs[0]["line"])
	assert.Equal(t, "main", logs[0]["function"])
}

func TestQueryLogs(t *testing.T) {
	// Skip if DuckDB is not available
	if os.Getenv("SKIP_DUCKDB_TESTS") == "1" {
		t.Skip("Skipping test that requires DuckDB")
	}

	// Create a temporary directory for the test
	tempDir, err := os.MkdirTemp("", "sift-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create test config with in-memory DB for faster tests
	cfg := &config.Config{
		Storage: struct {
			DuckDB struct {
				Path     string `mapstructure:"path"`
				MemoryDB bool   `mapstructure:"memory_db"`
			} `mapstructure:"duckdb"`
			Parquet struct {
				S3Bucket           string `mapstructure:"s3_bucket"`
				S3Region           string `mapstructure:"s3_region"`
				S3Endpoint         string `mapstructure:"s3_endpoint"`
				RotationIntervalHr int    `mapstructure:"rotation_interval_hr"`
				RetentionDays      int    `mapstructure:"retention_days"`
				UseLocalFS         bool   `mapstructure:"use_local_fs"`
				LocalPath          string `mapstructure:"local_path"`
			} `mapstructure:"parquet"`
		}{
			DuckDB: struct {
				Path     string `mapstructure:"path"`
				MemoryDB bool   `mapstructure:"memory_db"`
			}{
				Path:     filepath.Join(tempDir, "test.db"),
				MemoryDB: true,
			},
			Parquet: struct {
				S3Bucket           string `mapstructure:"s3_bucket"`
				S3Region           string `mapstructure:"s3_region"`
				S3Endpoint         string `mapstructure:"s3_endpoint"`
				RotationIntervalHr int    `mapstructure:"rotation_interval_hr"`
				RetentionDays      int    `mapstructure:"retention_days"`
				UseLocalFS         bool   `mapstructure:"use_local_fs"`
				LocalPath          string `mapstructure:"local_path"`
			}{
				RotationIntervalHr: 1,
				RetentionDays:      1,
				UseLocalFS:         true,
				LocalPath:          filepath.Join(tempDir, "parquet"),
			},
		},
	}

	// Create logger
	logger := zaptest.NewLogger(t)

	// Create storage
	storage, err := NewStorage(cfg, logger)
	if err != nil {
		t.Skipf("Skipping test due to DuckDB error: %v", err)
		return
	}
	defer storage.Close()

	// Insert test data directly
	_, err = storage.db.Exec(`
		INSERT INTO log_events (
			timestamp, service, instance_id, level, message
		) VALUES 
		(?, 'service1', 'instance1', 2, 'Message 1'),
		(?, 'service1', 'instance2', 3, 'Message 2'),
		(?, 'service2', 'instance3', 4, 'Message 3')
	`, time.Now(), time.Now(), time.Now())
	require.NoError(t, err)

	// Test query with no filter
	logs, err := storage.QueryLogs(map[string]interface{}{}, 10, 0)
	require.NoError(t, err)
	require.Len(t, logs, 3)

	// Test query with service filter
	logs, err = storage.QueryLogs(map[string]interface{}{
		"service": "service1",
	}, 10, 0)
	require.NoError(t, err)
	require.Len(t, logs, 2)
	for _, log := range logs {
		assert.Equal(t, "service1", log["service"])
	}

	// Test query with instance filter
	logs, err = storage.QueryLogs(map[string]interface{}{
		"instance_id": "instance2",
	}, 10, 0)
	require.NoError(t, err)
	require.Len(t, logs, 1)
	assert.Equal(t, "instance2", logs[0]["instance_id"])

	// Test query with level filter
	logs, err = storage.QueryLogs(map[string]interface{}{
		"level": int32(4),
	}, 10, 0)
	require.NoError(t, err)
	require.Len(t, logs, 1)
	assert.Equal(t, int32(4), logs[0]["level"])

	// Test limit and offset
	logs, err = storage.QueryLogs(map[string]interface{}{}, 1, 1)
	require.NoError(t, err)
	require.Len(t, logs, 1)
}

func TestFlushToParquet(t *testing.T) {
	// Skip if DuckDB is not available
	if os.Getenv("SKIP_DUCKDB_TESTS") == "1" {
		t.Skip("Skipping test that requires DuckDB")
	}

	// Create a temporary directory for the test
	tempDir, err := os.MkdirTemp("", "sift-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	parquetDir := filepath.Join(tempDir, "parquet")
	err = os.MkdirAll(parquetDir, 0755)
	require.NoError(t, err)

	// Create test config with in-memory DB for faster tests
	cfg := &config.Config{
		Storage: struct {
			DuckDB struct {
				Path     string `mapstructure:"path"`
				MemoryDB bool   `mapstructure:"memory_db"`
			} `mapstructure:"duckdb"`
			Parquet struct {
				S3Bucket           string `mapstructure:"s3_bucket"`
				S3Region           string `mapstructure:"s3_region"`
				S3Endpoint         string `mapstructure:"s3_endpoint"`
				RotationIntervalHr int    `mapstructure:"rotation_interval_hr"`
				RetentionDays      int    `mapstructure:"retention_days"`
				UseLocalFS         bool   `mapstructure:"use_local_fs"`
				LocalPath          string `mapstructure:"local_path"`
			} `mapstructure:"parquet"`
		}{
			DuckDB: struct {
				Path     string `mapstructure:"path"`
				MemoryDB bool   `mapstructure:"memory_db"`
			}{
				Path:     filepath.Join(tempDir, "test.db"),
				MemoryDB: true,
			},
			Parquet: struct {
				S3Bucket           string `mapstructure:"s3_bucket"`
				S3Region           string `mapstructure:"s3_region"`
				S3Endpoint         string `mapstructure:"s3_endpoint"`
				RotationIntervalHr int    `mapstructure:"rotation_interval_hr"`
				RetentionDays      int    `mapstructure:"retention_days"`
				UseLocalFS         bool   `mapstructure:"use_local_fs"`
				LocalPath          string `mapstructure:"local_path"`
			}{
				RotationIntervalHr: 1,
				RetentionDays:      1,
				UseLocalFS:         true,
				LocalPath:          parquetDir,
			},
		},
	}

	// Create logger
	logger := zaptest.NewLogger(t)

	// Create storage
	storage, err := NewStorage(cfg, logger)
	if err != nil {
		t.Skipf("Skipping test due to DuckDB error: %v", err)
		return
	}
	defer storage.Close()

	// Insert test data directly with a timestamp in the past
	pastTime := time.Now().Add(-2 * time.Hour)
	_, err = storage.db.Exec(`
		INSERT INTO log_events (
			timestamp, service, instance_id, level, message, attributes, file, line, function, error_type, error_stack
		) VALUES 
		(?, 'service1', 'instance1', 2, 'Message 1', '[]', 'file1.go', 10, 'func1', '', ''),
		(?, 'service1', 'instance2', 3, 'Message 2', '[]', 'file2.go', 20, 'func2', '', ''),
		(?, 'service2', 'instance3', 4, 'Message 3', '[]', 'file3.go', 30, 'func3', 'error1', 'stack1')
	`, pastTime, pastTime, pastTime)
	require.NoError(t, err)

	// Insert test data with current timestamp (should not be flushed)
	_, err = storage.db.Exec(`
		INSERT INTO log_events (
			timestamp, service, instance_id, level, message
		) VALUES 
		(?, 'service3', 'instance4', 2, 'Message 4')
	`, time.Now())
	require.NoError(t, err)

	// Verify we have 4 records
	var count int
	err = storage.db.QueryRow("SELECT COUNT(*) FROM log_events").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 4, count)

	// Trigger flush
	err = storage.flushToParquet()
	require.NoError(t, err)

	// Verify only 1 record remains (the current one)
	err = storage.db.QueryRow("SELECT COUNT(*) FROM log_events").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 1, count)

	// Find the Parquet file
	files, err := os.ReadDir(parquetDir)
	require.NoError(t, err)
	require.Len(t, files, 1)

	parquetFile := filepath.Join(parquetDir, files[0].Name())

	// Read the Parquet file to verify its contents
	fr, err := local.NewLocalFileReader(parquetFile)
	require.NoError(t, err)
	defer fr.Close()

	pr, err := reader.NewParquetReader(fr, new(LogEventParquet), 4)
	require.NoError(t, err)
	defer pr.ReadStop()

	// Verify the number of rows
	require.Equal(t, int64(3), pr.GetNumRows())

	// Read the records
	records := make([]LogEventParquet, 3)
	err = pr.Read(&records)
	require.NoError(t, err)

	// Verify the records
	serviceMap := make(map[string]bool)
	for _, record := range records {
		serviceMap[record.Service] = true

		// Verify common fields
		assert.NotZero(t, record.ID)
		assert.NotZero(t, record.Timestamp)

		// Verify service-specific fields
		switch record.Service {
		case "service1":
			assert.Contains(t, []string{"instance1", "instance2"}, record.InstanceID)
			if record.InstanceID == "instance1" {
				assert.Equal(t, int32(2), record.Level)
				assert.Equal(t, "Message 1", record.Message)
				assert.Equal(t, "file1.go", record.File)
				assert.Equal(t, int32(10), record.Line)
				assert.Equal(t, "func1", record.Function)
			} else {
				assert.Equal(t, int32(3), record.Level)
				assert.Equal(t, "Message 2", record.Message)
				assert.Equal(t, "file2.go", record.File)
				assert.Equal(t, int32(20), record.Line)
				assert.Equal(t, "func2", record.Function)
			}
		case "service2":
			assert.Equal(t, "instance3", record.InstanceID)
			assert.Equal(t, int32(4), record.Level)
			assert.Equal(t, "Message 3", record.Message)
			assert.Equal(t, "file3.go", record.File)
			assert.Equal(t, int32(30), record.Line)
			assert.Equal(t, "func3", record.Function)
			assert.Equal(t, "error1", record.ErrorType)
			assert.Equal(t, "stack1", record.ErrorStack)
		}
	}

	// Verify we have both services
	assert.Len(t, serviceMap, 2)
	assert.True(t, serviceMap["service1"])
	assert.True(t, serviceMap["service2"])
}

func createTestLogger() *zap.Logger {
	logger, _ := zap.NewDevelopment()
	return logger
}
