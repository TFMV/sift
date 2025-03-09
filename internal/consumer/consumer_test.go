package consumer

import (
	"errors"
	"sync"
	"testing"

	"github.com/TFMV/sift/pkg/config"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
	"go.uber.org/zap/zaptest/observer"
)

// fakeStorage is a mock implementation of the ConsumerStorageInterface
type fakeStorage struct {
	mu     sync.Mutex
	events [][]byte
	err    error // if set, StoreLogEvent returns this error.
}

func (fs *fakeStorage) StoreLogEvent(data []byte) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.events = append(fs.events, data)
	return fs.err
}

func (fs *fakeStorage) TriggerFlush() {
	// no-op for testing
}

// TestProcessRecordSuccess verifies that processRecord stores the log event correctly.
func TestProcessRecordSuccess(t *testing.T) {
	fs := &fakeStorage{}
	logger := zap.NewNop()
	// Create a minimal dummy config.
	dummyCfg := &config.Config{
		Consumer: struct {
			Brokers          []string `mapstructure:"brokers"`
			Topic            string   `mapstructure:"topic"`
			GroupID          string   `mapstructure:"group_id"`
			MaxWorkers       int      `mapstructure:"max_workers"`
			CommitInterval   int      `mapstructure:"commit_interval_ms"`
			SessionTimeout   int      `mapstructure:"session_timeout_ms"`
			RebalanceTimeout int      `mapstructure:"rebalance_timeout_ms"`
		}{
			MaxWorkers: 1,
		},
	}

	// Create a consumer without needing a real Kafka client (client is not used in processRecord).
	cons := &Consumer{
		// client is not used in processRecord so we leave it nil.
		storage:    fs,
		logger:     logger,
		config:     dummyCfg,
		workerPool: make(chan struct{}, dummyCfg.Consumer.MaxWorkers),
		// ctx and cancel are not used in processRecord.
	}

	// Create a fake record.
	record := &kgo.Record{
		Value:     []byte("test event"),
		Offset:    100,
		Topic:     "test-topic",
		Partition: 0,
	}

	// Process the record.
	cons.processRecord(record)

	// Wait for the goroutine to complete.
	for len(cons.workerPool) > 0 {
		// Wait for the worker to finish.
	}

	// Verify the record was stored.
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if len(fs.events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(fs.events))
	}
	if string(fs.events[0]) != "test event" {
		t.Errorf("expected 'test event', got %q", string(fs.events[0]))
	}
}

// TestProcessRecordFailure verifies that processRecord handles storage errors gracefully.
func TestProcessRecordFailure(t *testing.T) {
	// Create a storage that will return an error.
	expectedErr := &fakeStorage{err: errors.New("storage error")}

	// Create a logger that will capture error logs.
	core, logs := observer.New(zapcore.ErrorLevel)
	logger := zap.New(core)
	dummyCfg := &config.Config{
		Consumer: struct {
			Brokers          []string `mapstructure:"brokers"`
			Topic            string   `mapstructure:"topic"`
			GroupID          string   `mapstructure:"group_id"`
			MaxWorkers       int      `mapstructure:"max_workers"`
			CommitInterval   int      `mapstructure:"commit_interval_ms"`
			SessionTimeout   int      `mapstructure:"session_timeout_ms"`
			RebalanceTimeout int      `mapstructure:"rebalance_timeout_ms"`
		}{
			MaxWorkers: 1,
		},
	}

	// Create a consumer.
	cons := &Consumer{
		storage:    expectedErr,
		logger:     logger,
		config:     dummyCfg,
		workerPool: make(chan struct{}, dummyCfg.Consumer.MaxWorkers),
	}

	// Create a fake record.
	record := &kgo.Record{
		Value:     []byte("failed event"),
		Offset:    100,
		Topic:     "test-topic",
		Partition: 0,
	}

	// Process the record.
	cons.processRecord(record)

	// Wait for the goroutine to complete.
	for len(cons.workerPool) > 0 {
		// Wait for the worker to finish.
	}

	// Verify that an error was logged.
	if logs.Len() != 1 {
		t.Fatalf("expected 1 error log, got %d", logs.Len())
	}
	logEntry := logs.All()[0]
	if logEntry.Message != "Failed to store log event" {
		t.Errorf("unexpected log message: %s", logEntry.Message)
	}
}

// TestNewConsumerInvalidConfig verifies that NewConsumer returns an error with invalid config.
func TestNewConsumerInvalidConfig(t *testing.T) {
	// Provide a config with an empty Brokers slice. This should cause kgo.NewClient to fail.
	cfg := &config.Config{
		Consumer: struct {
			Brokers          []string `mapstructure:"brokers"`
			Topic            string   `mapstructure:"topic"`
			GroupID          string   `mapstructure:"group_id"`
			MaxWorkers       int      `mapstructure:"max_workers"`
			CommitInterval   int      `mapstructure:"commit_interval_ms"`
			SessionTimeout   int      `mapstructure:"session_timeout_ms"`
			RebalanceTimeout int      `mapstructure:"rebalance_timeout_ms"`
		}{
			Brokers:          []string{}, // invalid: no brokers provided
			GroupID:          "test-group",
			Topic:            "test-topic",
			MaxWorkers:       1,
			CommitInterval:   1000,
			SessionTimeout:   30000,
			RebalanceTimeout: 60000,
		},
	}

	logger := zaptest.NewLogger(t)
	store := &fakeStorage{}

	// Attempt to create a consumer.
	_, err := NewConsumer(cfg, store, logger)

	// Verify that an error was returned.
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
}

// Skip the TestStop test for now as it requires more complex mocking of the kgo.Client
func TestStop(t *testing.T) {
	t.Skip("Skipping test that requires complex mocking of kgo.Client")
}
