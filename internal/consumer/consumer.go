package consumer

import (
	"context"
	"sync"
	"time"

	"github.com/TFMV/sift/pkg/config"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

// ConsumerStorageInterface defines the interface for storage operations needed by the consumer
type ConsumerStorageInterface interface {
	StoreLogEvent(data []byte) error
	TriggerFlush()
}

// Consumer represents a Redpanda/Kafka consumer for log events
type Consumer struct {
	client     *kgo.Client
	storage    ConsumerStorageInterface
	logger     *zap.Logger
	config     *config.Config
	workerPool chan struct{}
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
}

// NewConsumer creates a new Redpanda/Kafka consumer
func NewConsumer(cfg *config.Config, store ConsumerStorageInterface, logger *zap.Logger) (*Consumer, error) {
	// Create Kafka client options
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Consumer.Brokers...),
		kgo.ConsumerGroup(cfg.Consumer.GroupID),
		kgo.ConsumeTopics(cfg.Consumer.Topic),
		kgo.DisableAutoCommit(),
		kgo.SessionTimeout(time.Duration(cfg.Consumer.SessionTimeout) * time.Millisecond),
		kgo.RebalanceTimeout(time.Duration(cfg.Consumer.RebalanceTimeout) * time.Millisecond),
	}

	// Create Kafka client
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}

	// Create context with cancel
	ctx, cancel := context.WithCancel(context.Background())

	// Create consumer
	consumer := &Consumer{
		client:     client,
		storage:    store,
		logger:     logger,
		config:     cfg,
		workerPool: make(chan struct{}, cfg.Consumer.MaxWorkers),
		ctx:        ctx,
		cancel:     cancel,
	}

	return consumer, nil
}

// Start starts consuming log events
func (c *Consumer) Start() error {
	c.logger.Info("Starting consumer",
		zap.String("group_id", c.config.Consumer.GroupID),
		zap.String("topic", c.config.Consumer.Topic),
		zap.Int("max_workers", c.config.Consumer.MaxWorkers))

	// Setup periodic commit
	commitTicker := time.NewTicker(time.Duration(c.config.Consumer.CommitInterval) * time.Millisecond)
	defer commitTicker.Stop()

	go func() {
		for {
			select {
			case <-commitTicker.C:
				c.client.CommitOffsets(c.ctx, nil, nil)
			case <-c.ctx.Done():
				return
			}
		}
	}()

	// Main polling loop
	for {
		select {
		case <-c.ctx.Done():
			return nil
		default:
			fetches := c.client.PollFetches(c.ctx)
			if fetches.IsClientClosed() {
				return nil
			}

			if errs := fetches.Errors(); len(errs) > 0 {
				for _, err := range errs {
					c.logger.Error("Error polling",
						zap.String("topic", err.Topic),
						zap.Error(err.Err))
				}
				continue
			}

			// Process fetched records
			fetches.EachRecord(func(record *kgo.Record) {
				c.processRecord(record)
			})
		}
	}
}

// Stop stops the consumer
func (c *Consumer) Stop() error {
	c.logger.Info("Stopping consumer")
	c.cancel()
	c.client.Close()
	c.wg.Wait()
	return nil
}

// processRecord processes a single Kafka record
func (c *Consumer) processRecord(record *kgo.Record) {
	// Acquire a worker from the pool
	c.workerPool <- struct{}{}
	c.wg.Add(1)

	go func() {
		defer func() {
			// Release worker back to pool
			<-c.workerPool
			c.wg.Done()
		}()

		// Process the record (deserialize flatbuffer and store)
		if err := c.storage.StoreLogEvent(record.Value); err != nil {
			c.logger.Error("Failed to store log event",
				zap.Error(err),
				zap.Int64("offset", record.Offset),
				zap.String("topic", record.Topic),
				zap.Int32("partition", record.Partition))
		}
	}()
}
