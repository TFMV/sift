package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/TFMV/sift/internal/api"
	"github.com/TFMV/sift/internal/consumer"
	"github.com/TFMV/sift/internal/observability"
	"github.com/TFMV/sift/internal/storage"
	"github.com/TFMV/sift/pkg/config"
	"go.uber.org/zap"
)

func main() {
	// Initialize configuration
	cfg, err := config.Load()
	if err != nil {
		fmt.Printf("Failed to load configuration: %v\n", err)
		os.Exit(1)
	}

	// Initialize logger
	logger, err := observability.NewLogger(cfg.LogLevel)
	if err != nil {
		fmt.Printf("Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	// Initialize telemetry
	tp, err := observability.InitTracer(cfg.ServiceName)
	if err != nil {
		logger.Fatal("Failed to initialize tracer", zap.Error(err))
	}
	defer func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			logger.Error("Error shutting down tracer provider", zap.Error(err))
		}
	}()

	// Initialize storage
	store, err := storage.NewStorage(cfg, logger)
	if err != nil {
		logger.Fatal("Failed to initialize storage", zap.Error(err))
	}
	defer store.Close()

	// Initialize Redpanda consumer
	cons, err := consumer.NewConsumer(cfg, store, logger)
	if err != nil {
		logger.Fatal("Failed to initialize consumer", zap.Error(err))
	}

	// Initialize API server
	apiServer, err := api.NewServer(cfg, store, logger)
	if err != nil {
		logger.Fatal("Failed to initialize API server", zap.Error(err))
	}

	// Create a cancellation signal
	_, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start consumer in the background
	consumerErrCh := make(chan error, 1)
	go func() {
		if err := cons.Start(); err != nil {
			logger.Error("Consumer error", zap.Error(err))
			consumerErrCh <- err
		}
	}()

	// Start API server in the background
	apiErrCh := make(chan error, 1)
	go func() {
		if err := apiServer.Start(); err != nil {
			logger.Error("API server error", zap.Error(err))
			apiErrCh <- err
		}
	}()

	logger.Info("Sift is running...",
		zap.String("version", cfg.Version),
		zap.String("consumer_brokers", fmt.Sprintf("%v", cfg.Consumer.Brokers)),
		zap.Int("api_port", cfg.API.Port))

	// Graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Wait for shutdown signal or error
	select {
	case <-sigCh:
		logger.Info("Shutdown signal received")
	case err := <-consumerErrCh:
		logger.Error("Consumer failed", zap.Error(err))
	case err := <-apiErrCh:
		logger.Error("API server failed", zap.Error(err))
	}

	// Cancel context to signal shutdown to all components
	cancel()

	// Set a timeout for graceful shutdown
	time.AfterFunc(15*time.Second, func() {
		logger.Error("Graceful shutdown timed out, forcing exit")
		os.Exit(1)
	})

	logger.Info("Gracefully shutting down...")

	// Stop consumer
	if err := cons.Stop(); err != nil {
		logger.Error("Error stopping consumer", zap.Error(err))
	}

	// Stop API server
	if err := apiServer.Stop(); err != nil {
		logger.Error("Error stopping API server", zap.Error(err))
	}

	logger.Info("Shutdown complete")
}
