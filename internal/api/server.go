package api

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/TFMV/sift/pkg/config"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/compress"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/helmet"
	"github.com/gofiber/fiber/v2/middleware/limiter"
	fiberLogger "github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/gofiber/fiber/v2/middleware/monitor"
	"github.com/gofiber/fiber/v2/middleware/recover"
	"go.uber.org/zap"
)

// StorageInterface defines the interface for storage operations needed by the API
type StorageInterface interface {
	QueryLogs(filter map[string]interface{}, limit int, offset int) ([]map[string]interface{}, error)
	TriggerFlush()
}

// Server represents the API server
type Server struct {
	app     *fiber.App
	config  *config.Config
	storage StorageInterface
	logger  *zap.Logger
}

// NewServer creates a new API server
func NewServer(cfg *config.Config, store StorageInterface, logger *zap.Logger) (*Server, error) {
	// Create Fiber app with enhanced configuration
	app := fiber.New(fiber.Config{
		AppName:               "Sift",
		DisableStartupMessage: true,
		IdleTimeout:           30 * time.Second,
		ReadTimeout:           10 * time.Second,
		WriteTimeout:          10 * time.Second,
		ErrorHandler:          customErrorHandler(logger),
		// Prevent fiber from leaking server info in headers
		ServerHeader: "",
		// Disable header containing used framework
		DisableHeaderNormalizing: false,
	})

	// Add middleware
	app.Use(recover.New())

	// Add security headers with helmet
	app.Use(helmet.New())

	app.Use(cors.New(cors.Config{
		AllowOrigins:     "http://localhost:3000, http://localhost:5555",
		AllowMethods:     "GET,POST,HEAD,PUT,DELETE,PATCH",
		AllowHeaders:     "Origin, Content-Type, Accept, Authorization",
		AllowCredentials: true,
		MaxAge:           300,
	}))
	app.Use(fiberLogger.New(fiberLogger.Config{
		Format: "[${time}] ${status} - ${latency} ${method} ${path}\n",
	}))
	app.Use(compress.New())                  // Enable gzip compression
	app.Use(customLoggingMiddleware(logger)) // Add structured logging

	// Add rate limiting middleware
	app.Use(limiter.New(limiter.Config{
		Max:        100, // 100 requests per minute
		Expiration: 1 * time.Minute,
		KeyGenerator: func(c *fiber.Ctx) string {
			return c.IP() // Rate limit by IP address
		},
		LimitReached: func(c *fiber.Ctx) error {
			logger.Warn("Rate limit reached",
				zap.String("ip", c.IP()),
				zap.String("path", c.Path()),
			)
			return c.Status(fiber.StatusTooManyRequests).JSON(fiber.Map{
				"error":   true,
				"message": "Rate limit exceeded. Please try again later.",
			})
		},
	}))

	// Create server
	server := &Server{
		app:     app,
		config:  cfg,
		storage: store,
		logger:  logger,
	}

	// Register routes
	server.registerRoutes()

	return server, nil
}

// Start starts the API server with graceful shutdown handling
func (s *Server) Start() error {
	addr := fmt.Sprintf("%s:%d", s.config.API.Host, s.config.API.Port)
	s.logger.Info("Starting API server", zap.String("address", addr))

	// Setup graceful shutdown
	idleConnsClosed := make(chan struct{})
	go func() {
		sigint := make(chan os.Signal, 1)
		signal.Notify(sigint, os.Interrupt, syscall.SIGTERM)
		<-sigint

		s.logger.Info("Shutdown signal received")

		// Give server 10 seconds to shutdown gracefully
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := s.app.ShutdownWithContext(ctx); err != nil {
			s.logger.Error("Server shutdown error", zap.Error(err))
		}

		close(idleConnsClosed)
	}()

	// Start server
	if err := s.app.Listen(addr); err != nil {
		return fmt.Errorf("failed to start API server: %w", err)
	}

	<-idleConnsClosed
	return nil
}

// Stop stops the API server
func (s *Server) Stop() error {
	s.logger.Info("Stopping API server")
	return s.app.Shutdown()
}

// registerRoutes registers all API routes
func (s *Server) registerRoutes() {
	// API group
	api := s.app.Group(s.config.API.BaseURL)

	// Health and monitoring endpoints
	api.Get("/health", s.healthCheck)
	api.Get("/metrics", monitor.New()) // Add metrics endpoint
	api.Get("/version", s.versionHandler)
	api.Get("/liveness", s.livenessHandler)
	api.Get("/readiness", s.readinessHandler)

	// Logs endpoints
	logs := api.Group("/logs")
	logs.Get("/", s.getLogs)
	logs.Post("/query", s.queryLogs)
	logs.Get("/stats", s.getStats)

	// Admin endpoints with stricter rate limits
	admin := api.Group("/admin")
	admin.Use(limiter.New(limiter.Config{
		Max:        10, // 10 requests per minute for admin endpoints
		Expiration: 1 * time.Minute,
		KeyGenerator: func(c *fiber.Ctx) string {
			return c.IP()
		},
	}))
	admin.Post("/flush", s.triggerFlush)
}

// customErrorHandler provides structured error handling
func customErrorHandler(log *zap.Logger) fiber.ErrorHandler {
	return func(c *fiber.Ctx, err error) error {
		code := fiber.StatusInternalServerError
		message := "Internal Server Error"

		if e, ok := err.(*fiber.Error); ok {
			code = e.Code
			message = e.Message
		}

		log.Error("Request failed",
			zap.String("method", c.Method()),
			zap.String("path", c.Path()),
			zap.Int("status", code),
			zap.Error(err),
		)

		// Respect "Accept" headers for response format
		if c.Accepts("text/html") != "" {
			return c.Status(code).SendString(fmt.Sprintf("<h1>Error %d</h1><p>%s</p>", code, message))
		}

		return c.Status(code).JSON(fiber.Map{
			"error":   true,
			"message": message,
		})
	}
}

// customLoggingMiddleware logs requests in a structured format
func customLoggingMiddleware(log *zap.Logger) fiber.Handler {
	return func(c *fiber.Ctx) error {
		start := time.Now()
		err := c.Next()
		duration := time.Since(start)

		// Ensure status is set to avoid misleading logs
		status := c.Response().StatusCode()
		if status == 0 {
			status = fiber.StatusInternalServerError
		}

		fields := []zap.Field{
			zap.String("method", c.Method()),
			zap.String("path", c.Path()),
			zap.Int("status", status),
			zap.Duration("duration", duration),
			zap.String("client_ip", c.IP()),
			zap.String("user_agent", c.Get("User-Agent", "")),
		}

		if err != nil {
			fields = append(fields, zap.Error(err))
		}

		log.Info("Request handled", fields...)
		return err
	}
}

// healthCheck returns the health status of the API
func (s *Server) healthCheck(c *fiber.Ctx) error {
	return c.JSON(fiber.Map{
		"status":  "ok",
		"version": s.config.Version,
		"time":    time.Now().Format(time.RFC3339),
	})
}

// versionHandler provides API version information
func (s *Server) versionHandler(c *fiber.Ctx) error {
	s.logger.Debug("Version endpoint hit")
	return c.JSON(fiber.Map{
		"service": "Sift API",
		"version": s.config.Version,
		"time":    time.Now().UTC().Format(time.RFC3339),
	})
}

// livenessHandler for Kubernetes health probes
func (s *Server) livenessHandler(c *fiber.Ctx) error {
	return c.SendStatus(fiber.StatusOK)
}

// readinessHandler for Kubernetes health probes
func (s *Server) readinessHandler(c *fiber.Ctx) error {
	// Check if storage is available
	if s.storage == nil {
		return c.SendStatus(fiber.StatusServiceUnavailable)
	}
	return c.SendStatus(fiber.StatusOK)
}

// getLogs returns logs with simple filtering
func (s *Server) getLogs(c *fiber.Ctx) error {
	// Parse query parameters
	filter := make(map[string]interface{})

	// Example filters
	if service := c.Query("service"); service != "" {
		filter["service"] = service
	}
	if level := c.Query("level"); level != "" {
		filter["level"] = level
	}

	// Parse limit and offset
	limit := 100
	if limitStr := c.Query("limit"); limitStr != "" {
		if parsedLimit, err := strconv.Atoi(limitStr); err == nil {
			limit = parsedLimit
		}
	}

	offset := 0
	if offsetStr := c.Query("offset"); offsetStr != "" {
		if parsedOffset, err := strconv.Atoi(offsetStr); err == nil {
			offset = parsedOffset
		}
	}

	// Query logs
	logs, err := s.storage.QueryLogs(filter, limit, offset)
	if err != nil {
		s.logger.Error("Failed to query logs", zap.Error(err))
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to query logs",
		})
	}

	return c.JSON(fiber.Map{
		"logs":  logs,
		"count": len(logs),
	})
}

// queryLogs handles complex log queries
func (s *Server) queryLogs(c *fiber.Ctx) error {
	// Parse request body
	var req struct {
		Filter map[string]interface{} `json:"filter"`
		Limit  int                    `json:"limit"`
		Offset int                    `json:"offset"`
	}

	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Invalid request body",
		})
	}

	// Set defaults
	if req.Limit <= 0 {
		req.Limit = 100
	}

	// Query logs
	logs, err := s.storage.QueryLogs(req.Filter, req.Limit, req.Offset)
	if err != nil {
		s.logger.Error("Failed to query logs", zap.Error(err))
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to query logs",
		})
	}

	return c.JSON(fiber.Map{
		"logs":  logs,
		"count": len(logs),
	})
}

// getStats returns log statistics
func (s *Server) getStats(c *fiber.Ctx) error {
	// In a real implementation, we would query DuckDB for stats
	// This is just a placeholder
	return c.JSON(fiber.Map{
		"total_logs": 1000,
		"services": []fiber.Map{
			{"name": "api", "count": 500},
			{"name": "worker", "count": 300},
			{"name": "scheduler", "count": 200},
		},
		"levels": []fiber.Map{
			{"level": "INFO", "count": 800},
			{"level": "WARN", "count": 150},
			{"level": "ERROR", "count": 50},
		},
	})
}

// triggerFlush triggers a manual flush to long-term storage
func (s *Server) triggerFlush(c *fiber.Ctx) error {
	// Trigger flush in a separate goroutine
	go s.storage.TriggerFlush()

	return c.JSON(fiber.Map{
		"status":  "ok",
		"message": "Flush triggered",
	})
}
