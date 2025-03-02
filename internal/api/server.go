package api

import (
	"fmt"
	"strconv"
	"time"

	"github.com/TFMV/sift/internal/storage"
	"github.com/TFMV/sift/pkg/config"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	fiberLogger "github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/gofiber/fiber/v2/middleware/recover"
	"go.uber.org/zap"
)

// Server represents the API server
type Server struct {
	app     *fiber.App
	config  *config.Config
	storage *storage.Storage
	logger  *zap.Logger
}

// NewServer creates a new API server
func NewServer(cfg *config.Config, store *storage.Storage, logger *zap.Logger) (*Server, error) {
	// Create Fiber app
	app := fiber.New(fiber.Config{
		AppName:               "Sift",
		DisableStartupMessage: true,
		IdleTimeout:           30 * time.Second,
		ReadTimeout:           10 * time.Second,
		WriteTimeout:          10 * time.Second,
	})

	// Add middleware
	app.Use(recover.New())
	app.Use(cors.New())
	app.Use(fiberLogger.New(fiberLogger.Config{
		Format: "[${time}] ${status} - ${latency} ${method} ${path}\n",
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

// Start starts the API server
func (s *Server) Start() error {
	addr := fmt.Sprintf("%s:%d", s.config.API.Host, s.config.API.Port)
	s.logger.Info("Starting API server", zap.String("address", addr))
	return s.app.Listen(addr)
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

	// Health check
	api.Get("/health", s.healthCheck)

	// Logs endpoints
	logs := api.Group("/logs")
	logs.Get("/", s.getLogs)
	logs.Post("/query", s.queryLogs)
	logs.Get("/stats", s.getStats)

	// Admin endpoints
	admin := api.Group("/admin")
	admin.Post("/flush", s.triggerFlush)
}

// healthCheck returns the health status of the API
func (s *Server) healthCheck(c *fiber.Ctx) error {
	return c.JSON(fiber.Map{
		"status":  "ok",
		"version": s.config.Version,
		"time":    time.Now().Format(time.RFC3339),
	})
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
