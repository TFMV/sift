package config

import (
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

// Config holds all configuration for the application
type Config struct {
	Version     string `mapstructure:"version"`
	ServiceName string `mapstructure:"service_name"`
	LogLevel    string `mapstructure:"log_level"`

	API struct {
		Port    int    `mapstructure:"port"`
		Host    string `mapstructure:"host"`
		BaseURL string `mapstructure:"base_url"`
	} `mapstructure:"api"`

	Consumer struct {
		Brokers          []string `mapstructure:"brokers"`
		Topic            string   `mapstructure:"topic"`
		GroupID          string   `mapstructure:"group_id"`
		MaxWorkers       int      `mapstructure:"max_workers"`
		CommitInterval   int      `mapstructure:"commit_interval_ms"`
		SessionTimeout   int      `mapstructure:"session_timeout_ms"`
		RebalanceTimeout int      `mapstructure:"rebalance_timeout_ms"`
	} `mapstructure:"consumer"`

	Storage struct {
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
	} `mapstructure:"storage"`
}

// Load loads configuration from file and environment variables
func Load() (*Config, error) {
	var config Config

	// Set defaults
	setDefaults()

	// Read from config file
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")
	viper.AddConfigPath("./config")
	viper.AddConfigPath("$HOME/.sift")
	viper.AddConfigPath("/etc/sift/")

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, fmt.Errorf("error reading config file: %w", err)
		}
		// Continue if config file not found, will rely on defaults and env vars
	}

	// Read from environment variables
	viper.SetEnvPrefix("SIFT")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()

	// Unmarshal into config struct
	if err := viper.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("unable to decode config into struct: %w", err)
	}

	return &config, nil
}

// setDefaults sets default values for configuration
func setDefaults() {
	// Service defaults
	viper.SetDefault("version", "0.1.0")
	viper.SetDefault("service_name", "sift")
	viper.SetDefault("log_level", "info")

	// API defaults
	viper.SetDefault("api.port", 8080)
	viper.SetDefault("api.host", "0.0.0.0")
	viper.SetDefault("api.base_url", "/api/v1")

	// Consumer defaults
	viper.SetDefault("consumer.brokers", []string{"localhost:9092"})
	viper.SetDefault("consumer.topic", "logs")
	viper.SetDefault("consumer.group_id", "sift-consumer")
	viper.SetDefault("consumer.max_workers", 10)
	viper.SetDefault("consumer.commit_interval_ms", 5000)
	viper.SetDefault("consumer.session_timeout_ms", 30000)
	viper.SetDefault("consumer.rebalance_timeout_ms", 60000)

	// Storage defaults
	viper.SetDefault("storage.duckdb.path", "./data/sift.db")
	viper.SetDefault("storage.duckdb.memory_db", false)

	viper.SetDefault("storage.parquet.use_local_fs", true)
	viper.SetDefault("storage.parquet.local_path", "./data/archive")
	viper.SetDefault("storage.parquet.rotation_interval_hr", 24)
	viper.SetDefault("storage.parquet.retention_days", 30)
}
