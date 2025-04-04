//! Configuration module
//!
//! Provide configuration management for the stream processing engine.

use serde::{Deserialize, Serialize};
use toml;

use crate::{stream::StreamConfig, Error};

/// Configuration file format
#[derive(Debug, Clone, Copy)]
pub enum ConfigFormat {
    YAML,
    JSON,
    TOML,
}

/// Log configuration

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    /// Log level
    pub level: String,
    /// Output to file?
    pub file_output: Option<bool>,
    /// Log file path
    pub file_path: Option<String>,
}

/// Health check configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheckConfig {
    /// Whether health check is enabled
    pub enabled: bool,
    /// Listening address for health check server
    pub address: String,
    /// Path for health check endpoint
    pub path: String,
    /// Path for readiness check endpoint
    pub readiness_path: String,
    /// Path for liveness check endpoint
    pub liveness_path: String,
}

/// Engine configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineConfig {
    /// Streams configuration
    pub streams: Vec<StreamConfig>,
    /// Logging configuration (optional)
    pub logging: Option<LoggingConfig>,
    /// Health check configuration (optional)
    pub health_check: Option<HealthCheckConfig>,
}

impl EngineConfig {
    /// Load configuration from file
    pub fn from_file(path: &str) -> Result<Self, Error> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| Error::Config(format!("Unable to read configuration file: {}", e)))?;

        // Determine the format based on the file extension.
        if let Some(format) = get_format_from_path(path) {
            return match format {
                ConfigFormat::YAML => serde_yaml::from_str(&content)
                    .map_err(|e| Error::Config(format!("YAML parsing error: {}", e))),
                ConfigFormat::JSON => serde_json::from_str(&content)
                    .map_err(|e| Error::Config(format!("JSON parsing error: {}", e))),
                ConfigFormat::TOML => toml::from_str(&content)
                    .map_err(|e| Error::Config(format!("TOML parsing error: {}", e))),
            };
        };

        Err(Error::Config("The configuration file format cannot be determined. Please use YAML, JSON, or TOML format.".to_string()))
    }
}

/// Get configuration format from file path.
fn get_format_from_path(path: &str) -> Option<ConfigFormat> {
    let path = path.to_lowercase();
    if path.ends_with(".yaml") || path.ends_with(".yml") {
        Some(ConfigFormat::YAML)
    } else if path.ends_with(".json") {
        Some(ConfigFormat::JSON)
    } else if path.ends_with(".toml") {
        Some(ConfigFormat::TOML)
    } else {
        None
    }
}
