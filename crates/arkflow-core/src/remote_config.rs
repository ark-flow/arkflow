/*
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

//! Remote Configuration Management Module
//!
//! This module provides functionality to automatically fetch configuration from remote APIs
//! and manage stream processing pipelines dynamically.

use crate::config::{EngineConfig, LoggingConfig};
use crate::stream::StreamConfig;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

/// Remote configuration response structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteConfigResponse {
    /// Configuration version for change detection
    pub version: String,
    /// List of pipeline configurations
    pub streams: Vec<StreamInfo>,
}

/// Stream information from remote API
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamInfo {
    /// Unique pipeline identifier
    pub id: String,
    /// Pipeline name
    pub name: String,
    /// Pipeline status (active, inactive, deleted)
    pub status: StreamStatus,
    /// Stream configuration
    pub config: StreamConfig,
    /// Configuration version
    pub version: String,
}

/// Steam status enumeration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum StreamStatus {
    Active,
    Inactive,
    Deleted,
}

/// Stream runtime information
#[derive(Debug)]
struct StreamRuntime {
    /// Pipeline information
    info: StreamInfo,
    /// Cancellation token for stopping the pipeline
    cancellation_token: CancellationToken,
    /// Task handle
    handle: Option<tokio::task::JoinHandle<()>>,
}

/// Remote configuration manager
pub struct RemoteConfigManager {
    /// Remote API endpoint URL
    api_url: String,
    /// Polling interval in seconds
    poll_interval: u64,
    /// Authentication token
    auth_token: Option<String>,
    /// HTTP client
    client: reqwest::Client,
    /// Currently running pipelines
    streams: Arc<RwLock<HashMap<String, StreamRuntime>>>,
    /// Last known configuration version
    last_version: Arc<RwLock<Option<String>>>,
}

impl RemoteConfigManager {
    /// Create a new remote configuration manager
    pub fn new(api_url: String, poll_interval: u64, auth_token: Option<String>) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .expect("Failed to create HTTP client");

        Self {
            api_url,
            poll_interval,
            auth_token,
            client,
            streams: Arc::new(RwLock::new(HashMap::new())),
            last_version: Arc::new(RwLock::new(None)),
        }
    }

    /// Start the remote configuration management loop
    pub async fn run(&self, token: CancellationToken) -> Result<(), Box<dyn std::error::Error>> {
        info!("Starting remote configuration manager");
        info!("Polling interval: {} seconds", self.poll_interval);
        info!("API endpoint: {}", self.api_url);

        // Initialize default logging
        self.init_default_logging();

        let mut interval_timer = interval(Duration::from_secs(self.poll_interval));

        loop {
            tokio::select! {
                _ = token.cancelled() => {
                        info!("Shutting down remote configuration manager");
                        break;
                }
                _ = interval_timer.tick() => {
                        if let Err(e) = self.fetch_and_update_config().await {
                            error!("Failed to fetch remote configuration: {}", e);
                        }
                }
            }
        }
        Ok(())
    }

    /// Initialize default logging configuration
    fn init_default_logging(&self) {
        let default_config = EngineConfig {
            streams: vec![],
            logging: LoggingConfig::default(),
            health_check: crate::config::HealthCheckConfig::default(),
        };
        crate::cli::init_logging(&default_config);
    }

    /// Fetch configuration from remote API and update pipelines
    async fn fetch_and_update_config(&self) -> Result<(), Box<dyn std::error::Error>> {
        let config = self.fetch_remote_config().await?;

        // Check if configuration has changed
        let last_version = self.last_version.read().await;
        if let Some(ref last_ver) = *last_version {
            if last_ver == &config.version {
                // No changes, skip update
                return Ok(());
            }
        }
        drop(last_version);

        info!(
            "Configuration changed, updating pipelines (version: {})",
            config.version
        );

        // Update pipelines
        self.update_streams(config.streams).await?;

        // Update version
        let mut last_version = self.last_version.write().await;
        *last_version = Some(config.version);

        Ok(())
    }

    /// Fetch configuration from remote API
    async fn fetch_remote_config(
        &self,
    ) -> Result<RemoteConfigResponse, Box<dyn std::error::Error>> {
        let mut request = self.client.get(&self.api_url);

        // Add authentication header if token is provided
        if let Some(ref token) = self.auth_token {
            request = request.header("Authorization", format!("Bearer {}", token));
        }

        let response = request.send().await?;

        if !response.status().is_success() {
            return Err(format!("HTTP error: {}", response.status()).into());
        }

        let config: RemoteConfigResponse = response.json().await?;
        Ok(config)
    }

    /// Update pipelines based on remote configuration
    async fn update_streams(
        &self,
        new_streams: Vec<StreamInfo>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut streams = self.streams.write().await;
        let mut new_streams_ids = std::collections::HashSet::new();

        // Process new/updated pipelines
        for stream_info in new_streams {
            new_streams_ids.insert(stream_info.id.clone());

            match stream_info.status {
                StreamStatus::Active => {
                    if let Some(existing) = streams.get(&stream_info.id) {
                        // Check if pipeline needs to be restarted
                        if existing.info.version != stream_info.version {
                            info!(
                                "Restarting pipeline '{}' (version: {} -> {})",
                                stream_info.name, existing.info.version, stream_info.version
                            );

                            // Stop existing stream
                            existing.cancellation_token.cancel();
                            if let Some(handle) = &existing.handle {
                                let _ = handle.abort();
                            }

                            // Start new stream
                            self.start_stream(&mut streams, stream_info).await?;
                        }
                    } else {
                        // Start new stream
                        info!("Starting new stream '{}'", stream_info.name);
                        self.start_stream(&mut streams, stream_info).await?;
                    }
                }
                StreamStatus::Inactive => {
                    if let Some(existing) = streams.get(&stream_info.id) {
                        info!("Stopping stream '{}'", stream_info.name);
                        existing.cancellation_token.cancel();
                        if let Some(handle) = &existing.handle {
                            let _ = handle.abort();
                        }
                        streams.remove(&stream_info.id);
                    }
                }
                StreamStatus::Deleted => {
                    if let Some(existing) = streams.get(&stream_info.id) {
                        info!("Deleting stream '{}'", stream_info.name);
                        existing.cancellation_token.cancel();
                        if let Some(handle) = &existing.handle {
                            let _ = handle.abort();
                        }
                        streams.remove(&stream_info.id);
                    }
                }
            }
        }

        // Remove stream that are no longer in the configuration
        let current_ids: Vec<String> = streams.keys().cloned().collect();
        for id in current_ids {
            if !new_streams_ids.contains(&id) {
                if let Some(existing) = streams.get(&id) {
                    warn!(
                        "Removing stream '{}' (no longer in remote config)",
                        existing.info.name
                    );
                    existing.cancellation_token.cancel();
                    if let Some(handle) = &existing.handle {
                        let _ = handle.abort();
                    }
                }
                streams.remove(&id);
            }
        }

        Ok(())
    }

    /// Start a new stream
    async fn start_stream(
        &self,
        streams: &mut HashMap<String, StreamRuntime>,
        stream_info: StreamInfo,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let cancellation_token = CancellationToken::new();
        let token_clone = cancellation_token.clone();
        let config_clone = stream_info.config.clone();
        let stream_name = stream_info.name.clone();
        let stream_id = stream_info.id.clone();

        // Build and start the stream
        let handle = tokio::spawn(async move {
            match config_clone.build() {
                Ok(mut stream) => {
                    info!("Stream '{}' started successfully", stream_name);
                    if let Err(e) = stream.run(token_clone).await {
                        error!("Stream '{}' error: {}", stream_name, e);
                    } else {
                        info!("Stream '{}' completed", stream_name);
                    }
                }
                Err(e) => {
                    error!("Failed to build stream '{}': {}", stream_name, e);
                }
            }
        });

        let runtime = StreamRuntime {
            info: stream_info,
            cancellation_token,
            handle: Some(handle),
        };

        streams.insert(stream_id, runtime);
        Ok(())
    }
}
