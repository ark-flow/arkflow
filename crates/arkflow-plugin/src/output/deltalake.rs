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

//! Delta Lake output component
//!
//! This component writes data to Delta Lake tables stored in object storage.

use std::collections::HashMap;
use std::sync::Arc;

use arkflow_core::output::{register_output_builder, Output, OutputBuilder};
use arkflow_core::{Error, MessageBatch, Resource};
use async_trait::async_trait;
use deltalake::kernel::Schema;
use deltalake::{
    arrow::record_batch::RecordBatch, operations::create::CreateBuilder, DeltaOps, DeltaTable,
    DeltaTableBuilder,
};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Delta Lake output configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
struct DeltaLakeOutputConfig {
    /// Delta table URI (e.g., s3://bucket/path/to/table, azure://container/path, gs://bucket/path)
    table_uri: String,
    /// Write mode: append, overwrite, error, ignore
    #[serde(default = "default_write_mode")]
    write_mode: WriteMode,
    /// Storage options for authentication and configuration
    #[serde(default)]
    storage_options: HashMap<String, String>,
    /// Whether to create table if it doesn't exist
    #[serde(default = "default_create_if_not_exists")]
    create_if_not_exists: bool,
    /// Schema for table creation (optional, inferred from data if not provided)
    schema: Option<String>,
    /// Partition columns (optional)
    #[serde(default)]
    partition_columns: Vec<String>,
    /// Maximum number of retries for write operations
    #[serde(default = "default_max_retries")]
    max_retries: u32,
    /// Retry delay in milliseconds
    #[serde(default = "default_retry_delay_ms")]
    retry_delay_ms: u64,
}

/// Write mode enumeration for better type safety
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
enum WriteMode {
    Append,
    Overwrite,
    Error,
    Ignore,
}

fn default_write_mode() -> WriteMode {
    WriteMode::Append
}

fn default_create_if_not_exists() -> bool {
    true
}

fn default_max_retries() -> u32 {
    3
}

fn default_retry_delay_ms() -> u64 {
    1000
}

/// Delta Lake output component
struct DeltaLakeOutput {
    config: DeltaLakeOutputConfig,
    table: Arc<RwLock<Option<DeltaTable>>>,
}

impl DeltaLakeOutput {
    /// Create a new Delta Lake output component
    pub fn new(config: DeltaLakeOutputConfig) -> Result<Self, Error> {
        // Validate configuration at creation time
        Self::validate_config(&config)?;
        
        Ok(Self {
            config,
            table: Arc::new(RwLock::new(None)),
        })
    }
    
    /// Validate configuration
    fn validate_config(config: &DeltaLakeOutputConfig) -> Result<(), Error> {
        if config.table_uri.trim().is_empty() {
            return Err(Error::Config("table_uri cannot be empty".to_string()));
        }
        
        // Validate URI format
        if !config.table_uri.starts_with("s3://") 
            && !config.table_uri.starts_with("azure://") 
            && !config.table_uri.starts_with("gs://")
            && !config.table_uri.starts_with("file://")
            && !config.table_uri.starts_with("/") {
            warn!("Table URI '{}' may not be a valid storage path", config.table_uri);
        }
        
        Ok(())
    }

    /// Get or create the Delta table
    async fn get_or_create_table(&self) -> Result<DeltaTable, Error> {
        // First try read lock for better concurrency
        {
            let table_guard = self.table.read().await;
            if let Some(table) = table_guard.as_ref() {
                return Ok(table.clone());
            }
        }
        
        // Need write lock to initialize table
        let mut table_guard = self.table.write().await;
        
        // Double-check pattern to avoid race condition
        if let Some(table) = table_guard.as_ref() {
            return Ok(table.clone());
        }

        // Try to load existing table
        let mut builder = DeltaTableBuilder::from_uri(&self.config.table_uri);

        // Add storage options
        builder = builder.with_storage_options(self.config.storage_options.clone());

        match builder.load().await {
            Ok(table) => {
                info!("Loaded existing Delta table from {}", self.config.table_uri);
                table_guard.replace(table.clone());
                Ok(table)
            }
            Err(load_err) if self.config.create_if_not_exists => {
                info!("Creating new Delta table at {} (load error: {})", self.config.table_uri, load_err);
                self.create_table_internal(&mut table_guard).await
            }
            Err(e) => Err(Error::Connection(format!(
                "Failed to load Delta table and create_if_not_exists is false: {}",
                e
            ))),
        }
    }

    /// Create a new Delta table (internal method with write lock already held)
    async fn create_table_internal(
        &self,
        table_guard: &mut tokio::sync::RwLockWriteGuard<'_, Option<DeltaTable>>,
    ) -> Result<DeltaTable, Error> {
        let mut create_builder = CreateBuilder::new()
            .with_location(&self.config.table_uri)
            .with_storage_options(self.config.storage_options.clone());

        // Add partition columns if specified
        if !self.config.partition_columns.is_empty() {
            create_builder =
                create_builder.with_partition_columns(self.config.partition_columns.clone());
        }

        // If schema is provided, parse and use it
        if let Some(schema_str) = &self.config.schema {
            let schema = self.parse_schema(schema_str)?;
            let fields: Vec<_> = schema.fields().cloned().collect();
            create_builder = create_builder.with_columns(fields);
        }

        let table = create_builder
            .await
            .map_err(|e| Error::Connection(format!("Failed to create Delta table: {}", e)))?;

        table_guard.replace(table.clone());
        Ok(table)
    }

    /// Execute write operation with retry logic
    async fn write_with_retry(&self, record_batch: RecordBatch) -> Result<DeltaTable, Error> {
        let mut last_error = None;
        
        for attempt in 0..=self.config.max_retries {
            if attempt > 0 {
                let delay = std::time::Duration::from_millis(self.config.retry_delay_ms * attempt as u64);
                debug!("Retrying write operation after {:?} (attempt {})", delay, attempt);
                tokio::time::sleep(delay).await;
            }
            
            match self.execute_write_operation(record_batch.clone()).await {
                Ok(table) => return Ok(table),
                Err(e) => {
                    warn!("Write attempt {} failed: {}", attempt + 1, e);
                    last_error = Some(e);
                }
            }
        }
        
        Err(last_error.unwrap_or_else(|| Error::Process("All write attempts failed".to_string())))
    }
    
    /// Execute the actual write operation
    async fn execute_write_operation(&self, record_batch: RecordBatch) -> Result<DeltaTable, Error> {
        let table = self.get_or_create_table().await?;
        let ops = DeltaOps::from(table);

        let result = match self.config.write_mode {
            WriteMode::Append => ops
                .write([record_batch])
                .await
                .map_err(|e| Error::Process(format!("Failed to append to Delta table: {}", e)))?,
            WriteMode::Overwrite => ops
                .write([record_batch])
                .with_save_mode(deltalake::protocol::SaveMode::Overwrite)
                .await
                .map_err(|e| Error::Process(format!("Failed to overwrite Delta table: {}", e)))?,
            WriteMode::Error | WriteMode::Ignore => {
                return Err(Error::Config(format!(
                    "Write mode '{:?}' not implemented yet",
                    self.config.write_mode
                )));
            }
        };
        
        Ok(result)
    }

    /// Parse schema from JSON string
    fn parse_schema(&self, schema_str: &str) -> Result<Schema, Error> {
        serde_json::from_str(schema_str)
            .map_err(|e| Error::Config(format!("Invalid schema JSON: {}", e)))
    }
}

#[async_trait]
impl Output for DeltaLakeOutput {
    async fn connect(&self) -> Result<(), Error> {
        // Initialize the table connection
        self.get_or_create_table().await?;
        info!("Connected to Delta table at {}", self.config.table_uri);
        Ok(())
    }

    async fn write(&self, msg: MessageBatch) -> Result<(), Error> {
        // Convert MessageBatch to RecordBatch
        let record_batch: RecordBatch = msg.into();
        
        // Early return for empty batches
        if record_batch.num_rows() == 0 {
            debug!("Skipping write for empty record batch");
            return Ok(());
        }

        debug!("Writing {} rows to Delta table", record_batch.num_rows());

        // Execute write with retry logic
        let result = self.write_with_retry(record_batch).await?;

        debug!(
            "Successfully wrote data to Delta table, version: {}",
            result.version()
        );

        // Update the table reference with the new version
        let mut table_guard = self.table.write().await;
        table_guard.replace(result);

        Ok(())
    }

    async fn close(&self) -> Result<(), Error> {
        info!("Closing Delta Lake output connection");
        let mut table_guard = self.table.write().await;
        table_guard.take();
        Ok(())
    }
}

/// Delta Lake output builder
pub(crate) struct DeltaLakeOutputBuilder;

impl OutputBuilder for DeltaLakeOutputBuilder {
    fn build(
        &self,
        _name: Option<&String>,
        config: &Option<serde_json::Value>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Output>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "Delta Lake output configuration is missing".to_string(),
            ));
        }

        // Parse the configuration
        let config: DeltaLakeOutputConfig = serde_json::from_value(config.clone().unwrap())
            .map_err(|e| Error::Config(format!("Invalid Delta Lake configuration: {}", e)))?;

        // Validate required fields
        if config.table_uri.is_empty() {
            return Err(Error::Config("table_uri is required".to_string()));
        }

        Ok(Arc::new(DeltaLakeOutput::new(config)?))
    }
}

/// Initialize the Delta Lake output component
pub fn init() -> Result<(), Error> {
    register_output_builder("deltalake", Arc::new(DeltaLakeOutputBuilder))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_config_parsing() {
        let config_json = json!({
            "table_uri": "s3://my-bucket/my-table",
            "write_mode": "append",
            "storage_options": {
                "AWS_ACCESS_KEY_ID": "test",
                "AWS_SECRET_ACCESS_KEY": "test"
            },
            "create_if_not_exists": true,
            "partition_columns": ["year", "month"],
            "max_retries": 5,
            "retry_delay_ms": 2000
        });

        let config: DeltaLakeOutputConfig = serde_json::from_value(config_json).unwrap();
        assert_eq!(config.table_uri, "s3://my-bucket/my-table");
        assert_eq!(config.write_mode, WriteMode::Append);
        assert_eq!(config.storage_options.len(), 2);
        assert!(config.create_if_not_exists);
        assert_eq!(config.partition_columns, vec!["year", "month"]);
        assert_eq!(config.max_retries, 5);
        assert_eq!(config.retry_delay_ms, 2000);
    }

    #[test]
    fn test_config_defaults() {
        let minimal_config_json = json!({
            "table_uri": "s3://my-bucket/my-table"
        });

        let config: DeltaLakeOutputConfig = serde_json::from_value(minimal_config_json).unwrap();
        assert_eq!(config.table_uri, "s3://my-bucket/my-table");
        assert_eq!(config.write_mode, WriteMode::Append);
        assert!(config.storage_options.is_empty());
        assert!(config.create_if_not_exists);
        assert!(config.partition_columns.is_empty());
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.retry_delay_ms, 1000);
    }

    #[test]
    fn test_write_mode_serialization() {
        assert_eq!(serde_json::to_string(&WriteMode::Append).unwrap(), "\"append\"");
        assert_eq!(serde_json::to_string(&WriteMode::Overwrite).unwrap(), "\"overwrite\"");
        assert_eq!(serde_json::to_string(&WriteMode::Error).unwrap(), "\"error\"");
        assert_eq!(serde_json::to_string(&WriteMode::Ignore).unwrap(), "\"ignore\"");
    }

    #[test]
    fn test_config_validation() {
        // Valid config
        let valid_config = DeltaLakeOutputConfig {
            table_uri: "s3://my-bucket/my-table".to_string(),
            write_mode: WriteMode::Append,
            storage_options: HashMap::new(),
            create_if_not_exists: true,
            schema: None,
            partition_columns: vec![],
            max_retries: 3,
            retry_delay_ms: 1000,
        };
        assert!(DeltaLakeOutput::new(valid_config).is_ok());

        // Invalid config - empty table_uri
        let invalid_config = DeltaLakeOutputConfig {
            table_uri: "".to_string(),
            write_mode: WriteMode::Append,
            storage_options: HashMap::new(),
            create_if_not_exists: true,
            schema: None,
            partition_columns: vec![],
            max_retries: 3,
            retry_delay_ms: 1000,
        };
        assert!(DeltaLakeOutput::new(invalid_config).is_err());

        // Invalid config - whitespace only table_uri
        let whitespace_config = DeltaLakeOutputConfig {
            table_uri: "   ".to_string(),
            write_mode: WriteMode::Append,
            storage_options: HashMap::new(),
            create_if_not_exists: true,
            schema: None,
            partition_columns: vec![],
            max_retries: 3,
            retry_delay_ms: 1000,
        };
        assert!(DeltaLakeOutput::new(whitespace_config).is_err());
    }
}
