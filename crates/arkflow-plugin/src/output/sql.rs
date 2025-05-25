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
use arkflow_core::output::{register_output_builder, Output, OutputBuilder};
use arkflow_core::{Error, MessageBatch, Resource};

use async_trait::async_trait;
use datafusion::arrow::array::{
    Array, BooleanArray, Float64Array, Int64Array, StringArray, UInt64Array,
};
use datafusion::arrow::datatypes::DataType;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use sqlx::mysql::{MySqlConnectOptions, MySqlSslMode};
use sqlx::postgres::{PgConnectOptions, PgSslMode};
use sqlx::{Connection, MySqlConnection, PgConnection, SqliteConnection, QueryBuilder};

#[derive(Debug, Clone)]
pub enum SqlValue {
    String(String),
    Int64(i64),
    UInt64(u64),
    Float64(f64),
    Boolean(bool),
    Null,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DatabaseType {
    Mysql,
    Postgres,
    Sqlite,
}
pub enum DatabaseConnection {
    Mysql(MySqlConnection),
    Postgres(PgConnection),
    Sqlite(SqliteConnection),
}
impl DatabaseConnection {
    /// Executes an INSERT query with the given columns and rows
    /// Handles type conversion and proper escaping for different database types
    /// Returns a Result indicating success or detailed error information
    pub async fn execute_insert(
        &mut self,
        output_config: &SqlOutputConfig,
        columns: Vec<String>,
        rows: Vec<Vec<SqlValue>>,
    ) -> Result<(), Error> {
        match self {
            DatabaseConnection::Mysql(conn) => {
                let mut query_builder = QueryBuilder::<sqlx::MySql>::new(format!(
                    "INSERT INTO {} ({})",
                    output_config.table_name,
                    columns
                        .iter()
                        .map(|c| format!("`{}`", c))
                        .collect::<Vec<_>>()
                        .join(", "),
                ));

                query_builder.push_values(rows, |mut b, row| {
                    for value in row {
                        match value {
                            SqlValue::String(s) => b.push_bind(s),
                            SqlValue::Int64(i) => b.push_bind(i),
                            SqlValue::UInt64(u) => b.push_bind(u),
                            SqlValue::Float64(f) => b.push_bind(f),
                            SqlValue::Boolean(bool) => b.push_bind(bool),
                            SqlValue::Null => b.push_bind(None::<String>),
                        };
                    }
                });

                let query = query_builder.build();
                query
                    .execute(conn)
                    .await
                    .map_err(|e| Error::Process(format!("Failed to execute MySQL query: {}", e)))?;

                Ok(())
            }
            DatabaseConnection::Postgres(conn) => {
                let mut query_builder = QueryBuilder::<sqlx::Postgres>::new(format!(
                    "INSERT INTO {} ({})",
                    output_config.table_name,
                    columns
                        .iter()
                        .map(|c| format!("\"{}\"", c))
                        .collect::<Vec<_>>()
                        .join(", "),
                ));
                query_builder.push_values(rows, |mut b, row| {
                    for value in row {
                        match value {
                            SqlValue::String(s) => b.push_bind(s),
                            SqlValue::Int64(i) => b.push_bind(i),
                            SqlValue::UInt64(u) => b.push_bind(u as i64),
                            SqlValue::Float64(f) => b.push_bind(f),
                            SqlValue::Boolean(bool) => b.push_bind(bool),
                            SqlValue::Null => b.push_bind(None::<String>),
                        };
                    }
                });

                let query = query_builder.build();
                query.execute(conn).await.map_err(|e| {
                    Error::Process(format!("Failed to execute PostgresSQL query: {}", e))
                })?;

                Ok(())
            }
            DatabaseConnection::Sqlite(conn) => {
                let mut query_builder = QueryBuilder::<sqlx::Sqlite>::new(format!(
                    "INSERT INTO {} ({})",
                    output_config.table_name,
                    columns
                        .iter()
                        .map(|c| format!("\"{}\"", c))
                        .collect::<Vec<_>>()
                        .join(", "),
                ));
                query_builder.push_values(rows, |mut b, row| {
                    for value in row {
                        match value {
                            SqlValue::String(s) => b.push_bind(s),
                            SqlValue::Int64(i) => b.push_bind(i),
                            SqlValue::UInt64(u) => b.push_bind(u as i64),
                            SqlValue::Float64(f) => b.push_bind(f),
                            SqlValue::Boolean(bool) => b.push_bind(bool),
                            SqlValue::Null => b.push_bind(None::<String>),
                        };
                    }
                });

                let query = query_builder.build();
                query.execute(conn).await.map_err(|e| {
                    Error::Process(format!("Failed to execute SQLite query: {}", e))
                })?;

                Ok(())
            }
        }
    }
}

/// Configuration for SQL output
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct SqlOutputConfig {
    /// SQL query statement
    database_type: DatabaseType,
    table_name: String,
    uri: String,
    ssl: Option<SslConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SslConfig {
    ssl_mode: String,
    root_cert: Option<String>,
    client_cert: Option<String>,
    client_key: Option<String>,
}
impl SslConfig {
    pub async fn generate_mysql_ssl_opts(
        &self,
        output_config: &SqlOutputConfig,
    ) -> Result<MySqlConnectOptions, Error> {
        let ssl_mode = match self.ssl_mode.to_lowercase().as_str() {
            "disable" => MySqlSslMode::Disabled,
            "prefer" => MySqlSslMode::Preferred,
            "require" => MySqlSslMode::Required,
            "verify_ca" => MySqlSslMode::VerifyCa,
            "verify_full" => MySqlSslMode::VerifyIdentity,
            _ => return Err(Error::Config("Invalid SSL mode".to_string())),
        };
        let mut opts = MySqlConnectOptions::from_str(&output_config.uri)
            .map_err(|e| Error::Config(format!("Invalid MySQL URI: {}", e)))?;
        opts = opts.ssl_mode(ssl_mode);

        if let Some(root_cert) = &self.root_cert {
            opts = opts.ssl_ca(Path::new(root_cert));
        }

        if let Some(client_cert) = &self.client_cert {
            if let Some(client_key) = &self.client_key {
                opts = opts.ssl_client_cert(Path::new(client_cert));
                opts = opts.ssl_client_key(Path::new(client_key));
            } else {
                warn!("Client certificate provided without private key - will be ignored");
            }
        } else if self.client_key.is_some() {
            warn!("Client key provided without certificate - will be ignored");
        }
        Ok(opts)
    }
    async fn generate_postgres_ssl_opts(
        &self,
        output_config: &SqlOutputConfig,
    ) -> Result<PgConnectOptions, Error> {
        let ssl_mode = match self.ssl_mode.to_lowercase().as_str() {
            "disable" => PgSslMode::Disable,
            "prefer" => PgSslMode::Prefer,
            "require" => PgSslMode::Require,
            "verify_ca" => PgSslMode::VerifyCa,
            "verify_full" => PgSslMode::VerifyFull,
            _ => return Err(Error::Config("Invalid SSL mode".to_string())),
        };
        let mut opts = PgConnectOptions::from_str(&output_config.uri)
            .map_err(|e| Error::Config(format!("Invalid PostgreSQL URI: {}", e)))?;
        opts = opts.ssl_mode(ssl_mode);

        if let Some(root_cert) = &self.root_cert {
            opts = opts.ssl_root_cert(Path::new(root_cert));
        }

        if let Some(client_cert) = &self.client_cert {
            if let Some(client_key) = &self.client_key {
                opts = opts.ssl_client_cert(Path::new(client_cert));
                opts = opts.ssl_client_key(Path::new(client_key));
            } else {
                warn!("Client certificate provided without private key - will be ignored");
            }
        } else if self.client_key.is_some() {
            warn!("Client key provided without certificate - will be ignored");
        }
        Ok(opts)
    }
}

pub struct SqlOutput {
    sql_config: SqlOutputConfig,
    connected: std::sync::atomic::AtomicBool,
    conn_lock: Arc<Mutex<Option<DatabaseConnection>>>,
    cancellation_token: CancellationToken,
}

impl SqlOutput {
    pub fn new(sql_config: SqlOutputConfig) -> Result<Self, Error> {
        let cancellation_token = CancellationToken::new();

        Ok(Self {
            sql_config,
            connected: std::sync::atomic::AtomicBool::new(false),
            conn_lock: Arc::new(Mutex::new(None)),
            cancellation_token,
        })
    }
}
#[async_trait]
impl Output for SqlOutput {
    async fn connect(&self) -> Result<(), Error> {
        if self.connected.load(std::sync::atomic::Ordering::SeqCst) {
            return Ok(());
        }

        let conn = self.init_connect().await?;
        let mut conn_guard = self.conn_lock.lock().await;
        *conn_guard = Some(conn);
        self.connected
            .store(true, std::sync::atomic::Ordering::SeqCst);

        Ok(())
    }
    async fn write(&self, msg: MessageBatch) -> Result<(), Error> {
        let mut conn_guard = self.conn_lock.lock().await;
        let conn = conn_guard
            .as_mut()
            .ok_or_else(|| Error::Process("Database connection is not initialized".to_string()))?;

        self.insert_row(conn, &msg).await?;
        Ok(())
    }

    async fn close(&self) -> Result<(), Error> {
        self.cancellation_token.cancel();
        Ok(())
    }
}
impl SqlOutput {
    /// Initialize a new DB connection.  
    /// If `ssl` is configured, apply root certificates to the SSL options.
    async fn init_connect(&self) -> Result<DatabaseConnection, Error> {
        let conn = match self.sql_config.database_type {
            DatabaseType::Mysql => self.generate_mysql_conn().await?,
            DatabaseType::Postgres => self.generate_postgres_conn().await?,
            DatabaseType::Sqlite => self.generate_sqlite_conn().await?,
        };
        Ok(conn)
    }
    /// Processes a batch of Arrow data and inserts it into the database
    /// 1. Extracts schema and column names
    /// 2. Converts each row to SQL-compatible values
    /// 3. Executes the insert query with proper batching
    async fn insert_row(
        &self,
        conn: &mut DatabaseConnection,
        msg: &MessageBatch,
    ) -> Result<(), Error> {
        let schema = msg.schema();
        let num_rows = msg.len();
        let num_columns = schema.fields().len();
        let columns: Vec<String> = (0..num_columns)
            .map(|i| schema.field(i).name().clone())
            .collect();

        let mut rows = Vec::with_capacity(num_columns * num_rows);
        for row_index in 0..num_rows {
            for col_index in 0..num_columns {
                let column = msg.column(col_index);

                let value = self.matching_data_type(column, row_index).await?;
                rows.push(value);
            }
        }
        let rows: Vec<Vec<SqlValue>> = rows
            .chunks(num_columns)
            .map(|chunk| chunk.to_vec())
            .collect();

        conn.execute_insert(&self.sql_config, columns, rows).await?;
        Ok(())
    }
    // Convert Arrow data types to SQL-compatible string representation
    async fn matching_data_type(
        &self,
        column: &dyn Array,
        row_index: usize,
    ) -> Result<SqlValue, Error> {
        // Determine the data type of the column and convert to appropriate SQL format
        let column_type = column.data_type();
        match column_type {
            DataType::Utf8 => {
                let utf8_array = column.as_any().downcast_ref::<StringArray>().unwrap();
                if utf8_array.is_null(row_index) {
                    Ok(SqlValue::Null)
                } else {
                    Ok(SqlValue::String(utf8_array.value(row_index).to_string()))
                }
            }
            DataType::Int64 => {
                let int_array = column.as_any().downcast_ref::<Int64Array>().unwrap();
                if int_array.is_null(row_index) {
                    Ok(SqlValue::Null)
                } else {
                    Ok(SqlValue::Int64(int_array.value(row_index)))
                }
            }
            DataType::UInt64 => {
                let uint_array = column.as_any().downcast_ref::<UInt64Array>().unwrap();
                if uint_array.is_null(row_index) {
                    Ok(SqlValue::Null)
                } else {
                    Ok(SqlValue::UInt64(uint_array.value(row_index)))
                }
            }
            DataType::Float64 => {
                let float_array = column.as_any().downcast_ref::<Float64Array>().unwrap();
                if float_array.is_null(row_index) {
                    Ok(SqlValue::Null)
                } else {
                    Ok(SqlValue::Float64(float_array.value(row_index)))
                }
            }
            DataType::Boolean => {
                let bool_array = column.as_any().downcast_ref::<BooleanArray>().unwrap();
                if bool_array.is_null(row_index) {
                    Ok(SqlValue::Null)
                } else {
                    Ok(SqlValue::Boolean(bool_array.value(row_index)))
                }
            }
            _ => Err(Error::Process(format!(
                "Unsupported data type: {:?}",
                column_type
            ))),
        }
    }
    /// Generates MySQL SSL connection options based on configuration
    /// Validates SSL mode and sets up certificates if provided
    async fn generate_mysql_conn(&self) -> Result<DatabaseConnection, Error> {
        let mysql_conn = if let Some(ssl) = &self.sql_config.ssl {
            let opts = ssl.generate_mysql_ssl_opts(&self.sql_config).await?;
            MySqlConnection::connect_with(&opts)
                .await
                .map_err(|e| Error::Config(format!("Failed to connect to MySQL with SSL: {}", e)))?
        } else {
            MySqlConnection::connect(&self.sql_config.uri)
                .await
                .map_err(|e| Error::Config(format!("Failed to connect to MySQL: {}", e)))?
        };  
        Ok(DatabaseConnection::Mysql(mysql_conn))
    }
    async fn generate_postgres_conn(&self) -> Result<DatabaseConnection, Error> {
        let postgres_conn = if let Some(ssl) = &self.sql_config.ssl {
            let opts = ssl.generate_postgres_ssl_opts(&self.sql_config).await?;
            PgConnection::connect_with(&opts).await.map_err(|e| {
                Error::Config(format!("Failed to connect to PostgreSQL with SSL: {}", e))
            })?
        } else {
            PgConnection::connect(&self.sql_config.uri)
                .await
                .map_err(|e| Error::Config(format!("Failed to connect to PostgreSQL: {}", e)))?
        };
        Ok(DatabaseConnection::Postgres(postgres_conn))
    }
    async fn generate_sqlite_conn(&self) -> Result<DatabaseConnection, Error> {
        let sqlite_conn = SqliteConnection::connect(&self.sql_config.uri)
            .await
            .map_err(|e| Error::Config(format!("Failed to connect to SQLite: {}", e)))?;
        Ok(DatabaseConnection::Sqlite(sqlite_conn))
    }

}

pub(crate) struct SqlOutputBuilder;
impl OutputBuilder for SqlOutputBuilder {
    fn build(
        &self,
        _name: Option<&String>,
        config: &Option<serde_json::Value>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Output>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "SQL output configuration is missing".to_string(),
            ));
        }

        let config: SqlOutputConfig = serde_json::from_value(config.clone().unwrap())?;
        Ok(Arc::new(SqlOutput::new(config)?))
    }
}
pub fn init() -> Result<(), Error> {
    register_output_builder("sql", Arc::new(SqlOutputBuilder))
}
