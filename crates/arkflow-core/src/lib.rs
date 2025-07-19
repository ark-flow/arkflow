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

//! Rust stream processing engine

use crate::temporary::Temporary;
use datafusion::arrow::array::{Array, ArrayRef, BinaryArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::data_type::AsBytes;
use serde::Serialize;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use thiserror::Error;

pub mod buffer;
pub mod cli;
pub mod codec;
pub mod config;
pub mod engine;
pub mod input;
pub mod output;
pub mod pipeline;
pub mod processor;
pub mod remote_config;
pub mod stream;
pub mod temporary;

pub const DEFAULT_BINARY_VALUE_FIELD: &str = "__value__";
pub const DEFAULT_RECORD_BATCH: usize = 8192;

/// Error in the stream processing engine
#[derive(Error, Debug)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Read error: {0}")]
    Read(String),

    #[error("Process errors: {0}")]
    Process(String),

    #[error("Connection error: {0}")]
    Connection(String),

    /// Reconnection should be attempted after a connection loss.
    #[error("Connection lost")]
    Disconnection,

    #[error("Timeout error")]
    Timeout,

    #[error("Unknown error: {0}")]
    Unknown(String),

    #[error("EOF")]
    EOF,
}

#[derive(Clone)]
pub struct Resource {
    pub temporary: HashMap<String, Arc<dyn Temporary>>,
    pub input_names: RefCell<Vec<String>>,
}

pub type Bytes = Vec<u8>;

/// Represents a message in a stream processing engine.
#[derive(Clone, Debug)]
pub struct MessageBatch {
    record_batch: RecordBatch,
    input_name: Option<String>,
}

impl MessageBatch {
    pub fn new_binary(content: Vec<Bytes>) -> Result<Self, Error> {
        Self::new_binary_with_field_name(content, None)
    }
    pub fn new_binary_with_field_name(
        content: Vec<Bytes>,
        field_name: Option<&str>,
    ) -> Result<Self, Error> {
        let fields = vec![Field::new(
            field_name.unwrap_or(DEFAULT_BINARY_VALUE_FIELD),
            DataType::Binary,
            false,
        )];
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(content.len());

        let bytes: Vec<_> = content.iter().map(|x| x.as_bytes()).collect();

        let array = BinaryArray::from_vec(bytes);
        columns.push(Arc::new(array));

        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema, columns)
            .map_err(|e| Error::Process(format!("Creating an Arrow record batch failed: {}", e)))?;

        Ok(Self {
            record_batch: batch,
            input_name: None,
        })
    }

    pub fn set_input_name(&mut self, input_name: Option<String>) {
        self.input_name = input_name;
    }

    pub fn get_input_name(&self) -> Option<String> {
        self.input_name.clone()
    }

    pub fn new_binary_with_origin(&self, content: Vec<Bytes>) -> Result<Self, Error> {
        let schema = self.schema();
        let mut fields: Vec<Arc<Field>> = schema.fields().iter().cloned().collect();

        fields.push(Arc::new(Field::new(
            DEFAULT_BINARY_VALUE_FIELD,
            DataType::Binary,
            false,
        )));
        let new_schema = Arc::new(Schema::new(fields));

        let mut columns: Vec<ArrayRef> = Vec::new();
        for i in 0..schema.fields().len() {
            columns.push(self.column(i).clone());
        }

        let binary_data: Vec<&[u8]> = content.iter().map(|v| v.as_slice()).collect();
        columns.push(Arc::new(BinaryArray::from(binary_data)));

        let new_msg = RecordBatch::try_new(new_schema, columns)
            .map_err(|e| Error::Process(format!("Creating an Arrow record batch failed: {}", e)))?;
        Ok(MessageBatch::new_arrow(new_msg))
    }

    pub fn filter_columns(
        &self,
        field_names_to_include: &HashSet<String>,
    ) -> Result<MessageBatch, Error> {
        let schema = self.schema();

        let cap = field_names_to_include.len();
        let mut new_columns = Vec::with_capacity(cap);
        let mut fields = Vec::with_capacity(cap);

        for (i, col) in self.columns().iter().enumerate() {
            let field = schema.field(i);
            let name = field.name();

            if field_names_to_include.contains(name.as_str()) {
                new_columns.push(col.clone());
                fields.push(field.clone());
            }
        }

        let new_schema: SchemaRef = SchemaRef::new(Schema::new(fields));
        let batch = RecordBatch::try_new(new_schema, new_columns)
            .map_err(|e| Error::Process(format!("Creating an Arrow record batch failed: {}", e)))?;
        Ok(batch.into())
    }

    pub fn from_json<T: Serialize>(value: &T) -> Result<Self, Error> {
        let content = serde_json::to_vec(value)?;
        Ok(Self::new_binary(vec![content])?)
    }

    pub fn new_arrow(content: RecordBatch) -> Self {
        Self {
            record_batch: content,
            input_name: None,
        }
    }

    /// Create a message from a string.
    pub fn from_string(content: &str) -> Result<Self, Error> {
        Self::new_binary(vec![content.as_bytes().to_vec()])
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> usize {
        self.record_batch.num_rows()
    }

    pub fn to_binary(&self, name: &str) -> Result<Vec<&[u8]>, Error> {
        let Some(array_ref) = self.record_batch.column_by_name(name) else {
            return Err(Error::Process("not found column".to_string()));
        };

        let data = array_ref.to_data();

        if *data.data_type() != DataType::Binary {
            return Err(Error::Process("not support data type".to_string()));
        }

        let Some(v) = array_ref.as_any().downcast_ref::<BinaryArray>() else {
            return Err(Error::Process("not support data type".to_string()));
        };
        let mut vec_bytes = Vec::with_capacity(v.len());
        for x in v {
            if let Some(data) = x {
                vec_bytes.push(data)
            }
        }
        Ok(vec_bytes)
    }
}

impl Deref for MessageBatch {
    type Target = RecordBatch;

    fn deref(&self) -> &Self::Target {
        &self.record_batch
    }
}

impl From<RecordBatch> for MessageBatch {
    fn from(batch: RecordBatch) -> Self {
        Self {
            record_batch: batch,
            input_name: None,
        }
    }
}

impl From<MessageBatch> for RecordBatch {
    fn from(batch: MessageBatch) -> Self {
        batch.record_batch
    }
}

impl TryFrom<Vec<Bytes>> for MessageBatch {
    type Error = Error;

    fn try_from(value: Vec<Bytes>) -> Result<Self, Self::Error> {
        Self::new_binary(value)
    }
}

impl TryFrom<Vec<String>> for MessageBatch {
    type Error = Error;

    fn try_from(value: Vec<String>) -> Result<Self, Self::Error> {
        Self::new_binary(value.into_iter().map(|s| s.into_bytes()).collect())
    }
}

impl TryFrom<Vec<&str>> for MessageBatch {
    type Error = Error;

    fn try_from(value: Vec<&str>) -> Result<Self, Self::Error> {
        Self::new_binary(value.into_iter().map(|s| s.as_bytes().to_vec()).collect())
    }
}

impl DerefMut for MessageBatch {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.record_batch
    }
}

pub fn split_batch(batch_to_split: RecordBatch, size: usize) -> Vec<RecordBatch> {
    let size = size.max(1);
    let total_rows = batch_to_split.num_rows();
    if total_rows <= DEFAULT_RECORD_BATCH {
        return vec![batch_to_split];
    }

    let (chunk_size, capacity) = if size * DEFAULT_RECORD_BATCH < total_rows {
        (total_rows.div_ceil(size), size)
    } else {
        (
            DEFAULT_RECORD_BATCH,
            total_rows.div_ceil(DEFAULT_RECORD_BATCH),
        )
    };

    let mut chunks = Vec::with_capacity(capacity);
    let mut offset = 0;
    while offset < total_rows {
        let length = std::cmp::min(chunk_size, total_rows - offset);
        let slice = batch_to_split.slice(offset, length);
        chunks.push(slice);
        offset += length;
    }

    chunks
}
