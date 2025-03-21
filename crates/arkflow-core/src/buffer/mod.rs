use crate::input::Ack;
use crate::{Error, MessageBatch};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

lazy_static::lazy_static! {
    static ref BUFFER_BUILDERS: RwLock<HashMap<String, Arc<dyn BufferBuilder>>> = RwLock::new(HashMap::new());
}

#[async_trait]
pub trait Buffer: Send + Sync {
    async fn write(&self, msg: MessageBatch, arc: Arc<dyn Ack>) -> Result<(), Error>;

    async fn read(&self) -> Result<Option<(MessageBatch, Arc<dyn Ack>)>, Error>;

    async fn close(&self) -> Result<(), Error>;
}

/// Buffer builder
pub trait BufferBuilder: Send + Sync {
    fn build(&self, config: &Option<serde_json::Value>) -> Result<Arc<dyn Buffer>, Error>;
}

/// Buffer configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BufferConfig {
    #[serde(rename = "type")]
    pub buffer_type: String,
    #[serde(flatten)]
    pub config: Option<serde_json::Value>,
}

impl BufferConfig {
    /// Building buffer components
    pub fn build(&self) -> Result<Arc<dyn Buffer>, Error> {
        let builders = BUFFER_BUILDERS.read().unwrap();

        if let Some(builder) = builders.get(&self.buffer_type) {
            builder.build(&self.config)
        } else {
            Err(Error::Config(format!(
                "Unknown buffer type: {}",
                self.buffer_type
            )))
        }
    }
}

pub fn register_buffer_builder(type_name: &str, builder: Arc<dyn BufferBuilder>) {
    let mut builders = BUFFER_BUILDERS.write().unwrap();
    if builders.contains_key(type_name) {
        panic!("Buffer type already registered: {}", type_name)
    }
    builders.insert(type_name.to_string(), builder);
}

pub fn get_registered_buffer_types() -> Vec<String> {
    let builders = BUFFER_BUILDERS.read().unwrap();
    builders.keys().cloned().collect()
}
