use crate::time::deserialize_duration;
use arkflow_core::buffer::{register_buffer_builder, Buffer, BufferBuilder};
use arkflow_core::input::Ack;
use arkflow_core::{Error, MessageBatch};
use async_trait::async_trait;
use datafusion::arrow;
use datafusion::arrow::array::RecordBatch;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time;
use tokio::sync::{Notify, RwLock};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::info;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryBufferConfig {
    capacity: u32,
    #[serde(deserialize_with = "deserialize_duration")]
    timeout: time::Duration,
}

pub struct MemoryBuffer {
    config: MemoryBufferConfig,
    queue: Arc<RwLock<VecDeque<(MessageBatch, Arc<dyn Ack>)>>>,
    notify: Arc<Notify>,
    close: CancellationToken,
}

impl MemoryBuffer {
    fn new(config: MemoryBufferConfig) -> Result<Self, Error> {
        let notify = Arc::new(Notify::new());
        let notify_clone = notify.clone();
        let duration = config.timeout.clone();
        let close = CancellationToken::new();
        let close_clone = close.clone();

        tokio::spawn(async move {
            loop {
                let timer = sleep(duration);
                tokio::select! {
                    _ = timer => {
                        // notify read
                        info!("time");

                        notify_clone.notify_waiters();
                    }
                    _ = close_clone.cancelled() => {
                         // notify read
                        info!("cancelled");
                        notify_clone.notify_waiters();
                        break;
                    }
                    _ = notify_clone.notified() => {
                        info!("reset timer");
                    }
                }
            }
        });
        Ok(Self {
            close,
            notify,
            config,
            queue: Arc::new(Default::default()),
        })
    }

    async fn process_messages(&self) -> Result<Option<(MessageBatch, Arc<dyn Ack>)>, Error> {
        let queue_arc = self.queue.clone();
        let mut queue_lock = queue_arc.write().await;

        if queue_lock.is_empty() {
            return Ok(None);
        }

        let mut messages = Vec::new();
        let mut acks = Vec::new();

        while let Some((msg, ack)) = queue_lock.pop_back() {
            messages.push(msg);
            acks.push(ack);
        }

        // 如果没有消息，返回 None
        if messages.is_empty() {
            return Ok(None);
        }
        let schema = messages[0].schema();
        let x: Vec<RecordBatch> = messages.iter().map(|batch| batch.clone().into()).collect();
        let new_batch = arrow::compute::concat_batches(&schema, &x)
            .map_err(|e| Error::Process(format!("Merge batches failed: {}", e)))?;

        let new_ack = Arc::new(ArrayAck(acks));
        Ok(Some((MessageBatch::new_arrow(new_batch), new_ack)))
    }
}

#[async_trait]
impl Buffer for MemoryBuffer {
    async fn write(&self, msg: MessageBatch, arc: Arc<dyn Ack>) -> Result<(), Error> {
        let queue_arc = self.queue.clone();
        {
            let queue_lock = queue_arc.read().await;
            let len = queue_lock.len();

            if len >= self.config.capacity as usize - 1 {
                let notify = self.notify.clone();
                notify.notify_waiters();
            }
        }

        let mut queue_lock = queue_arc.write().await;

        queue_lock.push_front((msg, arc));

        Ok(())
    }

    async fn read(&self) -> Result<Option<(MessageBatch, Arc<dyn Ack>)>, Error> {
        let notify = self.notify.clone();
        notify.notified().await;

        self.process_messages().await
    }

    async fn flush(&self) -> Result<(), Error> {
        let notify = self.notify.clone();
        notify.notify_one();
        Ok(())
    }

    async fn close(&self) -> Result<(), Error> {
        // if let Some(queue) = self.queue.try_read() {
        //     if !queue.is_empty() {
        //         let notify = self.notify.clone();
        //         notify.notify_one();
        //     }
        // }
        self.close.cancel();

        Ok(())
    }
}
struct ArrayAck(Vec<Arc<dyn Ack>>);
#[async_trait]
impl Ack for ArrayAck {
    async fn ack(&self) {
        for ack in self.0.iter() {
            ack.ack().await;
        }
    }
}

struct MemoryBufferBuilder;

impl BufferBuilder for MemoryBufferBuilder {
    fn build(&self, config: &Option<Value>) -> Result<Arc<dyn Buffer>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "Memory buffer configuration is missing".to_string(),
            ));
        }

        let config: MemoryBufferConfig = serde_json::from_value(config.clone().unwrap())?;
        Ok(Arc::new(MemoryBuffer::new(config)?))
    }
}

pub fn init() {
    register_buffer_builder("memory", Arc::new(MemoryBufferBuilder))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arkflow_core::input::NoopAck;

    #[tokio::test]
    async fn test_memory_buffer_new() {
        let p = MemoryBuffer::new(MemoryBufferConfig {
            capacity: 10,
            timeout: time::Duration::from_secs(10),
        })
        .unwrap();

        let x = p
            .write(
                MessageBatch::new_binary(vec!["test".as_bytes().to_vec()]).unwrap(),
                Arc::new(NoopAck),
            )
            .await;
    }
}
