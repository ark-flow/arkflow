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

use arkflow_core::input::{Ack, Input, InputBuilder, NoopAck};
use arkflow_core::{input, Error, MessageBatch, Resource};
use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, BooleanArray, ListArray, RecordBatch, UInt16Array};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio_modbus::prelude::{tcp, Reader};
use tokio_modbus::{Address, Quantity};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ModbusInputConfig {
    addr: String,
    points: Vec<Point>,
    read_interval: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum PointType {
    Coils,
    DiscreteInputs,
    HoldingRegisters,
    InputRegisters,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Point {
    #[serde(rename = "type")]
    point_type: PointType,
    name: String,
    address: Address,
    quantity: Quantity,
}

struct ModbusInput {
    config: ModbusInputConfig,
    name: Option<String>,
    first_read: AtomicBool,
    client: Arc<Mutex<Option<tokio_modbus::client::Context>>>,
}

impl ModbusInput {
    fn new(config: ModbusInputConfig, name: Option<String>) -> Self {
        Self {
            config,
            first_read: AtomicBool::new(false),
            client: Arc::new(Mutex::new(None)),
            name,
        }
    }
}

#[async_trait]
impl Input for ModbusInput {
    async fn connect(&self) -> Result<(), Error> {
        let mut cli_lock = self.client.lock().await;
        let socket_addr = self
            .config
            .addr
            .parse()
            .map_err(|_| Error::Process("Failed to parse socket address".to_string()))?;

        let ctx = tcp::connect(socket_addr).await?;
        cli_lock.replace(ctx);
        Ok(())
    }

    async fn read(&self) -> Result<(MessageBatch, Arc<dyn Ack>), Error> {
        let mut ctx = self.client.lock().await;
        let Some(ctx) = ctx.as_mut() else {
            return Err(Error::Disconnection);
        };

        if self
            .first_read
            .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            tokio::time::sleep(self.config.read_interval).await;
        }

        let mut fields = Vec::with_capacity(self.config.points.len());
        let mut array: Vec<ArrayRef> = Vec::with_capacity(self.config.points.len());
        for x in &self.config.points {
            match x.point_type {
                PointType::Coils => {
                    let result = ctx
                        .read_coils(x.address, x.quantity)
                        .await
                        .map_err(|e| Error::Process(format!("Failed to read coils:{}", e)))?
                        .map_err(|e| Error::Process(format!("Failed to read coils code:{}", e)))?;

                    let (field, list_array) = Self::new_bool_list_array(&x.name, result)?;
                    fields.push(field);
                    array.push(list_array);
                }
                PointType::DiscreteInputs => {
                    let result = ctx
                        .read_discrete_inputs(x.address, x.quantity)
                        .await
                        .map_err(|e| {
                            Error::Process(format!("Failed to read discrete inputs:{}", e))
                        })?
                        .map_err(|e| {
                            Error::Process(format!("Failed to read discrete inputs code:{}", e))
                        })?;
                    let (field, list_array) = Self::new_bool_list_array(&x.name, result)?;
                    fields.push(field);
                    array.push(list_array);
                }
                PointType::HoldingRegisters => {
                    let result = ctx
                        .read_holding_registers(x.address, x.quantity)
                        .await
                        .map_err(|e| {
                            Error::Process(format!("Failed to read holding registers:{}", e))
                        })?
                        .map_err(|e| {
                            Error::Process(format!("Failed to read holding registers code:{}", e))
                        })?;

                    let (field, list_array) = Self::new_u16_list_array(&x.name, result)?;
                    fields.push(field);
                    array.push(list_array);
                }
                PointType::InputRegisters => {
                    let result = ctx
                        .read_input_registers(x.address, x.quantity)
                        .await
                        .map_err(|e| {
                            Error::Process(format!("Failed to read input registers:{}", e))
                        })?
                        .map_err(|e| {
                            Error::Process(format!("Failed to read input registers code:{}", e))
                        })?;

                    let (field, list_array) = Self::new_u16_list_array(&x.name, result)?;
                    fields.push(field);
                    array.push(list_array);
                }
            }
        }
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), array)
            .map_err(|e| Error::Process(format!("Failed to create record batch:{}", e)))?;
        let mut msg: MessageBatch = batch.into();
        msg.set_input_name(self.name.clone());
        Ok((msg, Arc::new(NoopAck)))
    }

    async fn close(&self) -> Result<(), Error> {
        let mut cli_lock = self.client.lock().await;
        let _ = cli_lock.take();
        Ok(())
    }
}

macro_rules! impl_list_array {
    ($name:ident, $data_type:expr, $array_type:ty, $rust_type:ty) => {
        fn $name(name: &str, data: Vec<$rust_type>) -> Result<(Field, ArrayRef), Error> {
            let field = Field::new(
                name,
                DataType::List(Arc::new(Field::new_list_field($data_type, false))),
                false,
            );
            let list_array = ListArray::try_new(
                Arc::new(Field::new_list_field($data_type, false)),
                Self::new_offset_buffer(data.len()),
                Arc::new(<$array_type>::from(data)),
                None,
            )
            .map_err(|e| Error::Process(format!("Failed to create list array:{}", e)))?;
            Ok((field, Arc::new(list_array)))
        }
    };
}

impl ModbusInput {
    fn new_offset_buffer(n: usize) -> OffsetBuffer<i32> {
        OffsetBuffer::<i32>::from_lengths([n])
    }

    impl_list_array!(new_bool_list_array, DataType::Boolean, BooleanArray, bool);
    impl_list_array!(new_u16_list_array, DataType::UInt16, UInt16Array, u16);
}

struct ModbusInputBuilder;

impl InputBuilder for ModbusInputBuilder {
    fn build(
        &self,
        name: Option<&String>,
        config: &Option<Value>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Input>, Error> {
        let config = config
            .as_ref()
            .ok_or(Error::Process("Modbus input config is missing".to_string()))?;
        let config: ModbusInputConfig = serde_json::from_value(config.clone())
            .map_err(|e| Error::Process(format!("Failed to parse modbus input config:{}", e)))?;
        Ok(Arc::new(ModbusInput::new(config, name.cloned())))
    }
}

pub fn init() -> Result<(), Error> {
    input::register_input_builder("modbus", Arc::new(ModbusInputBuilder))?;
    Ok(())
}
