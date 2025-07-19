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

use std::sync::Arc;

use arkflow_core::{
    processor::{register_processor_builder, Processor, ProcessorBuilder},
    Error, MessageBatch, Resource,
};
use async_trait::async_trait;
use base64::Engine as Base64Engine;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use wasmtime::{Engine, Instance, Memory, Module, Store};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WasmProcessorConfig {
    /// WASM module path or inline WASM bytes (base64 encoded)
    module: String,
    /// Function name to call in the WASM module
    function: String,
}

struct WasmProcessor {
    config: WasmProcessorConfig,
    engine: Engine,
    module: Module,
}

#[async_trait]
impl Processor for WasmProcessor {
    async fn process(&self, msg_batch: MessageBatch) -> Result<Vec<MessageBatch>, Error> {
        // Create a new store for this execution
        let mut store = Store::new(&self.engine, ());

        // Instantiate the module
        let instance = Instance::new(&mut store, &self.module, &[])
            .map_err(|e| Error::Process(format!("Failed to instantiate WASM module: {}", e)))?;

        // Get the function to call
        let func = instance
            .get_typed_func::<(i32, i32), i32>(&mut store, &self.config.function)
            .map_err(|e| {
                Error::Process(format!(
                    "Failed to get function '{}': {}",
                    self.config.function, e
                ))
            })?;

        // Convert message batch to JSON bytes for processing
        let input_data = self.message_batch_to_bytes(&msg_batch)?;

        // Allocate memory in WASM for input data
        let memory = instance
            .get_memory(&mut store, "memory")
            .ok_or_else(|| Error::Process("WASM module must export 'memory'".to_string()))?;

        // Allocate function to get memory for input
        let alloc_func = instance
            .get_typed_func::<i32, i32>(&mut store, "alloc")
            .map_err(|e| {
                Error::Process(format!("WASM module must export 'alloc' function: {}", e))
            })?;

        // Allocate memory for input data
        let input_ptr = alloc_func
            .call(&mut store, input_data.len() as i32)
            .map_err(|e| Error::Process(format!("Failed to allocate memory: {}", e)))?;

        // Write input data to WASM memory
        memory
            .write(&mut store, input_ptr as usize, &input_data)
            .map_err(|e| Error::Process(format!("Failed to write input data: {}", e)))?;

        // Call the processing function
        let output_ptr = func
            .call(&mut store, (input_ptr, input_data.len() as i32))
            .map_err(|e| Error::Process(format!("WASM function call failed: {}", e)))?;

        // Read output data from WASM memory
        let output_data = self.read_output_from_memory(&mut store, &memory, output_ptr)?;

        // Convert output data back to MessageBatch
        let output_batch = self.bytes_to_message_batch(output_data, &msg_batch)?;

        Ok(vec![output_batch])
    }

    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

impl WasmProcessor {
    fn new(config: WasmProcessorConfig) -> Result<Self, Error> {
        // Create WASM engine
        let engine = Engine::default();

        // Load WASM module
        let module_data =   // Load from file
            std::fs::read(&config.module).map_err(|e| {
                Error::Config(format!(
                    "Failed to read WASM file '{}': {}",
                    config.module, e
                ))
            })?;

        // Compile the module
        let module = Module::new(&engine, &module_data)
            .map_err(|e| Error::Config(format!("Failed to compile WASM module: {}", e)))?;

        Ok(Self {
            config,
            engine,
            module,
        })
    }

    fn message_batch_to_bytes(&self, msg_batch: &MessageBatch) -> Result<Vec<u8>, Error> {
        // Convert MessageBatch to JSON for WASM processing
        // This is a simplified approach - in practice you might want to use a more efficient format
        let json_data = msg_batch.to_binary("value")?;
        Ok(json_data.join(&b'\n'))
    }

    fn read_output_from_memory(
        &self,
        store: &mut Store<()>,
        memory: &Memory,
        ptr: i32,
    ) -> Result<Vec<u8>, Error> {
        // Read the length of output data first (assuming it's stored at ptr)
        let mut len_bytes = [0u8; 4];
        memory
            .read(&mut *store, ptr as usize, &mut len_bytes)
            .map_err(|e| Error::Process(format!("Failed to read output length: {}", e)))?;

        let len = u32::from_le_bytes(len_bytes) as usize;

        // Read the actual data
        let mut data = vec![0u8; len];
        memory
            .read(&mut *store, (ptr + 4) as usize, &mut data)
            .map_err(|e| Error::Process(format!("Failed to read output data: {}", e)))?;

        Ok(data)
    }

    fn bytes_to_message_batch(
        &self,
        data: Vec<u8>,
        original: &MessageBatch,
    ) -> Result<MessageBatch, Error> {
        // Convert processed bytes back to MessageBatch
        // This assumes the WASM function returns JSON data
        if data.is_empty() {
            return Ok(MessageBatch::new_binary(vec![]).unwrap());
        }

        // Try to parse as JSON and create new MessageBatch
        let json_str = String::from_utf8(data)
            .map_err(|e| Error::Process(format!("Invalid UTF-8 in WASM output: {}", e)))?;

        // For simplicity, return as binary data
        // In practice, you might want to parse JSON and convert back to Arrow format
        Ok(original.new_binary_with_origin(vec![json_str.into_bytes()])?)
    }
}

struct WasmProcessorBuilder;

impl ProcessorBuilder for WasmProcessorBuilder {
    fn build(
        &self,
        _name: Option<&String>,
        config: &Option<Value>,
        _resource: &Resource,
    ) -> Result<Arc<dyn Processor>, Error> {
        if config.is_none() {
            return Err(Error::Config(
                "WASM processor configuration is missing".to_string(),
            ));
        }

        let wasm_config: WasmProcessorConfig = serde_json::from_value(config.clone().unwrap())
            .map_err(|e| Error::Config(format!("Invalid WASM processor config: {}", e)))?;

        let processor = WasmProcessor::new(wasm_config)?;
        Ok(Arc::new(processor))
    }
}

pub fn init() -> Result<(), Error> {
    register_processor_builder("wasm", Arc::new(WasmProcessorBuilder))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    #[tokio::test]
    async fn test_wasm_processor_missing_config() {
        let result = WasmProcessorBuilder.build(
            None,
            &None,
            &Resource {
                temporary: Default::default(),
                input_names: RefCell::new(Default::default()),
            },
        );
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_wasm_processor_invalid_config() {
        let config = serde_json::json!({
            "invalid": "config"
        });

        let result = WasmProcessorBuilder.build(
            None,
            &Some(config),
            &Resource {
                temporary: Default::default(),
                input_names: RefCell::new(Default::default()),
            },
        );
        assert!(result.is_err());
    }
}
