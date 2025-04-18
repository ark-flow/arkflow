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
use arkflow_core::Error;
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::WindowUDF;
use std::sync::{Arc, RwLock};
use tracing::debug;

lazy_static::lazy_static! {
    static ref UDFS: RwLock<Vec<Arc<WindowUDF>>> = RwLock::new(Vec::new());
}

/// Register a new window UDF (User Defined Function).
///
/// This function wraps the provided WindowUDF instance in an Arc and stores it in the global UDFS list,
/// so it can later be registered with the FunctionRegistry.
///
/// # Arguments
/// * `udf` - The WindowUDF instance to register.
pub fn register(udf: WindowUDF) {
    let mut udfs = UDFS.write().expect("Failed to acquire write lock for UDFS");
    udfs.push(Arc::new(udf));
}

pub(crate) fn init<T: FunctionRegistry>(registry: &mut T) -> Result<(), Error> {
    let window_udfs = crate::processor::udf::window_udf::UDFS
        .read()
        .expect("Failed to acquire read lock for window UDFS");
    window_udfs
        .iter()
        .try_for_each(|udf| {
            let existing_udf = registry.register_udwf(Arc::clone(udf))?;
            if let Some(existing_udf) = existing_udf {
                debug!("Overwrite existing window UDF: {}", existing_udf.name());
            }
            Ok(()) as datafusion::common::Result<()>
        })
        .map_err(|e| Error::Config(format!("Failed to register window UDFs: {}", e)))
}
