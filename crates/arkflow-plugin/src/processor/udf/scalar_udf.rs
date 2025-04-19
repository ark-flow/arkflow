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
use datafusion::logical_expr::ScalarUDF;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tracing::debug;

lazy_static::lazy_static! {
   static ref UDFS: RwLock<HashMap<String,Arc<ScalarUDF>>> = RwLock::new(HashMap::new());
}

/// Register a new scalar UDF.
///
/// This function adds a UDF to the global registry. The UDF will be available for use
/// in SQL queries after the next call to `init`.
///
/// # Arguments
///
/// * `udf` - The UDF to register, wrapped in an Arc for shared ownership.
pub fn register(udf: ScalarUDF) -> Result<(), Error> {
    let mut udfs = UDFS
        .write()
        .map_err(|_| Error::Config("Failed to acquire write lock for UDFS".to_string()))?;

    let name = udf.name();
    if udfs.contains_key(name) {
        return Err(Error::Config(format!(
            "Scalar UDF with name '{}' already registered",
            name
        )));
    };
    udfs.insert(name.to_string(), Arc::new(udf));
    Ok(())
}

pub(crate) fn init<T: FunctionRegistry>(registry: &mut T) -> Result<(), Error> {
    let scalar_udfs = UDFS
        .read()
        .expect("Failed to acquire read lock for scalar UDFS");
    scalar_udfs
        .iter()
        .try_for_each(|(_, udf)| {
            let existing_udf = registry.register_udf(Arc::clone(udf))?;
            if let Some(existing_udf) = existing_udf {
                debug!("Overwrite existing scalar UDF: {}", existing_udf.name());
            }
            Ok(()) as datafusion::common::Result<()>
        })
        .map_err(|e| Error::Config(format!("Failed to register scalar UDFs: {}", e)))
}
