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

//! Output component module
//!
//! The output component is responsible for sending the processed data to the target system.

use arkflow_core::Error;

pub mod deltalake;
pub mod drop;
pub mod http;
pub mod kafka;
pub mod mqtt;
pub mod nats;
pub mod redis;
pub mod sql;
pub mod stdout;

pub fn init() -> Result<(), Error> {
    deltalake::init()?;
    drop::init()?;
    http::init()?;
    kafka::init()?;
    mqtt::init()?;
    stdout::init()?;
    sql::init()?;
    nats::init()?;
    redis::init()?;
    Ok(())
}
