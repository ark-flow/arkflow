//! Output component module
//!
//! The output component is responsible for sending the processed data to the target system.

use std::sync::OnceLock;

mod drop;
pub mod file;
pub mod http;
pub mod kafka;
pub mod mqtt;
pub mod stdout;

lazy_static::lazy_static! {
    static ref INITIALIZED: OnceLock<()> = OnceLock::new();
}

pub fn init() {
    INITIALIZED.get_or_init(|| {
        drop::init();
        file::init();
        http::init();
        kafka::init();
        mqtt::init();
        stdout::init();
    });
}
