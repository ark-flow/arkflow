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

use crate::config::{EngineConfig, LogFormat};
use crate::engine::Engine;
use crate::remote_config::RemoteConfigManager;
use clap::{Arg, Command};
use std::process;
use tokio::signal::unix::{signal, SignalKind};
use tokio_util::sync::CancellationToken;
use tracing::{info, Level};
use tracing_subscriber::fmt;

pub struct Cli {
    pub config: Option<EngineConfig>,
    pub remote_config_manager: Option<RemoteConfigManager>,
}
impl Default for Cli {
    fn default() -> Self {
        Self {
            config: None,
            remote_config_manager: None,
        }
    }
}

impl Cli {
    pub fn parse(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let matches = Command::new("arkflow")
            .version("0.4.0-rc1")
            .author("chenquan")
            .about("High-performance Rust stream processing engine, providing powerful data stream processing capabilities, supporting multiple input/output sources and processors")
            .arg(
                Arg::new("config")
                    .short('c')
                    .long("config")
                    .value_name("FILE")
                    .help("Specify the profile path")
            )
            .arg(
                Arg::new("validate")
                    .short('v')
                    .long("validate")
                    .help("Only the profile is verified, not the engine is started")
                    .action(clap::ArgAction::SetTrue),
            )
            .subcommand(
                Command::new("remote").about("Use remote configuration for automatic stream management")
                .arg(
                Arg::new("url")
                    .long("url")
                    .value_name("URL")
                    .help("Remote configuration API endpoint URL for automatic stream management")
                    .required( true)
            )
                .arg(
                    Arg::new("interval")
                        .long("interval")
                        .value_name("SECONDS")
                        .help("Interval in seconds for polling remote configuration")
                        .default_value("30"),
                )
                .arg(
                    Arg::new("token")
                        .long("token")
                        .value_name("TOKEN")
                        .help("Authentication token for remote configuration API")
                        .required( true),
                ))
            .get_matches();

        // Check if using remote configuration
        if let Some(remote) = matches.subcommand_matches("remote") {
            // Initialize remote configuration manager
            let interval = remote
                .get_one::<String>("interval")
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(30);
            let token = remote.get_one::<String>("token").cloned();
            let remote_url = remote
                .get_one::<String>("url")
                .expect("Remote configuration URL not found");

            let remote_manager = RemoteConfigManager::new(remote_url.clone(), interval, token);

            self.remote_config_manager.replace(remote_manager);
            info!("Using remote configuration from: {}", remote_url);
        } else {
            // Use local configuration file
            let config_path = matches
                .get_one::<String>("config")
                .ok_or("Configuration not found")?;

            let config = match EngineConfig::from_file(config_path) {
                Ok(config) => config,
                Err(e) => {
                    println!("Failed to load configuration file: {}", e);
                    process::exit(1);
                }
            };

            // If you just verify the configuration, exit it
            if matches.get_flag("validate") {
                info!("The config is validated.");
                return Ok(());
            }

            self.config = Some(config);
        }
        Ok(())
    }
    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        let token = CancellationToken::new();

        if let Some(remote_manager) = &self.remote_config_manager {
            // Run with remote configuration management
            remote_manager.run(token.clone()).await?;
        } else {
            // Run with local configuration
            let config = self.config.clone().unwrap();
            init_logging(&config);
            let engine = Engine::new(config);
            engine.run(token.clone()).await?;
        }

        // Set up signal handlers
        let mut sigint = signal(SignalKind::interrupt()).expect("Failed to set signal handler");
        let mut sigterm = signal(SignalKind::terminate()).expect("Failed to set signal handler");

        tokio::spawn(async move {
            tokio::select! {
                _ = sigint.recv() => {
                    info!("Received SIGINT, exiting...");

                },
                _ = sigterm.recv() => {
                    info!("Received SIGTERM, exiting...");
                }
            }
            token.cancel();
        });
        Ok(())
    }
}
pub(crate) fn init_logging(config: &EngineConfig) -> () {
    let log_level = match config.logging.level.as_str() {
        "trace" => Level::TRACE,
        "debug" => Level::DEBUG,
        "info" => Level::INFO,
        "warn" => Level::WARN,
        "error" => Level::ERROR,
        _ => Level::INFO,
    };

    let subscriber_builder = fmt::Subscriber::builder().with_max_level(log_level);

    // Check if we need to output logs to a file
    if let Some(file_path) = &config.logging.file_path {
        // Create the file and parent directories if they don't exist
        if let Some(parent) = std::path::Path::new(file_path).parent() {
            std::fs::create_dir_all(parent).ok();
        }

        // Open the file for writing
        match std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(file_path)
        {
            Ok(file) => {
                match config.logging.format {
                    LogFormat::JSON => {
                        let subscriber = subscriber_builder
                            .with_writer(std::sync::Mutex::new(file))
                            .pretty()
                            .json()
                            .finish();
                        tracing::subscriber::set_global_default(subscriber)
                            .expect("You can't set a global default log subscriber");
                    }
                    LogFormat::PLAIN => {
                        let subscriber = subscriber_builder
                            .with_writer(std::sync::Mutex::new(file))
                            .pretty()
                            .finish();
                        tracing::subscriber::set_global_default(subscriber)
                            .expect("You can't set a global default log subscriber");
                    }
                }

                info!("Logging to file: {}", file_path);
                return;
            }
            Err(e) => {
                eprintln!("Failed to open log file {}: {}", file_path, e);
                // Fall back to console logging
            }
        }
    }

    match config.logging.format {
        LogFormat::JSON => {
            let subscriber = subscriber_builder.pretty().json().finish();
            tracing::subscriber::set_global_default(subscriber)
                .expect("You can't set a global default log subscriber");
        }
        LogFormat::PLAIN => {
            let subscriber = subscriber_builder.pretty().finish();
            tracing::subscriber::set_global_default(subscriber)
                .expect("You can't set a global default log subscriber");
        }
    }
}
