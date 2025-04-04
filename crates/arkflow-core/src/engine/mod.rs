use crate::config::EngineConfig;
use std::process;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::signal::unix::{signal, SignalKind};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use axum::extract::State;
use axum::http::StatusCode;
// Import axum related dependencies
use axum::{routing::get, Router};

/// Health check status
struct HealthState {
    /// Whether the engine has been initialized
    is_ready: AtomicBool,
    /// Whether the engine is currently running
    is_running: AtomicBool,
}

/// The main engine that manages stream processing flows and health checks
///
/// The Engine is responsible for:
/// - Starting and managing the health check server
/// - Initializing and running all configured streams
/// - Handling graceful shutdown on signals
pub struct Engine {
    /// Engine configuration containing stream definitions and health check settings
    config: EngineConfig,
    /// Health check status shared between the engine and health check endpoints
    health_state: Arc<HealthState>,
}
impl Engine {
    /// Create a new engine with the provided configuration
    ///
    /// Initializes a new Engine instance with the given configuration and
    /// sets up the health state with default values (not ready, not running).
    ///
    /// # Arguments
    /// * `config` - The engine configuration containing stream definitions and settings
    pub fn new(config: EngineConfig) -> Self {
        Self {
            config,
            health_state: Arc::new(HealthState {
                is_ready: AtomicBool::new(false),
                is_running: AtomicBool::new(false),
            }),
        }
    }

    /// Start the health check server if enabled in configuration
    ///
    /// Sets up HTTP endpoints for health, readiness, and liveness checks.
    /// The server runs in a separate task and doesn't block the main execution.
    ///
    /// # Returns
    /// * `Ok(())` if the server started successfully or if health checks are disabled
    /// * `Err` if there was an error parsing the address or starting the server
    async fn start_health_check_server(&self) -> Result<(), Box<dyn std::error::Error>> {
        if !self.config.health_check.is_none() {
            return Ok(());
        }
        let Some(health_check) = &self.config.health_check else {
            return Ok(());
        };
        if !health_check.enabled {
            return Ok(());
        }

        let health_state = self.health_state.clone();

        // Create routes
        let app = Router::new()
            .route(&*health_check.path, get(Self::handle_health))
            .route(&*health_check.readiness_path, get(Self::handle_readiness))
            .route(&*health_check.liveness_path, get(Self::handle_liveness))
            .with_state(health_state);

        let addr = &*health_check
            .address
            .parse()
            .map_err(|e| format!("Invalid health check address: {}", e))?;

        info!("Starting health check server on {}", addr);

        // Start the server
        tokio::spawn(async move {
            axum::Server::bind(&addr)
                .serve(app.into_make_service())
                .await
                .map_err(|e| error!("Health check server error: {}", e))
        });

        Ok(())
    }

    /// Health check handler function that returns the overall health status
    ///
    /// Returns OK (200) if the engine is running, otherwise SERVICE_UNAVAILABLE (503)
    ///
    /// # Arguments
    /// * `state` - The shared health state containing running status
    async fn handle_health(State(state): State<Arc<HealthState>>) -> StatusCode {
        if state.is_running.load(Ordering::SeqCst) {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        }
    }

    /// Readiness check handler function that indicates if the engine is ready to process requests
    ///
    /// Returns OK (200) if the engine is initialized and ready, otherwise SERVICE_UNAVAILABLE (503)
    ///
    /// # Arguments
    /// * `state` - The shared health state containing readiness status
    async fn handle_readiness(State(state): State<Arc<HealthState>>) -> StatusCode {
        if state.is_ready.load(Ordering::SeqCst) {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        }
    }

    /// Liveness check handler function that indicates if the engine process is alive
    ///
    /// Always returns OK (200) as long as the server can respond to the request
    ///
    /// # Arguments
    /// * `_` - Unused health state parameter
    async fn handle_liveness(_: State<Arc<HealthState>>) -> StatusCode {
        // As long as the server can respond, it is considered alive
        StatusCode::OK
    }
    /// Run the engine and all configured streams
    ///
    /// This method:
    /// 1. Starts the health check server if enabled
    /// 2. Initializes all configured streams
    /// 3. Sets up signal handlers for graceful shutdown
    /// 4. Runs all streams concurrently
    /// 5. Waits for all streams to complete
    ///
    /// Returns an error if any part of the initialization or execution fails
    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Start the health check server
        self.start_health_check_server().await?;

        // Create and run all flows
        let mut streams = Vec::new();
        let mut handles = Vec::new();

        for (i, stream_config) in self.config.streams.iter().enumerate() {
            info!("Initializing flow #{}", i + 1);

            match stream_config.build() {
                Ok(stream) => {
                    streams.push(stream);
                }
                Err(e) => {
                    error!("Initializing flow #{} error: {}", i + 1, e);
                    process::exit(1);
                }
            }
        }

        // Set the readiness status
        self.health_state.is_ready.store(true, Ordering::SeqCst);
        // Set up signal handlers
        let mut sigint = signal(SignalKind::interrupt()).expect("Failed to set signal handler");
        let mut sigterm = signal(SignalKind::terminate()).expect("Failed to set signal handler");
        let token = CancellationToken::new();
        let token_clone = token.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = sigint.recv() => {
                    info!("Received SIGINT, exiting...");

                },
                _ = sigterm.recv() => {
                    info!("Received SIGTERM, exiting...");
                }
            }

            token_clone.cancel();
        });

        for (i, mut stream) in streams.into_iter().enumerate() {
            info!("Starting flow #{}", i + 1);
            let cancellation_token = token.clone();
            let handle = tokio::spawn(async move {
                match stream.run(cancellation_token).await {
                    Ok(_) => info!("Flow #{} completed successfully", i + 1),
                    Err(e) => {
                        error!("Flow #{} ran with error: {}", i + 1, e)
                    }
                }
            });

            handles.push(handle);
        }

        // Set the running status
        self.health_state.is_running.store(true, Ordering::SeqCst);

        // Wait for all flows to complete
        for handle in handles {
            handle.await?;
        }

        info!("All flow tasks have been complete");
        Ok(())
    }
}
