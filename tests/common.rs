//! Test harness helper module for logger initialization and fixture setup.

use std::sync::Once;

/// Static guard guaranteeing one-time logger configuration across concurrent test runners.
static LOGGER_INIT: Once = Once::new();

/// Prepares the test environment prior to executing integration test suites.
///
/// # Implementation Details
///
/// 1. Reads environment variables from a local `.env` file via [`dotenvy`](https://docs.rs/dotenvy) if present.
/// 2. Initializes standard test logging via [`env_logger`](https://docs.rs/env_logger) with capture enabled
///    and [`log::LevelFilter::Debug`] verbosity threshold.
/// 3. Emits a confirmation log record upon successful setup.
pub fn setup_test_environment() {
    LOGGER_INIT.call_once(|| {
        // Load .env configuration for integration tests touching live networks
        dotenvy::dotenv().ok();

        // Initialize standard test logger capturing output in the test runner harness
        env_logger::builder()
            .is_test(true)
            .filter_level(log::LevelFilter::Debug)
            .try_init()
            .ok();

        log::info!("🧪 Test environment successfully initialized!");
    });
}
