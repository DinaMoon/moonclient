//! High-performance, multi-threaded diagnostic logging engine with split-file destination sinks.
//!
//! Logging in high-concurrency API clients requires separating noisy low-level HTTP transport
//! events (retries, rate-limit pauses, connection resets) from high-level domain actions
//! (deserialized models, business errors).
//!
//! [`MoonLogger`] acts as a backend for the global [`log`](https://docs.rs/log) facade,
//! providing synchronized terminal output alongside isolated, split disk log sinks.
//!
//! ### Log Routing Architecture
//!
//! ```text
//!                         [Incoming log::Record]
//!                                   │
//!                  ┌────────────────┴────────────────┐
//!                  ▼                                 ▼
//!         [Standard Output]                 [Configured Sinks]
//!        (Terminal Diagnostics)                      │
//!                                           ┌────────┴────────┐
//!                                           ▼                 ▼
//!                                    [Global Sink]      [Split Sinks]
//!                                     (all events)            │
//!                                           ┌─────────────────┴─────────────────┐
//!                                           ▼                                   ▼
//!                                     [Core Sink]                          [API Sink]
//!                             (target: "moonclient*")             (target: external domains)
//! ```
//!
//! ### Panic & Poisoning Resilience
//!
//! Log file descriptors are wrapped in [`std::sync::Mutex`]. If a worker thread panics while
//! holding a log file lock, [`MoonLogger`] automatically recovers file ownership via
//! [`unwrap_or_else(|e| e.into_inner())`](std::sync::PoisonError::into_inner), guaranteeing
//! that diagnostic logging continues uninterrupted across multi-threaded task crashes.
//!
//! ### Quick Example
//!
//! ```rust,no_run
//! use moonclient::logger;
//! use log::LevelFilter;
//!
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Initialize logger with split sinks and Debug filtering threshold
//!     logger::init()
//!         .with_level(LevelFilter::Debug)
//!         .with_split_files("logs/moonclient.log", "logs/api_requests.log")
//!         .setup()?;
//!
//!     log::info!("🚀 Application logging initialized successfully!");
//!     Ok(())
//! }
//! ```

use chrono::Local;
use log::{LevelFilter, Metadata, Record};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// Thread-safe file output sink for asynchronous logging pipelines.
///
/// Encapsulates a writable filesystem [`File`](https://doc.rust-lang.org/std/fs/struct.File.html)
/// handle within a mutex and reference counter, synchronizing write operations across
/// concurrent worker threads.
type LogFileSink = Arc<Mutex<File>>;

/// Configurable multi-destination logger implementing the [`log::Log`](https://docs.rs/log/latest/log/trait.Log.html) facade.
///
/// Manages terminal formatting, target-based event routing, and non-blocking file writes
/// with automatic parent directory creation and mutex poison recovery.
pub struct MoonLogger {
    /// Consolidated sink recording all process log events regardless of target module.
    global_sink: Option<LogFileSink>,
    /// Dedicated sink recording internal `moonclient` core transport and middleware events.
    core_sink: Option<LogFileSink>,
    /// Dedicated sink recording domain-specific consumer events (e.g. Shikimori, Gelbooru, or custom APIs).
    api_sink: Option<LogFileSink>,
    /// Global log priority cutoff threshold. Records below this level are discarded immediately.
    default_level: LevelFilter,
}

impl MoonLogger {
    /// Opens or creates a log file on disk in append-only mode.
    ///
    /// # Implementation Details
    ///
    /// Inspects the destination path and recursively creates any missing parent directories
    /// via [`std::fs::create_dir_all`] before opening the file handle with write and append flags.
    ///
    /// # Arguments
    ///
    /// * `path` ([`&Path`](https://doc.rust-lang.org/std/path/struct.Path.html)) — Filesystem location of the target log file.
    ///
    /// # Returns
    ///
    /// Returns `Ok(File)` opened with append permissions.
    ///
    /// # Errors
    ///
    /// Emits [`std::io::Error`](https://doc.rust-lang.org/std/io/struct.Error.html) if directory
    /// creation fails or the file cannot be created due to permission constraints.
    fn open_log_file(path: &Path) -> std::io::Result<File> {
        if let Some(parent) = path.parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent)?;
            }
        }
        OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(path)
    }

    /// Safely appends a formatted message line to an active file sink under lock.
    ///
    /// # Implementation Details
    ///
    /// Acquires the sink's [`Mutex`]. If the mutex was poisoned by a previous thread panic,
    /// it recovers the underlying file reference via `unwrap_or_else(|e| e.into_inner())`.
    /// Immediately writes the formatted line followed by a newline and flushes the buffer.
    ///
    /// # Arguments
    ///
    /// * `sink` (`&LogFileSink`) — Shared mutex-protected file handle.
    /// * `message` (`&str`) — Pre-formatted log line text.
    fn write_to_sink(sink: &LogFileSink, message: &str) {
        let mut file = sink.lock().unwrap_or_else(|e| e.into_inner());
        let _ = writeln!(file, "{}", message);
        let _ = file.flush();
    }
}

impl log::Log for MoonLogger {
    /// Determines whether a log record with the specified metadata should be processed.
    ///
    /// # Arguments
    ///
    /// * `metadata` ([`&Metadata`](https://docs.rs/log/latest/log/struct.Metadata.html)) — Log record metadata including target and verbosity level.
    ///
    /// # Returns
    ///
    /// Returns `true` if the record's [`Level`](https://docs.rs/log/latest/log/enum.Level.html)
    /// is less than or equal to `self.default_level`.
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= self.default_level
    }

    /// Formats and dispatches an incoming diagnostic record to stdout and active file sinks.
    ///
    /// # Implementation Details
    ///
    /// 1. Short-circuits immediately if `self.enabled()` returns `false`.
    /// 2. Formats local timestamp with millisecond precision via [`chrono::Local`](https://docs.rs/chrono/latest/chrono/offset/struct.Local.html) (`%Y-%m-%d %H:%M:%S%.3f`).
    /// 3. Builds a structured line: `[timestamp] [LEVEL] [target] message`.
    /// 4. Prints the formatted line to standard output for real-time console feedback.
    /// 5. Writes to `global_sink` if configured.
    /// 6. Evaluates target prefix: targets starting with `"moonclient"` route to `core_sink`, while all other targets route to `api_sink`.
    ///
    /// # Arguments
    ///
    /// * `record` ([`&Record`](https://docs.rs/log/latest/log/struct.Record.html)) — Diagnostic entry emitted by a logging macro (e.g. `log::info!`).
    fn log(&self, record: &Record) {
        if !self.enabled(record.metadata()) {
            return;
        }

        // Format the current local timestamp with millisecond precision
        let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S%.3f");
        let level = record.level();
        let target = record.target();
        let args = record.args();

        // Assemble the final log line representation
        let log_line = format!("[{}] [{:<5}] [{}] {}", timestamp, level, target, args);

        // Echo to stdout for real-time developer terminal diagnostics
        println!("{}", log_line);

        // Dispatch to global sink if configured
        if let Some(global) = &self.global_sink {
            Self::write_to_sink(global, &log_line);
        }

        // Perform target routing when split log destinations are active
        if self.core_sink.is_some() || self.api_sink.is_some() {
            // Write internal framework events to core sink
            if target.starts_with("moonclient") {
                if let Some(core) = &self.core_sink {
                    Self::write_to_sink(core, &log_line);
                }
            } else {
                // Route all external domain events (Shikimori, Gelbooru, consumer crates) to API sink
                if let Some(api) = &self.api_sink {
                    Self::write_to_sink(api, &log_line);
                }
            }
        }
    }

    /// Flushes all active file sink buffers to persistent disk storage.
    ///
    /// Iterates over all configured file handles (`global_sink`, `core_sink`, `api_sink`)
    /// and invokes [`Write::flush`] under mutex protection, ensuring data integrity before shutdown.
    fn flush(&self) {
        if let Some(global) = &self.global_sink {
            let mut file = global.lock().unwrap_or_else(|e| e.into_inner());
            let _ = file.flush();
        }
        if let Some(core) = &self.core_sink {
            let mut file = core.lock().unwrap_or_else(|e| e.into_inner());
            let _ = file.flush();
        }
        if let Some(api) = &self.api_sink {
            let mut file = api.lock().unwrap_or_else(|e| e.into_inner());
            let _ = file.flush();
        }
    }
}

/// Fluent builder for configuring and registering a [`MoonLogger`] instance.
///
/// Provides a declarative interface to set log levels, define file destinations,
/// and register the logger with the global [`log`](https://docs.rs/log) facade.
pub struct MoonLoggerBuilder {
    /// Optional destination path for consolidated process-wide logging.
    global_path: Option<PathBuf>,
    /// Optional destination path for `moonclient` core transport logs.
    core_path: Option<PathBuf>,
    /// Optional destination path for external domain API logs.
    api_path: Option<PathBuf>,
    /// Maximum log level filter to apply.
    default_level: LevelFilter,
}

impl MoonLoggerBuilder {
    /// Creates a new builder initialized with [`LevelFilter::Info`](https://docs.rs/log/latest/log/enum.LevelFilter.html#variant.Info) and no file sinks.
    ///
    /// # Returns
    ///
    /// A clean [`MoonLoggerBuilder`] instance ready for configuration.
    ///
    /// # Example
    ///
    /// ```rust
    /// use moonclient::logger::MoonLoggerBuilder;
    ///
    /// let builder = MoonLoggerBuilder::new();
    /// println!("Logger builder created with default Info level");
    /// ```
    pub fn new() -> Self {
        Self {
            global_path: None,
            core_path: None,
            api_path: None,
            default_level: LevelFilter::Info,
        }
    }

    /// Sets the maximum logging verbosity threshold.
    ///
    /// Diagnostic records with priority lower than `level` will be filtered out before formatting.
    ///
    /// # Arguments
    ///
    /// * `level` ([`LevelFilter`](https://docs.rs/log/latest/log/enum.LevelFilter.html)) — Cutoff threshold (e.g. `LevelFilter::Debug`, `LevelFilter::Trace`).
    ///
    /// # Returns
    ///
    /// The updated builder instance.
    ///
    /// # Example
    ///
    /// ```rust
    /// use moonclient::logger::MoonLoggerBuilder;
    /// use log::LevelFilter;
    ///
    /// let builder = MoonLoggerBuilder::new().with_level(LevelFilter::Debug);
    /// ```
    pub fn with_level(mut self, level: LevelFilter) -> Self {
        self.default_level = level;
        self
    }

    /// Configures a single consolidated file destination for all log records.
    ///
    /// All events matching the verbosity filter will be written to this file regardless of module path.
    ///
    /// # Arguments
    ///
    /// * `path` (`impl Into<`[`PathBuf`](https://doc.rust-lang.org/std/path/struct.PathBuf.html)`>`) — Target file path.
    ///
    /// # Returns
    ///
    /// The updated builder instance.
    ///
    /// # Example
    ///
    /// ```rust
    /// use moonclient::logger::MoonLoggerBuilder;
    ///
    /// let builder = MoonLoggerBuilder::new().with_global_file("logs/app.log");
    /// ```
    pub fn with_global_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.global_path = Some(path.into());
        self
    }

    /// Configures independent file destinations for internal framework logs and external API adapters.
    ///
    /// Records with target module starting with `"moonclient"` are routed to `core_path`,
    /// while all other events are routed to `api_path`.
    ///
    /// # Arguments
    ///
    /// * `core_path` (`impl Into<`[`PathBuf`](https://doc.rust-lang.org/std/path/struct.PathBuf.html)`>`) — Destination file path for `moonclient` engine events.
    /// * `api_path` (`impl Into<`[`PathBuf`](https://doc.rust-lang.org/std/path/struct.PathBuf.html)`>`) — Destination file path for external domain events.
    ///
    /// # Returns
    ///
    /// The updated builder instance.
    ///
    /// # Example
    ///
    /// ```rust
    /// use moonclient::logger::MoonLoggerBuilder;
    ///
    /// let builder = MoonLoggerBuilder::new()
    ///     .with_split_files("logs/moonclient.log", "logs/shikimori.log");
    /// ```
    pub fn with_split_files(
        mut self,
        core_path: impl Into<PathBuf>,
        api_path: impl Into<PathBuf>,
    ) -> Self {
        self.core_path = Some(core_path.into());
        self.api_path = Some(api_path.into());
        self
    }

    /// Builds the logger, creates required directories and files, and registers it as the global logger.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` upon successful registration.
    ///
    /// # Errors
    ///
    /// Emits a [`Box<dyn std::error::Error>`] if:
    /// * Any target file sink fails to open (e.g. disk permission errors or invalid paths).
    /// * A global logger has already been initialized in the current process via [`log::set_boxed_logger`](https://docs.rs/log/latest/log/fn.set_boxed_logger.html).
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use moonclient::logger::MoonLoggerBuilder;
    /// use log::LevelFilter;
    ///
    /// fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     MoonLoggerBuilder::new()
    ///         .with_level(LevelFilter::Info)
    ///         .with_global_file("logs/combined.log")
    ///         .setup()?;
    ///
    ///     log::info!("System online!");
    ///     Ok(())
    /// }
    /// ```
    pub fn setup(self) -> Result<(), Box<dyn std::error::Error>> {
        let global_sink = match self.global_path {
            Some(path) => {
                let file = MoonLogger::open_log_file(&path)?;
                Some(Arc::new(Mutex::new(file)))
            }
            None => None,
        };

        let core_sink = match self.core_path {
            Some(path) => {
                let file = MoonLogger::open_log_file(&path)?;
                Some(Arc::new(Mutex::new(file)))
            }
            None => None,
        };

        let api_sink = match self.api_path {
            Some(path) => {
                let file = MoonLogger::open_log_file(&path)?;
                Some(Arc::new(Mutex::new(file)))
            }
            None => None,
        };

        let logger = MoonLogger {
            global_sink,
            core_sink,
            api_sink,
            default_level: self.default_level,
        };

        // Register the logger instance with the global log facade
        log::set_boxed_logger(Box::new(logger))?;
        log::set_max_level(self.default_level);

        Ok(())
    }
}

/// Initiates a new fluent [`MoonLoggerBuilder`] pipeline.
///
/// Convenience entry point equivalent to calling [`MoonLoggerBuilder::new()`].
///
/// # Returns
///
/// A fresh [`MoonLoggerBuilder`] configured with [`LevelFilter::Info`](https://docs.rs/log/latest/log/enum.LevelFilter.html#variant.Info).
///
/// # Example
///
/// ```rust
/// use moonclient::logger;
///
/// let builder = logger::init();
/// println!("Logger builder initialized via helper");
/// ```
pub fn init() -> MoonLoggerBuilder {
    MoonLoggerBuilder::new()
}
