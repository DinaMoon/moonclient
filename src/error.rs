//! Domain error models, failure representations, and error conversion mechanics.
//!
//! Encapsulates transport-level network faults, JSON parsing discrepancies, filesystem I/O issues,
//! and remote HTTP error status codes into a unified, strongly typed [`MoonError`] enum.
//!
//! ### Public API Stability Guarantee
//!
//! Volatile third-party error types (such as [`reqwest::Error`](https://docs.rs/reqwest/latest/reqwest/struct.Error.html)
//! or [`reqwest_middleware::Error`](https://docs.rs/reqwest-middleware/latest/reqwest_middleware/struct.Error.html))
//! are boxed or coerced into string representations at the boundary. This guarantees that major version
//! bumps in underlying network crates do not induce breaking changes in downstream consumer code.
//!
//! ### Dynamic Localization (i18n)
//!
//! All error variants integrate with Project Fluent via the [`tr!`](crate::tr) macro, formatting
//! localized diagnostic descriptions dynamically according to the active execution context
//! (Tokio Task-Local or process-wide locale).
//!
//! ### Quick Example
//!
//! ```rust
//! use moonclient::{MoonError, Result};
//!
//! fn handle_failure(result: Result<()>) {
//!     match result {
//!         Ok(()) => println!("Transaction completed successfully!"),
//!         Err(MoonError::ApiError { code, message }) => {
//!             eprintln!("Remote server error HTTP {}: {}", code, message);
//!         }
//!         Err(MoonError::ResponseDecode { error_message, raw }) => {
//!             eprintln!("Schema mismatch: {}\nRaw payload: {}", error_message, raw);
//!         }
//!         Err(err) => eprintln!("Network transaction failed: {}", err),
//!     }
//! }
//! ```

use serde::Deserialize;
use std::collections::HashMap;
use thiserror::Error;

/// Primary error type for the `moonclient` core framework.
///
/// Encapsulates transport-level network faults, parsing discrepancies, I/O failures,
/// and API validation errors. External dependency types are converted into stable internal
/// representations, guaranteeing public API stability.
#[derive(Error, Debug)]
pub enum MoonError {
    /// Failure originated within the internal middleware pipeline.
    ///
    /// Typically triggered by retry policy exhaustion, rate limiters, or tracing failures.
    #[error("{}", crate::tr!("err-middleware", "error" => .0))]
    Middleware(String),

    /// Transport-level network failure.
    ///
    /// Occurs during connection drops, DNS resolution failures, timeouts,
    /// or physical transport disconnects.
    #[error("{}", crate::tr!("err-network", "error" => .0))]
    Network(String),

    /// Serialization or deserialization failure.
    ///
    /// Caused by payload schema mismatches against expected serde structures.
    #[error("{}", crate::tr!("err-parse", "error" => .0))]
    Parse(#[from] serde_json::Error),

    /// Filesystem input/output error.
    ///
    /// Occurs when failing to read or persist session caches, disk stores,
    /// or encountering file permission violations.
    #[error("{}", crate::tr!("err-io", "error" => .0.to_string()))]
    Io(std::io::Error),

    /// Specific remote API validation failure (HTTP 422 Unprocessable Entity).
    ///
    /// Carries detailed per-field validation violation maps or flat message vectors.
    #[error("{}", crate::tr!("err-api-validation", "error" => .0.to_string()))]
    ApiValidation(ApiErrors),

    /// Standard HTTP error status returned by the remote server (HTTP 4xx, 5xx).
    ///
    /// Used for non-422 status codes (e.g., 404 Not Found, 502 Bad Gateway).
    #[error("{}", crate::tr!("err-api-error", "code" => code, "message" => message))]
    ApiError {
        /// HTTP status code returned by the server (e.g., 403, 500).
        code: u16,
        /// Raw response body or status code explanation.
        message: String,
    },

    /// Network client configuration error.
    ///
    /// Triggered by invalid rate limiter parameters, malformed base URLs,
    /// or missing required state components.
    #[error("{}", crate::tr!("err-config", "error" => .0))]
    Config(String),

    /// Response deserialization failure with bounded raw payload preservation for debugging.
    ///
    /// Prevents raw payload loss during deserialization crashes, allowing developers
    /// to inspect the unparseable payload fragment in logs.
    #[error("{}", crate::tr!("err-response-decode", "error" => error_message, "raw" => raw))]
    ResponseDecode {
        /// Textual error description provided by [`serde_json`](https://docs.rs/serde_json).
        error_message: String,
        /// Raw textual response body that triggered the crash (buffer-bounded to avoid OOM).
        raw: String,
    },

    /// Local client authorization failure.
    ///
    /// Triggered when attempting to invoke authenticated endpoints without an active session in memory.
    #[error("{}", crate::tr!("err-unauthorized", "error" => .0))]
    Unauthorized(String),

    /// Local client-side parameter validation error.
    ///
    /// Preemptively prevents dispatching obviously invalid requests, saving bandwidth and rate limits.
    #[error("{}", crate::tr!("err-validation-error", "error" => .0))]
    ValidationError(String),

    /// Unforeseen internal system failure.
    #[error("{}", crate::tr!("err-unknown"))]
    Unknown,
}

// =============================================================================
// 🔄 AUTOMATIC ERROR CONVERSIONS
// =============================================================================

impl From<reqwest_middleware::Error> for MoonError {
    /// Converts a [`reqwest_middleware::Error`](https://docs.rs/reqwest-middleware/latest/reqwest_middleware/struct.Error.html)
    /// into a stable [`MoonError::Middleware`].
    ///
    /// # Arguments
    ///
    /// * `err` ([`reqwest_middleware::Error`](https://docs.rs/reqwest-middleware/latest/reqwest_middleware/struct.Error.html)) — Middleware pipeline error.
    ///
    /// # Returns
    ///
    /// A standardized [`MoonError::Middleware`] variant.
    fn from(err: reqwest_middleware::Error) -> Self {
        MoonError::Middleware(err.to_string())
    }
}

impl From<reqwest::Error> for MoonError {
    /// Converts a [`reqwest::Error`](https://docs.rs/reqwest/latest/reqwest/struct.Error.html)
    /// into a stable [`MoonError::Network`].
    ///
    /// # Arguments
    ///
    /// * `err` ([`reqwest::Error`](https://docs.rs/reqwest/latest/reqwest/struct.Error.html)) — Transport or protocol error from reqwest.
    ///
    /// # Returns
    ///
    /// A standardized [`MoonError::Network`] variant.
    fn from(err: reqwest::Error) -> Self {
        MoonError::Network(err.to_string())
    }
}

impl From<std::io::Error> for MoonError {
    /// Converts an [`std::io::Error`](https://doc.rust-lang.org/std/io/struct.Error.html)
    /// into a [`MoonError::Io`].
    ///
    /// # Arguments
    ///
    /// * `err` ([`std::io::Error`](https://doc.rust-lang.org/std/io/struct.Error.html)) — Low-level filesystem or stream I/O error.
    ///
    /// # Returns
    ///
    /// A standardized [`MoonError::Io`] variant.
    fn from(err: std::io::Error) -> Self {
        MoonError::Io(err)
    }
}

// =============================================================================
// 📝 REMOTE API VALIDATION MODELS (HTTP 422)
// =============================================================================

/// Representation variants for remote API validation failures (HTTP 422 Unprocessable Entity).
///
/// Uses `#[serde(untagged)]` to automatically handle both structured per-field error dictionaries
/// and flat arrays of server validation warnings.
///
/// # Example
///
/// ```rust
/// use moonclient::error::ApiErrors;
///
/// // 1. Flat message list
/// let flat: ApiErrors = serde_json::from_str(r#"["Invalid email format", "Password too short"]"#).unwrap();
/// println!("Flat errors: {}", flat);
///
/// // 2. Per-field validation map
/// let fields: ApiErrors = serde_json::from_str(r#"{"email": ["Already taken"], "age": ["Must be 18+"]}"#).unwrap();
/// println!("Field errors: {}", fields);
/// ```
#[derive(Debug, Deserialize, Clone, PartialEq)]
#[serde(untagged)]
pub enum ApiErrors {
    /// Validation errors mapped to specific request fields (e.g. `{"errors": {"email": ["Already taken"]}}`).
    Fields(HashMap<String, Vec<String>>),
    /// Generic backend error messages (e.g. `{"errors": ["Access denied by rule"]}`).
    Messages(Vec<String>),
}

impl std::fmt::Display for ApiErrors {
    /// Formats the API errors into a human-readable comma-separated summary.
    ///
    /// # Arguments
    ///
    /// * `f` ([`&mut std::fmt::Formatter<'_>`](https://doc.rust-lang.org/std/fmt/struct.Formatter.html)) — Output formatter stream.
    ///
    /// # Returns
    ///
    /// Returns `std::fmt::Result` indicating successful formatting.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ApiErrors::Fields(map) => {
                let mut first = true;
                for (field, msgs) in map {
                    if !first {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}: {}", field, msgs.join("; "))?;
                    first = false;
                }
                Ok(())
            }
            ApiErrors::Messages(vec) => {
                write!(f, "{}", vec.join("; "))
            }
        }
    }
}

/// Helper structure for deserializing remote API validation error envelopes (HTTP 422).
///
/// Many modern REST backends wrap 422 validation failures inside an `{"errors": ...}` JSON object.
#[derive(Debug, Deserialize)]
pub struct ApiErrorResponse {
    /// Structured validation error payload body.
    pub errors: ApiErrors,
}

/// Ergonomic type alias for [`std::result::Result`] specialized over [`MoonError`].
///
/// Commonly used across all client methods and pipelines for concise signatures.
///
/// # Example
///
/// ```rust
/// use moonclient::{MoonError, Result};
///
/// fn check_quota(limit: u32) -> Result<()> {
///     if limit > 100 {
///         return Err(MoonError::ValidationError("Quota exceeded limit of 100".to_string()));
///     }
///     Ok(())
/// }
/// ```
pub type Result<T> = std::result::Result<T, MoonError>;
