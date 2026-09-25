//! Strongly typed response extractors and memory-safe bounded stream decoders.
//!
//! Standard HTTP clients frequently suffer from Out-Of-Memory (OOM) panics when upstream
//! proxies, web application firewalls (e.g. Cloudflare), or faulty backend APIs return
//! multi-gigabyte crash dumps or unbounded streaming error bodies.
//!
//! This module implements the **Extractor Pattern**, unifying deserialization across formats
//! through the [`FromResponse`] trait, while enforcing strict capacity boundaries via
//! chunk-by-chunk streaming decoders.
//!
//! ### Supported Extractors
//!
//! | Extractor | Target Representation | Allocation & Decoding Strategy |
//! | :--- | :--- | :--- |
//! | [`Json<T>`] | Deserialized JSON payload | Buffers up to [`DEFAULT_BODY_LIMIT`] (16 MiB), decodes via [`serde_json`](https://docs.rs/serde_json). |
//! | [`Xml<T>`] | Deserialized XML payload | Buffers up to [`DEFAULT_BODY_LIMIT`] (16 MiB), decodes via [`quick-xml`](https://docs.rs/quick-xml). Requires `"xml"` feature. |
//! | [`Response`] | Raw wire response stream | Zero buffering. Provides direct streaming byte access (e.g. file downloads). |
//! | `()` | Void / Unit type | Zero body allocation on success. Validates HTTP 2xx/3xx status codes. |
//!
//! ### Quick Example
//!
//! ```rust,no_run
//! use moonclient::response::Json;
//! use moonclient::{ClientHook, MoonClient, Result};
//! use serde::Deserialize;
//!
//! #[derive(Debug, Deserialize)]
//! struct ServerStatus {
//!     healthy: bool,
//!     version: String,
//! }
//!
//! # async fn doc_example<H: ClientHook>(client: &MoonClient<H>) -> Result<()> {
//! let request = client.get("https://api.site.com/health");
//!
//! // The extractor pattern automatically decodes the body based on type inference
//! let Json(status): Json<ServerStatus> = client.execute(request).await?;
//! println!("System health: {}, version: {}", status.healthy, status.version);
//! # Ok(())
//! # }
//! ```

use crate::error::MoonError;
use async_trait::async_trait;
use reqwest::Response;

/// Default buffer capacity limit for reading error response payloads (64 KiB).
///
/// Prevents memory exhaustion attacks when upstream proxies return massive HTML error pages.
pub const DEFAULT_ERROR_BODY_LIMIT: usize = 64 * 1024;

/// Default buffer capacity limit for reading successful API response payloads (16 MiB).
///
/// Comfortably accommodates high-volume JSON and XML result sets while preventing unbounded allocations.
pub const DEFAULT_BODY_LIMIT: usize = 16 * 1024 * 1024;

/// Safely streams and buffers asynchronous response chunks up to an enforced byte ceiling.
///
/// # Implementation Details
///
/// 1. Iterates over incoming network chunks asynchronously via [`reqwest::Response::chunk`].
/// 2. Verifies that cumulative buffered bytes do not exceed `max_size`.
/// 3. If the next chunk breaches `max_size`, truncates the buffer precisely at the limit and halts streaming immediately.
/// 4. Converts the accumulated byte vector into a UTF-8 string via lossy conversion ([`String::from_utf8_lossy`]).
///
/// # Arguments
///
/// * `response` ([`Response`](https://docs.rs/reqwest/latest/reqwest/struct.Response.html)) — Raw streaming HTTP response object from `reqwest`.
/// * `max_size` (`usize`) — Maximum permitted buffer capacity in bytes.
///
/// # Returns
///
/// Returns `Ok(String)` containing the decoded text representation up to `max_size` bytes.
///
/// # Errors
///
/// Emits [`MoonError::Network`] if an I/O or connection error occurs while receiving chunks.
///
/// # Example
///
/// ```rust,no_run
/// use moonclient::response::{read_response_text_bounded, DEFAULT_ERROR_BODY_LIMIT};
/// use reqwest::Response;
///
/// # async fn doc_example(response: Response) -> moonclient::Result<()> {
/// // Read up to 64 KiB of error body text safely
/// let error_text = read_response_text_bounded(response, DEFAULT_ERROR_BODY_LIMIT).await?;
/// println!("Safely captured error payload: {}", error_text);
/// # Ok(())
/// # }
/// ```
pub async fn read_response_text_bounded(
    mut response: Response,
    max_size: usize,
) -> Result<String, MoonError> {
    let mut body_bytes = Vec::new();

    // Sequentially stream and buffer incoming response chunks
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|e| MoonError::Network(e.to_string()))?
    {
        if body_bytes.len() + chunk.len() > max_size {
            let remaining = max_size - body_bytes.len();
            body_bytes.extend_from_slice(&chunk[..remaining]);
            break;
        }
        body_bytes.extend_from_slice(&chunk);
    }

    Ok(String::from_utf8_lossy(&body_bytes).into_owned())
}

/// Abstract contract for parsing and extracting typed values from raw HTTP responses.
///
/// Powers the unified [`MoonClient::execute`](crate::client::MoonClient::execute) pipeline,
/// replacing disparate format-specific execution methods with idiomatic generic type inference.
#[async_trait]
pub trait FromResponse: Sized {
    /// Extracts and parses `Self` from an incoming wire response.
    ///
    /// # Arguments
    ///
    /// * `response` ([`Response`](https://docs.rs/reqwest/latest/reqwest/struct.Response.html)) — Raw response envelope returned by the remote server.
    ///
    /// # Returns
    ///
    /// Returns `Ok(Self)` on successful extraction and validation.
    ///
    /// # Errors
    ///
    /// Emits [`MoonError::ApiError`] if the server returned an unsuccessful HTTP status code (4xx/5xx).
    /// Emits [`MoonError::ResponseDecode`] if payload deserialization fails.
    async fn from_response(response: Response) -> Result<Self, MoonError>;
}

/// Extractor implementation for empty HTTP responses (unit type replacement for `execute_void`).
///
/// Validates that the HTTP status falls within the successful range (2xx/3xx)
/// without allocating heap memory to decode or deserialize the response body.
///
/// # Implementation Details
///
/// * **HTTP 2xx/3xx:** Immediately returns `Ok(())` with zero body reads.
/// * **HTTP 4xx/5xx:** Reads up to [`DEFAULT_ERROR_BODY_LIMIT`] (64 KiB) to build an informative [`MoonError::ApiError`].
///
/// # Example
///
/// ```rust,no_run
/// use moonclient::{ClientHook, MoonClient, Result};
///
/// # async fn doc_example<H: ClientHook>(client: &MoonClient<H>) -> Result<()> {
/// let delete_request = client.delete("https://api.site.com/items/123");
///
/// // Extracting unit `()` confirms successful HTTP 204 No Content or 200 OK
/// client.execute::<()>(delete_request).await?;
/// println!("Resource deleted successfully!");
/// # Ok(())
/// # }
/// ```
#[async_trait]
impl FromResponse for () {
    async fn from_response(response: Response) -> Result<Self, MoonError> {
        if response.status().is_success() {
            Ok(())
        } else {
            // Buffer bounded error body (64 KiB) for diagnostic reporting
            let status = response.status();
            let text = read_response_text_bounded(response, DEFAULT_ERROR_BODY_LIMIT).await?;
            Err(MoonError::ApiError {
                code: status.as_u16(),
                message: text,
            })
        }
    }
}

/// Extractor implementation yielding the raw [`reqwest::Response`](https://docs.rs/reqwest/latest/reqwest/struct.Response.html) object.
///
/// Grants consumer API clients low-level access to the response stream
/// (e.g., for byte-level binary file downloads with CLI progress bars),
/// while guaranteeing execution through all configured pre-request and post-response hooks.
///
/// # Errors
///
/// Returns [`MoonError::ApiError`] if the server returned an unsuccessful HTTP status code.
///
/// # Example
///
/// ```rust,no_run
/// use moonclient::{ClientHook, MoonClient, Result};
/// use reqwest::Response;
///
/// # async fn doc_example<H: ClientHook>(client: &MoonClient<H>) -> Result<()> {
/// let download_request = client.get("https://site.com/large_archive.zip");
///
/// // Extract raw response stream for custom chunk-by-chunk processing
/// let raw_stream: Response = client.execute(download_request).await?;
/// println!("Streaming content length: {:?}", raw_stream.content_length());
/// # Ok(())
/// # }
/// ```
#[async_trait]
impl FromResponse for reqwest::Response {
    async fn from_response(response: Response) -> Result<Self, MoonError> {
        if response.status().is_success() {
            Ok(response)
        } else {
            // Buffer bounded error body (64 KiB) for diagnostic reporting
            let status = response.status();
            let text = read_response_text_bounded(response, DEFAULT_ERROR_BODY_LIMIT).await?;
            Err(MoonError::ApiError {
                code: status.as_u16(),
                message: text,
            })
        }
    }
}

/// Extractor for automatically decoding JSON payloads from response bodies.
///
/// Wraps the deserialized data model of type `T`.
///
/// # Example
///
/// ```rust,no_run
/// use moonclient::response::Json;
/// use moonclient::{ClientHook, MoonClient, Result};
/// use serde::Deserialize;
///
/// #[derive(Debug, Deserialize)]
/// struct User {
///     id: u64,
///     name: String,
/// }
///
/// # async fn doc_example<H: ClientHook>(client: &MoonClient<H>) -> Result<()> {
/// let request = client.get("https://api.site.com/users/1");
///
/// // Tuple pattern matching unwraps the inner `User` directly
/// let Json(user): Json<User> = client.execute(request).await?;
/// println!("Loaded user: {}", user.name);
/// # Ok(())
/// # }
/// ```
pub struct Json<T>(pub T);

#[async_trait]
impl<T: serde::de::DeserializeOwned> FromResponse for Json<T> {
    /// Deserializes a JSON response body into target type `T`.
    ///
    /// # Implementation Details
    ///
    /// 1. Validates that the response status code is within `2xx`. If not, buffers up to [`DEFAULT_ERROR_BODY_LIMIT`] and emits [`MoonError::ApiError`].
    /// 2. Reads up to [`DEFAULT_BODY_LIMIT`] (16 MiB) of response text.
    /// 3. Parses the text into `T` via [`serde_json::from_str`](https://docs.rs/serde_json/latest/serde_json/fn.from_str.html).
    ///
    /// # Errors
    ///
    /// * [`MoonError::ApiError`] — If the server returns a non-2xx status code.
    /// * [`MoonError::ResponseDecode`] — If JSON syntax is invalid or fields fail to match schema expectations.
    async fn from_response(response: Response) -> Result<Self, MoonError> {
        // Validate HTTP status code
        if !response.status().is_success() {
            let status = response.status();
            // Read bounded error buffer on failure (64 KiB)
            let text = read_response_text_bounded(response, DEFAULT_ERROR_BODY_LIMIT).await?;
            return Err(MoonError::ApiError {
                code: status.as_u16(),
                message: text,
            });
        }

        // Buffer standard JSON body limit (16 MiB) on success
        let text = read_response_text_bounded(response, DEFAULT_BODY_LIMIT).await?;

        // Deserialize JSON into the requested structure
        let data = serde_json::from_str(&text).map_err(|e| MoonError::ResponseDecode {
            error_message: e.to_string(),
            raw: text,
        })?;

        Ok(Json(data))
    }
}

/// Extractor for automatically decoding XML payloads from response bodies.
///
/// Available only when the `"xml"` feature flag is enabled in `Cargo.toml`.
///
/// # Example
///
/// ```rust,no_run
/// # #[cfg(feature = "xml")]
/// # {
/// use moonclient::response::Xml;
/// use moonclient::{ClientHook, MoonClient, Result};
/// use serde::Deserialize;
///
/// #[derive(Debug, Deserialize)]
/// struct RssFeed {
///     title: String,
/// }
///
/// # async fn doc_example<H: ClientHook>(client: &MoonClient<H>) -> Result<()> {
/// let request = client.get("https://site.com/feed.xml");
/// let Xml(feed): Xml<RssFeed> = client.execute(request).await?;
/// println!("Feed title: {}", feed.title);
/// # Ok(())
/// # }
/// # }
/// ```
#[cfg(feature = "xml")]
pub struct Xml<T>(pub T);

#[cfg(feature = "xml")]
#[async_trait]
impl<T: serde::de::DeserializeOwned> FromResponse for Xml<T> {
    /// Deserializes an XML response body into target type `T`.
    ///
    /// # Implementation Details
    ///
    /// 1. Validates that the response status code is within `2xx`. If not, buffers up to [`DEFAULT_ERROR_BODY_LIMIT`] and emits [`MoonError::ApiError`].
    /// 2. Reads up to [`DEFAULT_BODY_LIMIT`] (16 MiB) of response text.
    /// 3. Parses the text into `T` via [`quick_xml::de::from_str`](https://docs.rs/quick-xml/latest/quick_xml/de/fn.from_str.html).
    ///
    /// # Errors
    ///
    /// * [`MoonError::ApiError`] — If the server returns a non-2xx status code.
    /// * [`MoonError::ResponseDecode`] — If XML syntax is invalid or structure mapping fails.
    async fn from_response(response: Response) -> Result<Self, MoonError> {
        // Validate HTTP status code
        if !response.status().is_success() {
            let status = response.status();
            // Read bounded error buffer on failure (64 KiB)
            let text = read_response_text_bounded(response, DEFAULT_ERROR_BODY_LIMIT).await?;
            return Err(MoonError::ApiError {
                code: status.as_u16(),
                message: text,
            });
        }

        // Buffer standard XML body limit (16 MiB) on success
        let text = read_response_text_bounded(response, DEFAULT_BODY_LIMIT).await?;

        // Deserialize XML via quick-xml
        let data = quick_xml::de::from_str(&text).map_err(|e| MoonError::ResponseDecode {
            error_message: e.to_string(),
            raw: text,
        })?;

        Ok(Xml(data))
    }
}
