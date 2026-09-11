//! Implementation of the universal MoonClient network core, builder, and resilience middleware.
//!
//! This module contains the foundational HTTP execution engine ([`MoonClient`]) and its fluent
//! constructor pipeline ([`MoonClientBuilder`]). It orchestrates transport policies, automated
//! exponential retries, rate-limiting quotas, lifecycle hooks, and response decoding.
//!
//! ### Zero-Panic Multipart Streaming Guarantee
//!
//! Standard retry middleware (such as [`reqwest_retry::RetryTransientMiddleware`](https://docs.rs/reqwest-retry))
//! attempts to clone the request payload. When sending streaming multipart bodies (such as file or video uploads),
//! the request is non-cloneable, causing conventional middleware to panic or abort.
//!
//! [`MoonRetryMiddleware`] automatically detects non-cloneable requests via `req.try_clone()` and transparently
//! passes them through to the transport layer in a single attempt, eliminating panics while preserving
//! retries for standard requests.
//!
//! ### Quick Example
//!
//! ```rust,no_run
//! use moonclient::response::Json;
//! use moonclient::{ClientHook, MoonClient, Result};
//! use serde::Deserialize;
//! use std::time::Duration;
//!
//! #[derive(Debug, Deserialize)]
//! struct Post {
//!     id: u32,
//!     title: String,
//! }
//!
//! struct NoopHook;
//! #[async_trait::async_trait]
//! impl ClientHook for NoopHook {
//!     type Error = std::convert::Infallible;
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     // 1. Build client with 5 RPS rate limit and 3 retries
//!     let client = MoonClient::builder(NoopHook)
//!         .with_base_url("https://jsonplaceholder.typicode.com")
//!         .requests_per_second(5)
//!         .with_max_retries(3)
//!         .build();
//!
//!     // 2. Dispatch request and extract typed JSON
//!     let request = client.get("https://jsonplaceholder.typicode.com/posts/1");
//!     let Json(post): Json<Post> = client.execute(request).await?;
//!
//!     println!("Fetched post #{}: {}", post.id, post.title);
//!     Ok(())
//! }
//! ```

use async_trait::async_trait;
use bytes::Bytes;
use governor::{Quota, RateLimiter, clock::DefaultClock, state::direct::NotKeyed};
use reqwest::Client;
use reqwest_middleware::{
    ClientBuilder, ClientWithMiddleware, Middleware, Next, RequestBuilder,
    Result as MiddlewareResult,
};
use reqwest_retry::{DefaultRetryableStrategy, RetryPolicy, RetryableStrategy};
use reqwest_retry::{RetryTransientMiddleware, policies::ExponentialBackoff};
use std::num::NonZeroU32;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::io::AsyncWriteExt;

use crate::error::{MoonError, Result};
use crate::hooks::ClientHook;
use crate::response::FromResponse;

/// Default base API URL used as a fallback endpoint during builder initialization.
pub const DEFAULT_API_URL: &str = "https://test.api.io";

/// Default application name supplied in the mandatory `User-Agent` HTTP header.
pub const DEFAULT_APP_NAME: &str = "moonclient";

/// Default maximum allowable buffer capacity for successful API response bodies (16 MiB).
pub const DEFAULT_MAX_RESPONSE_SIZE: usize = 16 * 1024 * 1024;

/// Type alias for the in-memory token bucket rate limiter powered by the `governor` crate.
pub(crate) type ClientRateLimiter =
    RateLimiter<NotKeyed, governor::state::InMemoryState, DefaultClock>;

/// Internal rate limiting strategy holding active token buckets.
///
/// Holds an [`std::sync::Arc`]-wrapped vector of active rate limiter instances alongside
/// their corresponding sliding window durations for fast cloning on the critical execution path.
#[derive(Clone)]
pub(crate) struct RateLimitStrategy {
    /// Active rate limiters paired with their duration windows (RPS/RPM).
    pub(crate) limiters: Arc<Vec<(Duration, Arc<ClientRateLimiter>)>>,
}

/// Universal, high-performance `MoonClient` network engine.
///
/// Orchestrates the transport layer, automated retries, rate-limiting quotas, and timeouts.
/// Decoupled from domain-specific business logic and authentication schemes via
/// the pluggable lifecycle interceptor hook `H`.
///
/// # Concurrency
///
/// `MoonClient` is cheaply cloneable via internal [`std::sync::Arc`] references and is completely
/// thread-safe ([`Send`] + [`Sync`]). It is designed to be instantiated once and shared
/// across the entire application runtime.
#[derive(Clone)]
pub struct MoonClient<H: ClientHook> {
    /// Underlying HTTP client instrumented with middleware (retries, distributed tracing).
    pub(crate) http: ClientWithMiddleware,
    /// Direct uninstrumented `reqwest::Client` instance (used for unintercepted streaming and token refreshes).
    pub(crate) inner: Client,
    /// Base target endpoint URL.
    pub(crate) base_url: url::Url,
    /// Unique `User-Agent` header value attached to all outbound requests.
    pub(crate) user_agent: String,
    /// Configurable client-wide timeout protected by an RwLock.
    pub(crate) timeout: Arc<std::sync::RwLock<Option<Duration>>>,
    /// Abstract rate limiter provider (in-memory token bucket or distributed Redis synchronization).
    pub(crate) limiter: Arc<dyn crate::limiter::RequestLimiter>,
    /// Configurable maximum allowable response body capacity tracked by an atomic counter.
    pub(crate) max_response_size: Arc<AtomicUsize>,
    /// Pluggable request lifecycle hook instance tailored for a specific API domain.
    pub(crate) hook: Arc<H>,
}

impl<H: ClientHook> MoonClient<H> {
    /// Initializes a new client builder pipeline for the specified lifecycle hook.
    ///
    /// # Arguments
    ///
    /// * `hook` (`H`) — Instance implementing [`ClientHook`] tailored for a target API domain.
    ///
    /// # Returns
    ///
    /// A fresh [`MoonClientBuilder<H>`] initialized with default settings.
    ///
    /// # Example
    ///
    /// ```rust
    /// use moonclient::{ClientHook, MoonClient};
    ///
    /// struct MyHook;
    /// #[async_trait::async_trait]
    /// impl ClientHook for MyHook {
    ///     type Error = std::convert::Infallible;
    /// }
    ///
    /// let builder = MoonClient::builder(MyHook);
    /// ```
    pub fn builder(hook: H) -> MoonClientBuilder<H> {
        MoonClientBuilder::new(hook)
    }

    /// Constructs an asynchronous `GET` request routed through the middleware pipeline.
    ///
    /// # Arguments
    ///
    /// * `url` (`U`) — Target destination URL implementing [`reqwest::IntoUrl`](https://docs.rs/reqwest/latest/reqwest/trait.IntoUrl.html).
    ///
    /// # Returns
    ///
    /// A pre-configured [`RequestBuilder`](https://docs.rs/reqwest-middleware/latest/reqwest_middleware/struct.RequestBuilder.html).
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use moonclient::{ClientHook, MoonClient};
    /// # fn doc_example<H: ClientHook>(client: &MoonClient<H>) {
    /// let req = client.get("https://api.site.com/items");
    /// # }
    /// ```
    pub fn get<U: reqwest::IntoUrl>(&self, url: U) -> RequestBuilder {
        self.http.get(url)
    }

    /// Constructs an asynchronous `POST` request routed through the middleware pipeline.
    ///
    /// # Arguments
    ///
    /// * `url` (`U`) — Target destination URL implementing [`reqwest::IntoUrl`](https://docs.rs/reqwest/latest/reqwest/trait.IntoUrl.html).
    ///
    /// # Returns
    ///
    /// A pre-configured [`RequestBuilder`](https://docs.rs/reqwest-middleware/latest/reqwest_middleware/struct.RequestBuilder.html).
    pub fn post<U: reqwest::IntoUrl>(&self, url: U) -> RequestBuilder {
        self.http.post(url)
    }

    /// Constructs an asynchronous `PUT` request routed through the middleware pipeline.
    ///
    /// # Arguments
    ///
    /// * `url` (`U`) — Target destination URL implementing [`reqwest::IntoUrl`](https://docs.rs/reqwest/latest/reqwest/trait.IntoUrl.html).
    ///
    /// # Returns
    ///
    /// A pre-configured [`RequestBuilder`](https://docs.rs/reqwest-middleware/latest/reqwest_middleware/struct.RequestBuilder.html).
    pub fn put<U: reqwest::IntoUrl>(&self, url: U) -> RequestBuilder {
        self.http.put(url)
    }

    /// Constructs an asynchronous `PATCH` request routed through the middleware pipeline.
    ///
    /// # Arguments
    ///
    /// * `url` (`U`) — Target destination URL implementing [`reqwest::IntoUrl`](https://docs.rs/reqwest/latest/reqwest/trait.IntoUrl.html).
    ///
    /// # Returns
    ///
    /// A pre-configured [`RequestBuilder`](https://docs.rs/reqwest-middleware/latest/reqwest_middleware/struct.RequestBuilder.html).
    pub fn patch<U: reqwest::IntoUrl>(&self, url: U) -> RequestBuilder {
        self.http.patch(url)
    }

    /// Constructs an asynchronous `DELETE` request routed through the middleware pipeline.
    ///
    /// # Arguments
    ///
    /// * `url` (`U`) — Target destination URL implementing [`reqwest::IntoUrl`](https://docs.rs/reqwest/latest/reqwest/trait.IntoUrl.html).
    ///
    /// # Returns
    ///
    /// A pre-configured [`RequestBuilder`](https://docs.rs/reqwest-middleware/latest/reqwest_middleware/struct.RequestBuilder.html).
    pub fn delete<U: reqwest::IntoUrl>(&self, url: U) -> RequestBuilder {
        self.http.delete(url)
    }

    /// Constructs an asynchronous `HEAD` request routed through the middleware pipeline.
    ///
    /// # Arguments
    ///
    /// * `url` (`U`) — Target destination URL implementing [`reqwest::IntoUrl`](https://docs.rs/reqwest/latest/reqwest/trait.IntoUrl.html).
    ///
    /// # Returns
    ///
    /// A pre-configured [`RequestBuilder`](https://docs.rs/reqwest-middleware/latest/reqwest_middleware/struct.RequestBuilder.html).
    pub fn head<U: reqwest::IntoUrl>(&self, url: U) -> RequestBuilder {
        self.http.head(url)
    }

    /// Constructs an asynchronous request for an arbitrary HTTP method routed through middleware.
    ///
    /// # Arguments
    ///
    /// * `method` ([`reqwest::Method`](https://docs.rs/reqwest/latest/reqwest/struct.Method.html)) — Target HTTP verb (e.g. `Method::OPTIONS`).
    /// * `url` (`U`) — Target destination URL implementing [`reqwest::IntoUrl`](https://docs.rs/reqwest/latest/reqwest/trait.IntoUrl.html).
    ///
    /// # Returns
    ///
    /// A pre-configured [`RequestBuilder`](https://docs.rs/reqwest-middleware/latest/reqwest_middleware/struct.RequestBuilder.html).
    pub fn request<U: reqwest::IntoUrl>(&self, method: reqwest::Method, url: U) -> RequestBuilder {
        self.http.request(method, url)
    }

    /// Retrieves an immutable reference to the configured base API URL.
    ///
    /// # Returns
    ///
    /// Reference to the parsed [`url::Url`](https://docs.rs/url/latest/url/struct.Url.html).
    pub fn base_url(&self) -> &url::Url {
        &self.base_url
    }

    /// Retrieves the current maximum allowable response body capacity threshold in bytes.
    ///
    /// # Returns
    ///
    /// Current capacity threshold in bytes (defaults to 16 MiB).
    pub fn max_response_size(&self) -> usize {
        self.max_response_size.load(Ordering::Relaxed)
    }

    /// Dynamically adjusts the maximum allowable response body capacity on the fly.
    ///
    /// Updates the internal atomic counter without requiring client re-instantiation.
    ///
    /// # Arguments
    ///
    /// * `bytes` (`usize`) — New maximum response capacity threshold in bytes.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use moonclient::{ClientHook, MoonClient};
    /// # fn doc_example<H: ClientHook>(client: &MoonClient<H>) {
    /// // Increase buffer limit to 32 MiB for large downloads
    /// client.set_max_response_size(32 * 1024 * 1024);
    /// assert_eq!(client.max_response_size(), 32 * 1024 * 1024);
    /// # }
    /// ```
    pub fn set_max_response_size(&self, bytes: usize) {
        self.max_response_size.store(bytes, Ordering::Relaxed);
    }

    /// Retrieves an [`std::sync::Arc`] reference to the attached request lifecycle hook.
    ///
    /// # Returns
    ///
    /// Cloned or borrowed reference to the domain lifecycle hook `H`.
    pub fn hook(&self) -> &Arc<H> {
        &self.hook
    }

    /// Retrieves the client's configured `User-Agent` string.
    ///
    /// # Returns
    ///
    /// Borrowed slice of the user agent string.
    pub fn user_agent(&self) -> &str {
        &self.user_agent
    }

    /// Accesses the underlying uninstrumented [`reqwest::Client`](https://docs.rs/reqwest/latest/reqwest/struct.Client.html).
    ///
    /// Allows lifecycle hooks to dispatch out-of-band administrative requests (e.g. OAuth token refreshes)
    /// without entering recursive middleware loops or triggering duplicate interceptors.
    ///
    /// # Returns
    ///
    /// Reference to the raw inner client.
    pub fn inner(&self) -> &Client {
        &self.inner
    }

    /// Injects the configured `User-Agent` header into an outbound request builder.
    pub(crate) fn apply_user_agent(&self, request: RequestBuilder) -> RequestBuilder {
        request.header(reqwest::header::USER_AGENT, &self.user_agent)
    }

    // =========================================================================
    // ⏱️ TIMEOUT MANAGEMENT
    // =========================================================================

    /// Sets an explicit timeout duration applied to all subsequent outbound requests.
    ///
    /// This setting overrides the default timeout across all worker threads sharing this client.
    ///
    /// # Arguments
    ///
    /// * `timeout` ([`Option<Duration>`](https://doc.rust-lang.org/std/time/struct.Duration.html)) — Timeout limit duration, or `None` to disable timeouts entirely.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use moonclient::{ClientHook, MoonClient};
    /// use std::time::Duration;
    ///
    /// # fn doc_example<H: ClientHook>(client: &MoonClient<H>) {
    /// client.set_timeout(Some(Duration::from_secs(15)));
    /// println!("Client timeout set to 15 seconds");
    /// # }
    /// ```
    pub fn set_timeout(&self, timeout: Option<Duration>) {
        let mut t = self.timeout.write().unwrap_or_else(|e| e.into_inner());
        *t = timeout;

        match timeout {
            Some(d) => log::debug!(
                "{}",
                crate::tr!("timeout-changed", "timeout" => format!("{:?}", d))
            ),
            None => log::debug!("{}", crate::tr!("timeout-disabled")),
        }
    }

    /// Applies the active timeout configuration to the target request builder.
    pub(crate) fn apply_timeout(&self, request: RequestBuilder) -> RequestBuilder {
        let timeout_setting = {
            let t = self.timeout.read().unwrap_or_else(|e| e.into_inner());
            *t
        };
        match timeout_setting {
            Some(duration) => request.timeout(duration),
            None => request,
        }
    }

    // =========================================================================
    // 🚦 RATE LIMIT MANAGEMENT
    // =========================================================================

    /// Appends or updates an active rate limiting rule.
    ///
    /// # Arguments
    ///
    /// * `requests` (`u32`) — Maximum number of allowed requests within the window.
    /// * `window` ([`Duration`](https://doc.rust-lang.org/std/time/struct.Duration.html)) — Duration window for the rate limit quota.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use moonclient::{ClientHook, MoonClient};
    /// use std::time::Duration;
    ///
    /// # fn doc_example<H: ClientHook>(client: &MoonClient<H>) {
    /// client.set_limit(5, Duration::from_secs(1));
    /// # }
    /// ```
    pub fn set_limit(&self, requests: u32, window: Duration) {
        self.limiter.set_limit(requests, window);
        log::info!(
            "{}",
            crate::tr!("limit-rule-changed", "requests" => requests, "window" => format!("{:?}", window))
        );
    }

    /// Configures a requests-per-second (RPS) rate limiting quota.
    ///
    /// Convenience helper equivalent to calling `client.set_limit(rps, Duration::from_secs(1))`.
    ///
    /// # Arguments
    ///
    /// * `rps` (`u32`) — Target requests-per-second ceiling.
    pub fn set_requests_per_second(&self, rps: u32) {
        self.set_limit(rps, Duration::from_secs(1));
    }

    /// Configures a requests-per-minute (RPM) rate limiting quota.
    ///
    /// Convenience helper equivalent to calling `client.set_limit(rpm, Duration::from_secs(60))`.
    ///
    /// # Arguments
    ///
    /// * `rpm` (`u32`) — Target requests-per-minute ceiling.
    pub fn set_requests_per_minute(&self, rpm: u32) {
        self.set_limit(rpm, Duration::from_secs(60));
    }

    /// Replaces all active rate limiting rules in bulk.
    ///
    /// Warns and skips rules with zero requests or zero duration.
    ///
    /// # Arguments
    ///
    /// * `limits` (`Vec<(u32, Duration)>`) — New batch of `(request_quota, window)` tuples.
    pub fn set_limits_bulk(&self, limits: Vec<(u32, Duration)>) {
        for &(count, duration) in &limits {
            if count == 0 || duration.is_zero() {
                log::warn!(
                    "{}",
                    crate::tr!("limit-invalid-ignored", "count" => count, "duration" => format!("{:?}", duration))
                );
            }
        }

        self.limiter.set_limits_bulk(limits);
        log::info!("{}", crate::tr!("limit-strategy-reset"));
    }

    /// Asynchronously waits until the active rate limiter permits outbound request dispatch.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` once transmission capacity is granted.
    pub async fn wait_for_limits(&self) -> Result<()> {
        self.limiter.wait().await
    }

    // =========================================================================
    // 🚀 CORE EXECUTION PIPELINE
    // =========================================================================

    /// Universal asynchronous execution pipeline with automatic response extraction.
    ///
    /// # Execution Lifecycle Order
    ///
    /// 1. Clones the request builder via `try_clone()` for potential 401 retry recovery.
    /// 2. Executes the pre-request hook ([`ClientHook::pre_request`]) to inject auth/metadata.
    /// 3. Attaches mandatory `User-Agent` and configured timeouts.
    /// 4. Awaits rate limiter slot clearance via [`MoonClient::wait_for_limits`].
    /// 5. Transmits request across the wire through the retry middleware pipeline.
    /// 6. If server responds with `401 Unauthorized`, calls [`ClientHook::handle_unauthorized`].
    ///    If recovery returns `true`, re-runs pre-request hook with fresh credentials and replays request.
    /// 7. Deserializes the payload through target extractor `T` ([`FromResponse`]).
    ///
    /// # Arguments
    ///
    /// * `request` ([`RequestBuilder`](https://docs.rs/reqwest-middleware/latest/reqwest_middleware/struct.RequestBuilder.html)) — Configured HTTP request builder ready for execution.
    ///
    /// # Returns
    ///
    /// Returns `Ok(T)` containing the deserialized response payload or extracted stream.
    ///
    /// # Errors
    ///
    /// * [`MoonError::Middleware`] — If pre-request or unauthorized recovery hooks fail.
    /// * [`MoonError::Network`] — If the underlying TCP/TLS transport drops or timeouts expire.
    /// * [`MoonError::ApiError`] — If the remote server responds with an unsuccessful HTTP status code.
    /// * [`MoonError::ResponseDecode`] — If the response body fails payload deserialization into `T`.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use moonclient::response::Json;
    /// # use moonclient::{ClientHook, MoonClient, Result};
    /// # use serde::Deserialize;
    /// # #[derive(Deserialize)] struct Item { id: u32 }
    /// # async fn doc_example<H: ClientHook>(client: &MoonClient<H>) -> Result<()> {
    /// let request = client.get("https://api.site.com/item/1");
    /// let Json(item): Json<Item> = client.execute(request).await?;
    /// println!("Item ID: {}", item.id);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn execute<T>(&self, mut request: RequestBuilder) -> Result<T>
    where
        T: FromResponse,
    {
        // Clone the request representation to permit replaying on HTTP 401 recovery
        let retry_request = request.try_clone();

        // Dispatch through pre-request lifecycle hook and isolate errors
        request = self
            .hook
            .pre_request(request)
            .await
            .map_err(|e| MoonError::Middleware(e.to_string()))?;

        // Attach mandatory client headers and enforce configured timeouts
        request = self.apply_user_agent(request);
        request = self.apply_timeout(request);

        // Extract endpoint URL string for preliminary dispatch diagnostics
        let request_url = request
            .try_clone()
            .and_then(|r| r.build().ok())
            .map(|r| r.url().to_string())
            .unwrap_or_else(|| "<unknown>".to_string());

        // Await rate limiter quota authorization
        self.wait_for_limits().await?;

        // Log request dispatch diagnostics
        log::debug!(
            "{}",
            crate::tr!("network-request-sending", "url" => request_url.clone())
        );

        // Transmit request across the wire
        let response = request.send().await?;

        // Log successful response receipt diagnostics
        log::debug!(
            "{}",
            crate::tr!("network-response-received", "url" => response.url().to_string())
        );

        let status = response.status();

        // Intercept HTTP 401 Unauthorized status
        if status == reqwest::StatusCode::UNAUTHORIZED {
            // Inquire if the lifecycle hook can refresh the session in the background
            let refreshed = self
                .hook
                .handle_unauthorized()
                .await
                .map_err(|e| MoonError::Middleware(e.to_string()))?;

            if refreshed {
                // Log localized session recovery and retry dispatch
                log::info!("{}", crate::tr!("request-retrying"));

                if let Some(mut retry) = retry_request {
                    // Re-run through pre-request hook to inject freshly acquired tokens
                    retry = self
                        .hook
                        .pre_request(retry)
                        .await
                        .map_err(|e| MoonError::Middleware(e.to_string()))?;
                    retry = self.apply_timeout(retry);

                    let retry_response = retry.send().await?;
                    return T::from_response(retry_response).await;
                }
            }
        }

        // Delegate payload extraction and deserialization to the extractor
        T::from_response(response).await
    }

    /// Low-level asynchronous binary stream download pipeline.
    ///
    /// Bypasses string deserializers and reads the wire response directly into an immutable [`Bytes`] buffer.
    ///
    /// # Arguments
    ///
    /// * `request` ([`RequestBuilder`](https://docs.rs/reqwest-middleware/latest/reqwest_middleware/struct.RequestBuilder.html)) — Configured request builder targeting the binary asset.
    ///
    /// # Returns
    ///
    /// Returns `Ok(Bytes)` containing the complete binary buffer in memory.
    ///
    /// # Errors
    ///
    /// * [`MoonError::ApiError`] — If the server returns a 4xx/5xx HTTP error status.
    /// * [`MoonError::Network`] — If the binary stream drops prematurely.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use moonclient::{ClientHook, MoonClient, Result};
    /// # async fn doc_example<H: ClientHook>(client: &MoonClient<H>) -> Result<()> {
    /// let request = client.get("https://site.com/image.png");
    /// let image_bytes = client.download(request).await?;
    /// println!("Downloaded {} bytes", image_bytes.len());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn download(&self, mut request: RequestBuilder) -> Result<Bytes> {
        request = self.apply_user_agent(request);
        self.wait_for_limits().await?;

        let response = request.send().await?;
        log::debug!(
            "{}",
            crate::tr!("network-download-started", "url" => response.url().to_string())
        );

        if response.status().is_client_error() || response.status().is_server_error() {
            return Err(MoonError::ApiError {
                code: response.status().as_u16(),
                message: response.status().to_string(),
            });
        }

        let bytes = response.bytes().await?;
        Ok(bytes)
    }

    /// Executes a network request and streams the binary payload directly into a local file.
    ///
    /// Automatically detects and recursively creates any missing parent directories on disk.
    ///
    /// # Arguments
    ///
    /// * `request` ([`RequestBuilder`](https://docs.rs/reqwest-middleware/latest/reqwest_middleware/struct.RequestBuilder.html)) — Configured request targeting the binary asset.
    /// * `path` (`impl AsRef<`[`Path`](https://doc.rust-lang.org/std/path/struct.Path.html)`>`) — Local destination file path.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` upon successful disk persistence.
    ///
    /// # Errors
    ///
    /// Emits [`std::io::Error`](https://doc.rust-lang.org/std/io/struct.Error.html) if directory creation or file writing fails.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use moonclient::{ClientHook, MoonClient, Result};
    /// # async fn doc_example<H: ClientHook>(client: &MoonClient<H>) -> Result<()> {
    /// let request = client.get("https://site.com/archive.tar.gz");
    /// client.save_file(request, "downloads/archive.tar.gz").await?;
    /// println!("File saved to disk successfully!");
    /// # Ok(())
    /// # }
    /// ```
    pub async fn save_file(&self, request: RequestBuilder, path: impl AsRef<Path>) -> Result<()> {
        let bytes = self.download(request).await?;

        if let Some(parent) = path.as_ref().parent() {
            if !parent.exists() {
                tokio::fs::create_dir_all(parent).await?;
            }
        }

        let mut file = tokio::fs::File::create(path).await?;
        file.write_all(&bytes).await?;

        Ok(())
    }
}

// =============================================================================
// 🏗️ BUILDER IMPLEMENTATION
// =============================================================================

/// Fluent builder pipeline for configuring and instantiating a [`MoonClient<H>`].
pub struct MoonClientBuilder<H: ClientHook> {
    /// Base API URL to which all relative endpoint paths are anchored.
    base_url: String,

    /// Unique client identifier sent via the `User-Agent` HTTP header.
    user_agent: String,

    /// Collection of rate limiting rules represented as `(request_quota, duration_window)` tuples.
    limits: Vec<(u32, Duration)>,

    /// Maximum number of automatic exponential backoff retries on transient network errors.
    max_retries: u32,

    /// Optional client-wide HTTP request timeout duration.
    timeout: Option<Duration>,

    /// Optional custom or distributed rate limiter provided by the caller.
    custom_limiter: Option<Arc<dyn crate::limiter::RequestLimiter>>,

    /// Maximum response payload body capacity limit in bytes (defaults to 16 MiB).
    max_response_size: usize,

    /// Pluggable request lifecycle hook implementation for domain-specific API logic.
    hook: H,
}

impl<H: ClientHook> MoonClientBuilder<H> {
    /// Initializes a new client builder bound to a specific API lifecycle hook.
    ///
    /// By default, initializes with:
    /// * Baseline rate limits: `5 RPS` (1s window) and `90 RPM` (60s window).
    /// * Timeout: `60 seconds`.
    /// * Max retries: `3` attempts with exponential backoff.
    /// * Max response buffer: `16 MiB`.
    ///
    /// # Arguments
    ///
    /// * `hook` (`H`) — Domain lifecycle hook instance.
    pub fn new(hook: H) -> Self {
        Self {
            base_url: DEFAULT_API_URL.to_string(),
            user_agent: DEFAULT_APP_NAME.to_string(),
            limits: vec![
                (5, Duration::from_secs(1)),   // Default baseline limit: 5 RPS
                (90, Duration::from_secs(60)), // Default baseline limit: 90 RPM
            ],
            max_retries: 3,
            timeout: Some(Duration::from_secs(60)),
            custom_limiter: None,
            max_response_size: DEFAULT_MAX_RESPONSE_SIZE,
            hook,
        }
    }

    /// Sets the base API endpoint URL.
    ///
    /// Trailing slashes are automatically stripped during normalization.
    ///
    /// # Arguments
    ///
    /// * `base_url` (`impl Into<String>`) — Base endpoint string (e.g. `"https://shikimori.one"`).
    pub fn with_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.base_url = base_url.into();
        self
    }

    /// Sets the client application name for the `User-Agent` header.
    ///
    /// # Arguments
    ///
    /// * `app_name` (`impl Into<String>`) — Application name string.
    pub fn with_app_name(mut self, app_name: impl Into<String>) -> Self {
        self.user_agent = app_name.into();
        self
    }

    /// Sets the maximum number of automated exponential backoff retries on transient network faults.
    ///
    /// # Arguments
    ///
    /// * `retries` (`u32`) — Maximum retry attempts count.
    pub fn with_max_retries(mut self, retries: u32) -> Self {
        self.max_retries = retries;
        self
    }

    /// Configures a client-wide timeout duration for all outbound network transactions.
    ///
    /// # Arguments
    ///
    /// * `timeout` ([`Duration`](https://doc.rust-lang.org/std/time/struct.Duration.html)) — Maximum duration allowed per request.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Completely disables network timeouts (infinite wait duration).
    pub fn with_infinite_timeout(mut self) -> Self {
        self.timeout = None;
        self
    }

    /// Clears all default baseline rate limiting rules.
    pub fn clear_limits(mut self) -> Self {
        self.limits.clear();
        self
    }

    /// Appends or updates a single rate limiting quota rule.
    ///
    /// # Arguments
    ///
    /// * `requests` (`u32`) — Maximum requests allowed.
    /// * `window` ([`Duration`](https://doc.rust-lang.org/std/time/struct.Duration.html)) — Rolling time window duration.
    pub fn add_limit(mut self, requests: u32, window: Duration) -> Self {
        self.limits.retain(|(_, d)| *d != window);
        if requests > 0 && !window.is_zero() {
            self.limits.push((requests, window));
        }
        self
    }

    /// Configures a requests-per-second (RPS) rate limiting rule.
    ///
    /// # Arguments
    ///
    /// * `rps` (`u32`) — Target requests per second quota.
    pub fn requests_per_second(self, rps: u32) -> Self {
        self.add_limit(rps, Duration::from_secs(1))
    }

    /// Configures a requests-per-minute (RPM) rate limiting rule.
    ///
    /// # Arguments
    ///
    /// * `rpm` (`u32`) — Target requests per minute quota.
    pub fn requests_per_minute(self, rpm: u32) -> Self {
        self.add_limit(rpm, Duration::from_secs(60))
    }

    /// Supplies a custom or distributed rate limiter implementation (e.g. Redis).
    ///
    /// # Arguments
    ///
    /// * `limiter` (`impl` [`RequestLimiter`](crate::limiter::RequestLimiter)) — Custom rate limiter instance.
    pub fn with_custom_limiter(mut self, limiter: impl crate::limiter::RequestLimiter) -> Self {
        self.custom_limiter = Some(Arc::new(limiter));
        self
    }

    /// Configures a custom maximum allowable response body capacity threshold in bytes.
    ///
    /// # Arguments
    ///
    /// * `bytes` (`usize`) — Maximum allowable response body buffer limit in bytes.
    pub fn with_max_response_size(mut self, bytes: usize) -> Self {
        self.max_response_size = bytes;
        self
    }

    /// Builds and instantiates a fully initialized, thread-safe [`MoonClient<H>`].
    ///
    /// # Panics
    ///
    /// Panics if the configured base URL cannot be parsed or represents a non-hierarchical scheme
    /// (e.g. `mailto:` or `data:`) that cannot serve as an HTTP endpoint base.
    ///
    /// # Returns
    ///
    /// A configured [`MoonClient<H>`] ready for production network execution.
    pub fn build(self) -> MoonClient<H> {
        let retry_policy = ExponentialBackoff::builder().build_with_max_retries(self.max_retries);

        // Initialize the root reqwest client once
        let client_inner = Client::new();

        let http = ClientBuilder::new(client_inner.clone())
            .with(MoonRetryMiddleware::new_with_policy(retry_policy))
            .build();

        let parsed_url = url::Url::parse(self.base_url.trim_end_matches('/'))
            .expect(&crate::tr!("expect-invalid-base-url"));

        // Formal invariant check: reject non-hierarchical schemes (e.g., mailto: or data:)
        if parsed_url.cannot_be_a_base() {
            panic!("{}", crate::tr!("expect-invalid-base-url"));
        }

        // RESOLVE RATE LIMITING STRATEGY:
        // Prefer custom or distributed rate limiter if supplied by the caller (e.g., RedisLimiter).
        // Fall back to default in-memory token bucket limiter otherwise.
        let limiter: Arc<dyn crate::limiter::RequestLimiter> = match self.custom_limiter {
            Some(custom) => custom,
            None => {
                // Pre-allocate vector capacity to eliminate redundant heap re-allocations
                let mut strategy_limiters = Vec::with_capacity(self.limits.len());
                for &(count, duration) in &self.limits {
                    let limiter = create_limiter(count, duration);
                    strategy_limiters.push((duration, limiter));
                }
                let strategy = RateLimitStrategy {
                    limiters: Arc::new(strategy_limiters),
                };
                Arc::new(crate::limiter::InMemoryLimiter::new(strategy))
            }
        };

        MoonClient {
            http,
            inner: client_inner,
            base_url: parsed_url,
            user_agent: self.user_agent,
            timeout: Arc::new(std::sync::RwLock::new(self.timeout)),
            limiter,
            max_response_size: Arc::new(AtomicUsize::new(self.max_response_size)),
            hook: Arc::new(self.hook),
        }
    }
}

// =============================================================================
// 🔁 HYBRID RETRY MIDDLEWARE
// =============================================================================

/// Custom hybrid retry middleware tailored for the [`MoonClient`] engine.
///
/// Exposes a generic interface 100% compatible with [`RetryTransientMiddleware`](https://docs.rs/reqwest-retry/latest/reqwest_retry/struct.RetryTransientMiddleware.html).
/// Seamlessly manages transient retry policies for standard requests, while
/// transparently passing through non-cloneable requests (e.g., streaming multipart file uploads)
/// down the middleware pipeline without raising 'Request object is not cloneable' panics.
pub struct MoonRetryMiddleware<
    T: RetryPolicy + Send + Sync + 'static,
    R: RetryableStrategy + Send + Sync + 'static = DefaultRetryableStrategy,
> {
    inner: RetryTransientMiddleware<T, R>,
}

impl<T: RetryPolicy + Send + Sync + 'static> MoonRetryMiddleware<T, DefaultRetryableStrategy> {
    /// Instantiates `MoonRetryMiddleware` configured with a specified retry policy.
    ///
    /// # Arguments
    ///
    /// * `retry_policy` (`T`) — Retry policy strategy implementing [`RetryPolicy`](https://docs.rs/reqwest-retry/latest/reqwest_retry/trait.RetryPolicy.html).
    pub fn new_with_policy(retry_policy: T) -> Self {
        Self {
            inner: RetryTransientMiddleware::new_with_policy(retry_policy),
        }
    }
}

impl<T, R> MoonRetryMiddleware<T, R>
where
    T: RetryPolicy + Send + Sync + 'static,
    R: RetryableStrategy + Send + Sync + 'static,
{
    /// Instantiates `MoonRetryMiddleware` with a retry policy and custom error classification strategy.
    ///
    /// # Arguments
    ///
    /// * `retry_policy` (`T`) — Retry policy strategy.
    /// * `retryable_strategy` (`R`) — Strategy determining if an HTTP error or status is retryable.
    pub fn new_with_policy_and_strategy(retry_policy: T, retryable_strategy: R) -> Self {
        Self {
            inner: RetryTransientMiddleware::new_with_policy_and_strategy(
                retry_policy,
                retryable_strategy,
            ),
        }
    }
}

#[async_trait]
impl<T, R> Middleware for MoonRetryMiddleware<T, R>
where
    T: RetryPolicy + Send + Sync + 'static,
    R: RetryableStrategy + Send + Sync + 'static,
{
    /// Handles outbound requests, safely bypassing non-cloneable payloads around the retry loop.
    async fn handle(
        &self,
        req: reqwest::Request,
        extensions: &mut http::Extensions,
        next: Next<'_>,
    ) -> MiddlewareResult<reqwest::Response> {
        // If the request payload is non-cloneable (e.g., streaming multipart upload):
        if req.try_clone().is_none() {
            // Forward directly down the middleware pipeline without attempting retries or panicking
            return next.run(req, extensions).await;
        }

        // Delegate standard cloneable requests to the retry middleware
        self.inner.handle(req, extensions, next).await
    }
}

/// Internal helper function to instantiate a `governor` direct rate limiter.
///
/// # Arguments
///
/// * `count` (`u32`) — Maximum number of permits allowed per time window.
/// * `duration` ([`Duration`](https://doc.rust-lang.org/std/time/struct.Duration.html)) — Rolling time window duration.
pub(crate) fn create_limiter(count: u32, duration: Duration) -> Arc<ClientRateLimiter> {
    let count_nz = NonZeroU32::new(count).expect(&crate::tr!("expect-limit-greater-than-zero"));

    let quota = if duration.as_secs() == 1 {
        Quota::per_second(count_nz)
    } else if duration.as_secs() == 60 {
        Quota::per_minute(count_nz)
    } else {
        let total_nanos = duration.as_nanos();
        let nanos_per_token = total_nanos / (count as u128);
        let period = Duration::from_nanos(nanos_per_token as u64);

        Quota::with_period(period)
            .expect(&crate::tr!("expect-invalid-period-calculation"))
            .allow_burst(count_nz)
    };
    Arc::new(RateLimiter::direct(quota))
}
