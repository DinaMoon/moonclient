//! Pluggable lifecycle hooks and interceptors for network transaction pipelines.
//!
//! This module defines the [`ClientHook`] trait, which provides a flexible extension
//! mechanism for [`MoonClient`](crate::client::MoonClient). It allows downstream consumers
//! to inject custom authentication protocols, dynamic headers, telemetry, or reactive
//! error recovery without modifying the core network engine.
//!
//! ### Interception Lifecycle Timeline
//!
//! ```text
//! [Outbound Request]
//!         │
//!         ▼
//! 1. ClientHook::pre_request()        <-- Inject Bearer/HMAC, metadata, query params
//!         │
//!         ▼
//! [Wire Dispatch & Middleware Retry]
//!         │
//!         ▼
//! 2. ClientHook::post_request()       <-- Inspect status, audit rate-limit headers
//!         │
//!         ├─► If HTTP 401 Unauthorized:
//!         │   ClientHook::handle_unauthorized()
//!         │       ├─► Ok(true)  --> Replay original request through pipeline
//!         │       └─► Ok(false) --> Forward 401 error to caller
//!         │
//!         ▼
//! [Response Payload Extractor]
//! ```

use async_trait::async_trait;
use reqwest::Response;
use reqwest_middleware::RequestBuilder;

/// Interceptor contract for augmenting the HTTP request and response lifecycle.
///
/// Implementors of this trait can seamlessly integrate domain-specific logic,
/// such as appending authorization tokens, logging diagnostic metadata, refreshing
/// expired OAuth sessions, or translating proprietary vendor error codes.
///
/// All trait methods provide default no-op implementations, eliminating boilerplate
/// for read-only or unauthenticated APIs (such as public boorus or scrapers).
///
/// # Concurrency & Thread Safety
///
/// The trait requires implementors to be [`Send`], [`Sync`], and satisfy a `'static` lifetime,
/// allowing the hook instance to be safely shared across concurrent asynchronous tasks inside an [`std::sync::Arc`].
///
/// # Example: Bearer Token Authorization Hook
///
/// ```rust
/// use async_trait::async_trait;
/// use moonclient::ClientHook;
/// use reqwest::Response;
/// use reqwest_middleware::RequestBuilder;
/// use std::sync::atomic::{AtomicU32, Ordering};
/// use std::sync::Arc;
///
/// /// A realistic authentication hook injecting Bearer tokens and auditing traffic.
/// #[derive(Clone)]
/// pub struct BearerAuthHook {
///     token: String,
///     request_counter: Arc<AtomicU32>,
/// }
///
/// impl BearerAuthHook {
///     pub fn new(token: impl Into<String>) -> Self {
///         Self {
///             token: token.into(),
///             request_counter: Arc::new(AtomicU32::new(0)),
///         }
///     }
///
///     pub fn total_dispatches(&self) -> u32 {
///         self.request_counter.load(Ordering::Relaxed)
///     }
/// }
///
/// #[async_trait]
/// impl ClientHook for BearerAuthHook {
///     type Error = std::convert::Infallible;
///
///     async fn pre_request(&self, request: RequestBuilder) -> Result<RequestBuilder, Self::Error> {
///         let current_count = self.request_counter.fetch_add(1, Ordering::SeqCst) + 1;
///         println!("🚀 Preparing request #{} with authorization header", current_count);
///
///         // Inject authorization and diagnostic tracking header
///         Ok(request
///             .header("Authorization", format!("Bearer {}", self.token))
///             .header("X-Dispatch-Index", current_count.to_string()))
///     }
///
///     async fn post_request(&self, response: Response) -> Result<Response, Self::Error> {
///         println!("📥 Received wire response: status {}", response.status());
///         Ok(response)
///     }
/// }
/// ```
#[async_trait]
pub trait ClientHook: Send + Sync + 'static {
    /// Domain-specific error type that can be emitted during hook execution.
    ///
    /// Must implement standard error traits ([`std::error::Error`]) and be safe
    /// for multi-threaded transfer ([`Send`] + [`Sync`]).
    ///
    /// If your hook cannot fail (for instance, simple header appending), use
    /// [`std::convert::Infallible`] as the error type to guarantee zero failure branches.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Interceptor executed immediately **before** dispatching the request to the network.
    ///
    /// Allows mutating the underlying [`RequestBuilder`] on the fly:
    /// appending authorization headers, calculating dynamic cryptographic HMAC signatures,
    /// adding query parameters, or rewriting target endpoint paths.
    ///
    /// # Arguments
    ///
    /// * `request` ([`RequestBuilder`]) — The mutable request builder instance prepared for execution.
    ///
    /// # Returns
    ///
    /// Returns [`Ok`]`(`[`RequestBuilder`]`)` containing the enriched request ready for wire dispatch.
    /// Returns [`Err`]`(`[`Self::Error`]`)` if signature computation or authorization prerequisites fail,
    /// immediately aborting the request before touching the network.
    ///
    /// # Errors
    ///
    /// Emits [`Self::Error`] if local authorization generation fails (e.g. key derivation errors).
    ///
    /// # Example
    ///
    /// ```rust
    /// use async_trait::async_trait;
    /// use moonclient::ClientHook;
    /// use reqwest_middleware::RequestBuilder;
    ///
    /// struct ApiKeyHook {
    ///     api_key: String,
    /// }
    ///
    /// #[async_trait]
    /// impl ClientHook for ApiKeyHook {
    ///     type Error = std::convert::Infallible;
    ///
    ///     async fn pre_request(&self, request: RequestBuilder) -> Result<RequestBuilder, Self::Error> {
    ///         // Append custom static vendor header
    ///         let enriched_request = request.header("X-API-Key", &self.api_key);
    ///         println!("🔑 Appended X-API-Key header to outbound request");
    ///         Ok(enriched_request)
    ///     }
    /// }
    /// ```
    ///
    /// [`RequestBuilder`]: https://docs.rs/reqwest-middleware/latest/reqwest_middleware/struct.RequestBuilder.html
    async fn pre_request(&self, request: RequestBuilder) -> Result<RequestBuilder, Self::Error> {
        Ok(request)
    }

    /// Interceptor executed immediately **after** receiving a raw response, prior to deserialization.
    ///
    /// Provides low-level inspection of the raw [`Response`]: auditing incoming HTTP
    /// status codes, inspecting custom rate-limiting headers (e.g., `X-RateLimit-Remaining`),
    /// or registering timing telemetry.
    ///
    /// # Arguments
    ///
    /// * `response` ([`Response`]) — The raw response object received from the remote server.
    ///
    /// # Returns
    ///
    /// Returns [`Ok`]`(`[`Response`]`)` passing the original or modified response downstream to the extractor pipeline.
    /// Returns [`Err`]`(`[`Self::Error`]`)` if response validation fails at the protocol layer.
    ///
    /// # Errors
    ///
    /// Emits [`Self::Error`] if custom validation rules reject the received HTTP envelope.
    ///
    /// # Example
    ///
    /// ```rust
    /// use async_trait::async_trait;
    /// use moonclient::ClientHook;
    /// use reqwest::Response;
    ///
    /// struct RateLimitAuditorHook;
    ///
    /// #[async_trait]
    /// impl ClientHook for RateLimitAuditorHook {
    ///     type Error = std::convert::Infallible;
    ///
    ///     async fn post_request(&self, response: Response) -> Result<Response, Self::Error> {
    ///         if let Some(remaining) = response.headers().get("x-ratelimit-remaining") {
    ///             println!("⚡ Upstream API rate limit remaining: {:?}", remaining);
    ///         }
    ///         Ok(response)
    ///     }
    /// }
    /// ```
    ///
    /// [`Response`]: https://docs.rs/reqwest/latest/reqwest/struct.Response.html
    async fn post_request(&self, response: Response) -> Result<Response, Self::Error> {
        Ok(response)
    }

    /// Reactive authorization recovery handler invoked upon receiving an HTTP `401 Unauthorized`.
    ///
    /// When the remote server rejects a request with HTTP 401, [`MoonClient`](crate::client::MoonClient)
    /// intercepts the error and calls this method. The hook can perform an asynchronous token refresh
    /// cycle (e.g., exchanging a refresh token) and instruct the client to automatically replay
    /// the original request with the renewed credentials.
    ///
    /// # Returns
    ///
    /// * `Ok(true)` - The session was successfully refreshed; the engine will re-execute the request.
    /// * `Ok(false)` - Automatic recovery is disabled or unavailable; the engine will return the `401` error.
    ///
    /// # Errors
    ///
    /// Emits [`Self::Error`] if the token refresh network call fails or persistent storage cannot be updated.
    ///
    /// # Example
    ///
    /// ```rust
    /// use async_trait::async_trait;
    /// use moonclient::ClientHook;
    /// use std::sync::Arc;
    /// use tokio::sync::RwLock;
    ///
    /// struct OAuth2Hook {
    ///     token: Arc<RwLock<String>>,
    /// }
    ///
    /// #[async_trait]
    /// impl ClientHook for OAuth2Hook {
    ///     type Error = std::convert::Infallible;
    ///
    ///     async fn handle_unauthorized(&self) -> Result<bool, Self::Error> {
    ///         println!("🔄 Encountered HTTP 401! Triggering background token refresh...");
    ///
    ///         let mut token_guard = self.token.write().await;
    ///         *token_guard = "new_refreshed_access_token_xyz".to_string();
    ///
    ///         println!("✅ Token successfully refreshed, notifying client to replay request");
    ///         Ok(true) // Replay the request with the new token
    ///     }
    /// }
    /// ```
    async fn handle_unauthorized(&self) -> Result<bool, Self::Error> {
        Ok(false)
    }
}
