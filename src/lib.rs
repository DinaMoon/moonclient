//! # MoonClient 🌙
//!
//! An enterprise-grade, resilient, and middleware-driven HTTP client framework engineered
//! for high-concurrency API integrations, robust web scrapers, and mission-critical microservices.
//!
//! ---
//!
//! ## 🚀 Quick Start
//!
//! Here is a complete, copy-paste-ready example demonstrating how to initialize [`MoonClient`],
//! configure automatic rate limiting, construct endpoint URLs using [`build_url!`], and deserialize
//! typed JSON payloads:
//!
//! ```rust,no_run
//! use moonclient::response::Json;
//! use moonclient::{build_url, ClientHook, MoonClient, Result};
//! use serde::Deserialize;
//! use std::time::Duration;
//!
//! #[derive(Debug, Deserialize)]
//! struct UserProfile {
//!     id: u32,
//!     username: String,
//!     is_active: bool,
//! }
//!
//! // 1. Define a minimal lifecycle hook (or use an empty unit hook)
//! struct DefaultHook;
//!
//! #[async_trait::async_trait]
//! impl ClientHook for DefaultHook {
//!     type Error = std::convert::Infallible;
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     // 2. Build the client with timeouts, rate limits, and hooks
//!     let client = MoonClient::builder(DefaultHook)
//!         .with_base_url("https://api.example.com/v1")
//!         .with_app_name("MyProductionApp/1.0")
//!         .with_timeout(Duration::from_secs(10))
//!         .requests_per_second(5) // Autonomous 5 RPS Token Bucket
//!         .build();
//!
//!     // 3. Construct the endpoint safely with zero heap allocations
//!     let user_id = 42_u32;
//!     let endpoint = build_url!(client, "users", user_id, "profile")?;
//!
//!     // 4. Dispatch request and extract typed JSON in one expression
//!     let request = client.get(endpoint);
//!     let Json(profile): Json<UserProfile> = client.execute(request).await?;
//!
//!     println!("Successfully loaded user: {} (ID: {})", profile.username, profile.id);
//!     Ok(())
//! }
//! ```
//!
//! ---
//!
//! ## 🏛️ Architectural Pillars
//!
//! - **Middleware & Exponential Retries:** Built-in fault tolerance powered by
//!   [`reqwest-retry`](https://docs.rs/reqwest-retry) and [`reqwest-tracing`](https://docs.rs/reqwest-tracing).
//!   Features a hybrid retry middleware that transparently passes through non-cloneable streaming
//!   multipart file uploads without panics.
//! - **Autonomous Traffic Shaping (Rate Limiting):** Multi-window Token Bucket rate limiting
//!   powered by [`governor`](https://docs.rs/governor). Supports granular millisecond-level time windows
//!   ([`std::time::Duration`]), pre-configured RPS/RPM quotas, and optional distributed coordination
//!   via [`redis`](https://docs.rs/redis) connection pools.
//! - **Pluggable Lifecycle Hooks:** Interceptor architecture via the [`ClientHook`] trait,
//!   enabling asynchronous pre-request header injection (e.g., Bearer tokens, dynamic HMAC signatures)
//!   and reactive `401 Unauthorized` token renewal with automatic request replaying.
//! - **Memory Bomb Protection:** Bounded response buffer decoders safeguarding host processes
//!   against memory exhaustion attacks (unbounded streams or multi-gigabyte error payloads).
//! - **Extractor Pattern:** Unified, strongly typed `.execute()` pipeline automatically deserializing
//!   responses into `Json<T>`, `Xml<T>`, streaming `Response`, or empty `()` (void) unit types.
//! - **Zero-Allocation Routing:** High-performance [`build_url!`] macro converting arbitrary primitives
//!   and domain enums into safe URL paths via copy-on-write strings ([`std::borrow::Cow<'_, str>`]).
//! - **Project Fluent i18n:** Native multi-language diagnostic engine powered by
//!   [`fluent`](https://docs.rs/fluent), supporting thread-safe [`tokio`](https://docs.rs/tokio)
//!   Task-Local contexts and extensible external bundle registries.
//!
//! ---
//!
//! ## 🏗️ Pipeline Execution Flow
//!
//! Every outbound HTTP request dispatched through [`MoonClient::execute`] traverses an opinionated,
//! defensive pipeline designed to guarantee safety under extreme load:
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────┐
//! │                           MoonClient::execute(req)                          │
//! └──────────────────────────────────────┬──────────────────────────────────────┘
//!                                        │
//!                                        ▼
//! ┌─────────────────────────────────────────────────────────────────────────────┐
//! │ 1. Rate Limiting Gate (Token Bucket via governor or distributed Redis)      │
//! └──────────────────────────────────────┬──────────────────────────────────────┘
//!                                        │
//!                                        ▼
//! ┌─────────────────────────────────────────────────────────────────────────────┐
//! │ 2. Pre-Request Lifecycle Hook (ClientHook::pre_request - Auth / HMAC)       │
//! └──────────────────────────────────────┬──────────────────────────────────────┘
//!                                        │
//!                                        ▼
//! ┌─────────────────────────────────────────────────────────────────────────────┐
//! │ 3. Resilient Middleware Pipeline (reqwest-retry + reqwest-tracing)          │
//! │    • Transparent retry with exponential backoff on transient 5xx / drops    │
//! │    • Safe bypass for streaming multipart uploads (Zero-Panic Guarantee)     │
//! └──────────────────────────────────────┬──────────────────────────────────────┘
//!                                        │
//!                                        ▼
//! ┌─────────────────────────────────────────────────────────────────────────────┐
//! │ 4. Outbound Wire Transmission (reqwest + rustls)                            │
//! └──────────────────────────────────────┬──────────────────────────────────────┘
//!                                        │
//!                                        ▼
//! ┌─────────────────────────────────────────────────────────────────────────────┐
//! │ 5. Post-Request Interceptor & Reactive Auth Recovery                        │
//! │    • Intercepts 401 Unauthorized -> ClientHook::handle_unauthorized()       │
//! │    • Seamless background token refresh & automatic request replay           │
//! └──────────────────────────────────────┬──────────────────────────────────────┘
//!                                        │
//!                                        ▼
//! ┌─────────────────────────────────────────────────────────────────────────────┐
//! │ 6. OOM-Safe Bounded Buffer Decoders (Max response capacity guards)          │
//! └──────────────────────────────────────┬──────────────────────────────────────┘
//!                                        │
//!                                        ▼
//! ┌─────────────────────────────────────────────────────────────────────────────┐
//! │ 7. Extractor Pattern (FromResponse) -> Json<T>, Xml<T>, Response, ()        │
//! └─────────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ---
//!
//! ## 🧩 Feature Flags
//!
//! | Feature | Description | Dependencies | Default |
//! | :--- | :--- | :--- | :---: |
//! | `xml` | Enables strongly typed XML response deserialization via [`quick-xml`](https://docs.rs/quick-xml). | `quick-xml` | **Disabled** |
//! | `redis-limit` | Enables distributed multi-pod rate limiting synchronized via [`redis`](https://docs.rs/redis) connection pools. | `redis` | **Disabled** |

pub mod client;
pub mod error;
pub mod hooks;
pub mod i18n;
pub mod limiter;
pub mod logger;
pub mod response;

// Public re-exports of core engine types
pub use client::{MoonClient, MoonClientBuilder};
pub use error::{MoonError, Result};
pub use hooks::ClientHook;
pub use limiter::{InMemoryLimiter, RequestLimiter};
pub use response::FromResponse;

// Direct re-exports of underlying network primitives for downstream consumers
pub use reqwest;
pub use reqwest_middleware;
pub use reqwest_tracing;

#[cfg(feature = "redis-limit")]
pub use limiter::RedisLimiter;

// =============================================================================
// 🧭 ZERO-ALLOCATION ROUTING PRIMITIVES
// =============================================================================

/// Trait for automatically coercing arbitrary types into safe URL path segments.
///
/// This trait powers the [`build_url!`] macro. It uses the copy-on-write abstraction
/// [`std::borrow::Cow<'_, str>`] to guarantee **zero redundant heap allocations**
/// for borrowed string literals and slices (`&str`), while seamlessly allocating owned strings
/// only when necessary (e.g. formatting numeric identifiers like `u32` or `i64`).
///
/// # Implementing for Custom Domain Types
///
/// You can implement this trait for your own domain types (such as custom IDs, entity models,
/// or API routing enums) to pass them directly into [`build_url!`]:
///
/// ```rust
/// use moonclient::{build_url, IntoSegment};
/// use std::borrow::Cow;
///
/// /// Domain routing enumeration
/// enum ApiRoute {
///     Users,
///     Posts,
///     Comments,
/// }
///
/// impl IntoSegment for ApiRoute {
///     fn to_segment(&self) -> Cow<'_, str> {
///         match self {
///             Self::Users => Cow::Borrowed("users"),
///             Self::Posts => Cow::Borrowed("posts"),
///             Self::Comments => Cow::Borrowed("comments"),
///         }
///     }
/// }
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # struct MockClient { base_url: url::Url }
/// # impl MockClient { fn base_url(&self) -> &url::Url { &self.base_url } }
/// # let client = MockClient { base_url: url::Url::parse("https://api.test.com")? };
/// let route = ApiRoute::Users;
/// let user_id = 940032_u32;
///
/// // Notice how both `route` and `user_id` are passed without manual `.to_string()` calls:
/// let endpoint = build_url!(client, route, user_id)?;
/// assert_eq!(endpoint.as_str(), "https://api.test.com/users/940032");
/// # Ok(())
/// # }
/// ```
pub trait IntoSegment {
    /// Converts the reference into a borrowed or owned URL path segment string.
    fn to_segment(&self) -> std::borrow::Cow<'_, str>;
}

impl<'a> IntoSegment for &'a str {
    #[inline]
    fn to_segment(&self) -> std::borrow::Cow<'_, str> {
        std::borrow::Cow::Borrowed(self)
    }
}

impl IntoSegment for String {
    #[inline]
    fn to_segment(&self) -> std::borrow::Cow<'_, str> {
        std::borrow::Cow::Borrowed(self.as_str())
    }
}

impl IntoSegment for i32 {
    #[inline]
    fn to_segment(&self) -> std::borrow::Cow<'_, str> {
        std::borrow::Cow::Owned(self.to_string())
    }
}

impl IntoSegment for u32 {
    #[inline]
    fn to_segment(&self) -> std::borrow::Cow<'_, str> {
        std::borrow::Cow::Owned(self.to_string())
    }
}

impl IntoSegment for i64 {
    #[inline]
    fn to_segment(&self) -> std::borrow::Cow<'_, str> {
        std::borrow::Cow::Owned(self.to_string())
    }
}

impl IntoSegment for u64 {
    #[inline]
    fn to_segment(&self) -> std::borrow::Cow<'_, str> {
        std::borrow::Cow::Owned(self.to_string())
    }
}

impl<'a, T: IntoSegment + ?Sized> IntoSegment for &'a T {
    #[inline]
    fn to_segment(&self) -> std::borrow::Cow<'_, str> {
        (*self).to_segment()
    }
}

/// Global macro for safe, declarative, and zero-allocation construction of endpoint URLs.
///
/// Polymorphically converts any combination of string slices, numeric IDs, primitives,
/// and custom domain enums implementing [`IntoSegment`] into proper hierarchical URL path segments.
///
/// # Advantages over `format!("{}/foo/{}", base, id)`
///
/// 1. **Zero Path Concatenation Bugs:** Automatically handles leading and trailing slashes (`/`),
///    preventing duplicate slashes (`//`) or accidental host overrides.
/// 2. **Zero-Allocation Routing:** String slices are borrowed directly into the URL path buffer
///    via [`std::borrow::Cow::Borrowed`].
/// 3. **Non-Panicking Validation:** Completely avoids runtime panics. If the client base URL cannot
///    act as a base, it returns an explicit, localized [`crate::error::Result<url::Url>`].
///
/// # Arguments
///
/// * `$client` - An instance or reference to any client structure exposing a `.base_url() -> &url::Url` method (such as [`MoonClient`]).
/// * `$($segment)*` - One or more comma-separated path segments implementing [`IntoSegment`].
///
/// # Errors
///
/// Returns [`crate::error::MoonError::Config`] if the underlying base URL has a non-hierarchical scheme
/// (e.g. `data:`, `mailto:`) that cannot support path segments.
///
/// # Practical Examples
///
/// ### Combining Strings, Integers, and Sub-routes
/// ```rust
/// use moonclient::build_url;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # struct MockClient { base_url: url::Url }
/// # impl MockClient { fn base_url(&self) -> &url::Url { &self.base_url } }
/// # let client = MockClient { base_url: url::Url::parse("https://api.site.com/api/v2")? };
/// let target_user = 1007_u32;
/// let action = "deactivate";
///
/// // Result: "https://api.site.com/api/v2/users/1007/deactivate"
/// let url = build_url!(client, "users", target_user, action)?;
/// assert_eq!(url.as_str(), "https://api.site.com/api/v2/users/1007/deactivate");
/// # Ok(())
/// # }
/// ```
///
/// ### Safe Query Parameter Chaining
/// The returned [`url::Url`] can be immediately mutated or chained with query parameters:
/// ```rust
/// use moonclient::build_url;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # struct MockClient { base_url: url::Url }
/// # impl MockClient { fn base_url(&self) -> &url::Url { &self.base_url } }
/// # let client = MockClient { base_url: url::Url::parse("https://api.site.com")? };
/// let mut url = build_url!(client, "search")?;
/// url.query_pairs_mut().append_pair("q", "rustlang");
///
/// assert_eq!(url.as_str(), "https://api.site.com/search?q=rustlang");
/// # Ok(())
/// # }
/// ```
#[macro_export]
macro_rules! build_url {
    ($client:expr, $($segment:expr),+ $(,)?) => {{
        use $crate::IntoSegment;
        (|| -> $crate::error::Result<url::Url> {
            let mut url = $client.base_url().clone();
            {
                let mut segments = url
                    .path_segments_mut()
                    .map_err(|_| $crate::error::MoonError::Config($crate::tr!("expect-invalid-macro-base-url")))?;
                $(
                    segments.push(&$segment.to_segment());
                )*
            }
            Ok(url)
        })()
    }};
}
