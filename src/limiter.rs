//! Abstractions and implementations of outbound network traffic shaping and rate limiting.
//!
//! High-throughput API consumers and distributed scrapers must respect remote server rate limits
//! to avoid HTTP `429 Too Many Requests` bans. This module provides a swappable rate limiting
//! subsystem decoupled from transport mechanics.
//!
//! ### Supported Strategies
//!
//! 1. **Local In-Memory ([`InMemoryLimiter`]):**
//!    Powered by the Token Bucket algorithm via [`governor`](https://docs.rs/governor).
//!    Zero network latency, operates completely within process RAM. Ideal for standalone microservices.
//! 2. **Distributed Redis ([`RedisLimiter`]):**
//!    Powered by an atomic Lua script implementing a Sliding Window Log in [`redis`](https://docs.rs/redis).
//!    Coordinates request budgets across multiple container replicas sharing a single outbound public IP.
//!
//! ### Quick Example
//!
//! ```rust,no_run
//! use moonclient::limiter::{InMemoryLimiter, RequestLimiter};
//! use moonclient::client::RateLimitStrategy;
//! use std::time::Duration;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // 1. Create an in-memory limiter configured for 5 requests per second
//!     let limiter = InMemoryLimiter::new(RateLimitStrategy::default());
//!     limiter.set_limit(5, Duration::from_secs(1));
//!
//!     // 2. Await capacity before dispatching
//!     limiter.wait().await?;
//!     println!("✅ Rate limit quota granted, safe to send HTTP request!");
//!     Ok(())
//! }
//! ```

use async_trait::async_trait;
use std::sync::Arc;

use crate::client::RateLimitStrategy;
use crate::error::Result;

// Imports specific to the distributed Redis rate limiter, feature-gated
#[cfg(feature = "redis-limit")]
use crate::error::MoonError;
#[cfg(feature = "redis-limit")]
use std::time::Duration;
#[cfg(feature = "redis-limit")]
use tokio::time::sleep;

/// Abstract interface for throttling outbound API request dispatching.
///
/// Implementors regulate outbound throughput by suspending asynchronous tasks until
/// transmission capacity is replenished. Allows transparent switching between in-memory
/// and distributed backends without altering application logic.
///
/// # Concurrency
///
/// Implementors must satisfy [`Send`] + [`Sync`] + `'static`, ensuring safe utilization
/// across multiple worker threads in an asynchronous runtime.
#[async_trait]
pub trait RequestLimiter: Send + Sync + 'static {
    /// Pauses execution of the current asynchronous task until the limiter grants permission to dispatch.
    ///
    /// If the rate limit budget is exhausted, this method asynchronously yields control
    /// (via non-blocking sleep or token acquisition) until the quota resets.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` once capacity has been liberated and the request is permitted to proceed.
    ///
    /// # Errors
    ///
    /// Emits [`crate::error::MoonError::Network`] if a distributed limiter fails to communicate
    /// with its coordination coordinator (e.g. Redis connection timeout).
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use moonclient::limiter::RequestLimiter;
    /// # async fn example(limiter: &dyn RequestLimiter) -> moonclient::Result<()> {
    /// println!("⏳ Awaiting available rate limit slot...");
    /// limiter.wait().await?;
    /// println!("🚀 Slot acquired, dispatching request to network!");
    /// # Ok(())
    /// # }
    /// ```
    async fn wait(&self) -> Result<()>;

    /// Dynamically inserts or updates a specific rate limiting rule on the fly.
    ///
    /// Replaces any existing limit matching the exact same time window duration.
    /// If the underlying limiter implementation does not support runtime mutation,
    /// this method acts as a no-op by default.
    ///
    /// # Arguments
    ///
    /// * `requests` (`u32`) — Maximum number of allowed requests within the time window.
    /// * `window` ([`std::time::Duration`]) — Rolling time window period (e.g. 1 second or 200 ms).
    ///
    /// # Example
    ///
    /// ```rust
    /// use moonclient::limiter::RequestLimiter;
    /// use std::time::Duration;
    ///
    /// fn update_rules(limiter: &dyn RequestLimiter) {
    ///     // Dynamically throttle to 10 requests per minute
    ///     limiter.set_limit(10, Duration::from_secs(60));
    ///     println!("🔧 Updated rate limit window to 10 req / 60s");
    /// }
    /// ```
    fn set_limit(&self, _requests: u32, _window: std::time::Duration) {}

    /// Replaces all active rate limiting rules in a single atomic batch.
    ///
    /// Completely clears previously configured limits and applies the supplied set.
    ///
    /// # Arguments
    ///
    /// * `limits` (`Vec<(u32, std::time::Duration)>`) — Collection of `(request_quota, time_window)` tuples.
    ///
    /// # Example
    ///
    /// ```rust
    /// use moonclient::limiter::RequestLimiter;
    /// use std::time::Duration;
    ///
    /// fn apply_bulk_policy(limiter: &dyn RequestLimiter) {
    ///     limiter.set_limits_bulk(vec![
    ///         (5, Duration::from_secs(1)),   // 5 RPS burst cap
    ///         (90, Duration::from_secs(60)), // 90 RPM sustained cap
    ///     ]);
    ///     println!("📦 Applied dual-window rate limit policy");
    /// }
    /// ```
    fn set_limits_bulk(&self, _limits: Vec<(u32, std::time::Duration)>) {}
}

/// Standard in-memory rate limiter operating within local process memory.
///
/// Implements multi-window traffic shaping via the Token Bucket algorithm powered
/// by the [`governor`](https://docs.rs/governor) crate. Does not require external infrastructure.
///
/// # Concurrency
///
/// Internal limiter buckets are guarded by an [`std::sync::RwLock`] within an [`std::sync::Arc`],
/// enabling thread-safe dynamic rule updates without tearing active connections.
pub struct InMemoryLimiter {
    /// Thread-safe active strategy holding token bucket instances.
    pub(crate) strategy: Arc<std::sync::RwLock<RateLimitStrategy>>,
}

impl InMemoryLimiter {
    /// Creates a new in-memory rate limiter instance wrapping the provided strategy.
    ///
    /// The incoming [`RateLimitStrategy`] is encapsulated inside an [`std::sync::Arc`]
    /// and an [`std::sync::RwLock`], allowing concurrent, lock-free reads during high-frequency
    /// network request dispatches, and exclusive write access only during dynamic rule mutations.
    ///
    /// # Arguments
    ///
    /// * `strategy` ([`RateLimitStrategy`]) — Pre-configured strategy containing active token buckets.
    ///
    /// # Returns
    ///
    /// Returns a newly initialized [`InMemoryLimiter`] ready for local traffic shaping.
    ///
    /// # Example
    ///
    /// ```rust
    /// use moonclient::limiter::InMemoryLimiter;
    /// use moonclient::client::RateLimitStrategy;
    ///
    /// let strategy = RateLimitStrategy::default();
    /// let limiter = InMemoryLimiter::new(strategy);
    /// println!("InMemoryLimiter initialized with default strategy");
    /// ```
    pub fn new(strategy: RateLimitStrategy) -> Self {
        Self {
            strategy: Arc::new(std::sync::RwLock::new(strategy)),
        }
    }
}

impl Default for InMemoryLimiter {
    /// Instantiates an in-memory limiter with an empty initial rate limiting strategy.
    fn default() -> Self {
        Self::new(RateLimitStrategy::default())
    }
}

#[async_trait]
impl RequestLimiter for InMemoryLimiter {
    /// Asynchronously awaits capacity across all configured in-memory token buckets.
    ///
    /// # Implementation Details
    ///
    /// 1. Acquires a read lock on the internal `RateLimitStrategy` and clones the lightweight `Arc` pointer to the limiters vector.
    /// 2. Sequentially iterates through each active time window bucket.
    /// 3. Calls [`governor::RateLimiter::until_ready().await`](https://docs.rs/governor) on each bucket, suspending the current task if capacity is depleted.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` once tokens have been acquired across all registered time windows.
    ///
    /// # Errors
    ///
    /// This in-memory implementation is infallible and will never return an [`Err`].
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use moonclient::limiter::{InMemoryLimiter, RequestLimiter};
    /// use moonclient::client::RateLimitStrategy;
    ///
    /// # async fn doc_example(limiter: &InMemoryLimiter) -> moonclient::Result<()> {
    /// limiter.wait().await?;
    /// println!("In-memory rate limit tokens acquired!");
    /// # Ok(())
    /// # }
    /// ```
    async fn wait(&self) -> Result<()> {
        let limiters = {
            let state = self.strategy.read().unwrap_or_else(|e| e.into_inner());
            state.limiters.clone()
        };

        for (_, limiter) in limiters.iter() {
            limiter.until_ready().await;
        }

        Ok(())
    }

    /// Dynamically inserts or replaces an in-memory token bucket rule for the specified duration.
    ///
    /// # Implementation Details
    ///
    /// 1. Acquires an exclusive write lock on the internal `RateLimitStrategy`.
    /// 2. Clones the existing vector of limiters and evicts any existing bucket whose duration matches `window`.
    /// 3. If `requests > 0` and `window` is non-zero, constructs a new direct token bucket via `create_limiter` and appends it.
    /// 4. Atomically replaces the `Arc<Vec<(Duration, Limiter)>>` pointer. In-flight readers continue executing against their cloned `Arc` snapshots without data tearing or deadlocks.
    ///
    /// # Arguments
    ///
    /// * `requests` (`u32`) — Maximum request quota permitted within the window. Setting to `0` removes the rule.
    /// * `window` ([`std::time::Duration`]) — Rolling time window duration. If zero, the rule insertion is skipped.
    ///
    /// # Example
    ///
    /// ```rust
    /// use moonclient::limiter::{InMemoryLimiter, RequestLimiter};
    /// use moonclient::client::RateLimitStrategy;
    /// use std::time::Duration;
    ///
    /// let limiter = InMemoryLimiter::new(RateLimitStrategy::default());
    ///
    /// // Dynamically enforce 10 requests per second
    /// limiter.set_limit(10, Duration::from_secs(1));
    /// println!("Configured dynamic 10 RPS limit");
    /// ```
    fn set_limit(&self, requests: u32, window: std::time::Duration) {
        let mut strategy = self.strategy.write().unwrap_or_else(|e| e.into_inner());
        let mut limiters_vec = (*strategy.limiters).clone();

        limiters_vec.retain(|(d, _)| *d != window);

        if requests > 0 && !window.is_zero() {
            let limiter = crate::client::create_limiter(requests, window);
            limiters_vec.push((window, limiter));
        }

        strategy.limiters = Arc::new(limiters_vec);
    }

    /// Atomically replaces the entire active ruleset with a new collection of in-memory limits.
    ///
    /// # Implementation Details
    ///
    /// 1. Filters out invalid configuration pairs where `count == 0` or `duration.is_zero()`.
    /// 2. Instantiates fresh, isolated [`governor`](https://docs.rs/governor) token bucket instances for each valid pair.
    /// 3. Acquires a write lock on the internal `RateLimitStrategy` and atomically swaps the internal `Arc` pointer.
    ///
    /// # Arguments
    ///
    /// * `limits` (`Vec<(u32, std::time::Duration)>`) — Collection of `(request_quota, time_window)` pairs to set as the active policy.
    ///
    /// # Example
    ///
    /// ```rust
    /// use moonclient::limiter::{InMemoryLimiter, RequestLimiter};
    /// use moonclient::client::RateLimitStrategy;
    /// use std::time::Duration;
    ///
    /// let limiter = InMemoryLimiter::new(RateLimitStrategy::default());
    ///
    /// limiter.set_limits_bulk(vec![
    ///     (5, Duration::from_secs(1)),    // 5 RPS burst quota
    ///     (100, Duration::from_secs(60)), // 100 RPM sustained quota
    /// ]);
    /// println!("Applied bulk dual-window rate limit policy");
    /// ```
    fn set_limits_bulk(&self, limits: Vec<(u32, std::time::Duration)>) {
        let mut new_limiters = Vec::new();
        for (count, duration) in limits {
            if count == 0 || duration.is_zero() {
                continue;
            }
            let limiter = crate::client::create_limiter(count, duration);
            new_limiters.push((duration, limiter));
        }

        let mut strategy = self.strategy.write().unwrap_or_else(|e| e.into_inner());
        strategy.limiters = Arc::new(new_limiters);
    }
}

// =============================================================================
// 🌐 DISTRIBUTED REDIS LIMITER (COMPILED UNDER FEATURE FLAG)
// =============================================================================

/// Distributed rate limiter synchronizing request quotas across multiple nodes via Redis.
///
/// Employs an atomic Lua script implementing the **Sliding Window Log** algorithm
/// inside [`redis`](https://docs.rs/redis). Guarantees exact cluster-wide RPS compliance
/// across multiple Docker containers or Kubernetes pods without race conditions.
///
/// ### Algorithm Mechanics (Sliding Window Log)
///
/// 1. Uses a Redis Sorted Set (`ZSET`) keyed by `limit_key`.
/// 2. Evicts expired elements with score older than `(now - window)` via `ZREMRANGEBYSCORE`.
/// 3. Counts remaining active elements via `ZCARD`.
/// 4. If under capacity: records current timestamp with `ZADD` and permits immediate execution (0 ms sleep).
/// 5. If capacity exceeded: queries the oldest element via `ZRANGE 0 0 WITHSCORES` and returns the exact millisecond sleep delta until capacity is freed.
#[cfg(feature = "redis-limit")]
pub struct RedisLimiter {
    /// Thread-safe Redis asynchronous connection manager.
    connection_manager: redis::aio::ConnectionManager,
    /// Unique Redis counter key identifier (e.g., `"shikimori-api-limit"`).
    limit_key: String,
    /// Maximum permitted request count within the window.
    limit: u32,
    /// Time window duration expressed in milliseconds.
    window_ms: u64,
}

#[cfg(feature = "redis-limit")]
impl RedisLimiter {
    /// Creates and connects a new distributed Redis rate limiter.
    ///
    /// Connects to the Redis coordinator via asynchronous connection pooling
    /// and pre-calculates the rolling window duration in milliseconds.
    ///
    /// # Arguments
    ///
    /// * `client` ([`redis::Client`](https://docs.rs/redis/latest/redis/struct.Client.html)) — Configured Redis client instance.
    /// * `limit_key` (`impl Into<String>`) — Target key identifier used in Redis for the ZSET counter.
    /// * `limit` (`u32`) — Maximum number of permitted requests per window.
    /// * `window` ([`std::time::Duration`]) — Rolling rate limit time window.
    ///
    /// # Returns
    ///
    /// Returns [`Ok`]`(`[`RedisLimiter`]`)` connected and ready for traffic shaping.
    ///
    /// # Errors
    ///
    /// Emits [`crate::error::MoonError::Config`] if the initial connection manager fails to establish.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use moonclient::limiter::RedisLimiter;
    /// use std::time::Duration;
    ///
    /// #[tokio::main]
    /// async fn main() -> moonclient::Result<()> {
    ///     let redis_client = redis::Client::open("redis://127.0.0.1:6379")
    ///         .map_err(|e| moonclient::MoonError::Config(e.to_string()))?;
    ///
    ///     // 5 requests per 1 second distributed across all workers
    ///     let limiter = RedisLimiter::new(redis_client, "api_global_limit", 5, Duration::from_secs(1)).await?;
    ///     println!("🔗 Connected to Redis rate limit coordinator");
    ///     Ok(())
    /// }
    /// ```
    pub async fn new(
        client: redis::Client,
        limit_key: impl Into<String>,
        limit: u32,
        window: Duration,
    ) -> Result<Self> {
        let connection_manager = client
            .get_connection_manager()
            .await
            .map_err(|e| MoonError::Config(e.to_string()))?;

        Ok(Self {
            connection_manager,
            limit_key: limit_key.into(),
            limit,
            window_ms: window.as_millis() as u64,
        })
    }
}

#[cfg(feature = "redis-limit")]
#[async_trait]
impl RequestLimiter for RedisLimiter {
    /// Enforces distributed rate limiting by executing an atomic Lua sliding window script in Redis.
    ///
    /// # Implementation Details
    ///
    /// 1. Evaluates the current epoch timestamp in milliseconds (`now_ms`).
    /// 2. Dispatches an atomic Lua script through [`redis::aio::ConnectionManager`](https://docs.rs/redis/latest/redis/aio/struct.ConnectionManager.html).
    /// 3. The Lua script trims expired records outside `(now - window)` with `ZREMRANGEBYSCORE` and inspects member cardinality via `ZCARD`.
    /// 4. If current count < limit: records the request via `ZADD` and returns `0` (immediate dispatch permission).
    /// 5. If limit is saturated: computes required sleep delta via `ZRANGE ... WITHSCORES`, suspends execution asynchronously with [`tokio::time::sleep`](https://docs.rs/tokio/latest/tokio/time/fn.sleep.html), and retries.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` once quota has been granted by the Redis coordinator.
    ///
    /// # Errors
    ///
    /// Emits [`crate::error::MoonError::Network`] if communication with the Redis cluster fails or timeouts occur.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use moonclient::limiter::{RedisLimiter, RequestLimiter};
    ///
    /// # async fn doc_example(limiter: &RedisLimiter) -> moonclient::Result<()> {
    /// limiter.wait().await?;
    /// println!("Distributed rate limit quota granted across all cluster nodes!");
    /// # Ok(())
    /// # }
    /// ```
    async fn wait(&self) -> Result<()> {
        let mut conn = self.connection_manager.clone();

        let script = redis::Script::new(
            r#"
            local key = KEYS[1]
            local now = tonumber(ARGV[1])
            local window = tonumber(ARGV[2])
            local limit = tonumber(ARGV[3])
            local clear_before = now - window

            -- Evict stale entries falling outside the active sliding window
            redis.call('ZREMRANGEBYSCORE', key, '-inf', clear_before)

            -- Retrieve current request count within the window
            local amount = redis.call('ZCARD', key)

            if amount < limit then
                -- Quota available: record request and permit immediate dispatch (0 ms wait)
                redis.call('ZADD', key, now, now)
                return 0
            else
                -- Limit reached: compute time remaining until earliest slot is liberated
                local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
                if #oldest > 0 then
                    local oldest_score = tonumber(oldest[2])
                    local wait_time = (oldest_score + window) - now
                    return wait_time
                end
                return window
            end
        "#,
        );

        loop {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;

            let wait_time_ms: u64 = script
                .key(&self.limit_key)
                .arg(now_ms)
                .arg(self.window_ms)
                .arg(self.limit)
                .invoke_async(&mut conn)
                .await
                .map_err(|e| {
                    let err_msg = crate::tr!("redis-limit-error", "error" => e.to_string());
                    log::error!("{}", err_msg);
                    MoonError::Network(err_msg)
                })?;

            if wait_time_ms == 0 {
                break;
            }

            log::debug!(
                "{}",
                crate::tr!(
                    "redis-limit-waiting",
                    "key" => &self.limit_key,
                    "duration" => wait_time_ms
                )
            );
            sleep(Duration::from_millis(wait_time_ms)).await;
        }

        Ok(())
    }
}
