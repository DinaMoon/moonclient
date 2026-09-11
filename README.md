# MoonClient 🌙

<div align="center">

[![Crates.io](https://img.shields.io/crates/v/moonclient.svg?style=flat-square)](https://crates.io/crates/moonclient)
[![Documentation](https://docs.rs/moonclient/badge.svg?style=flat-square)](https://docs.rs/moonclient)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg?style=flat-square)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-2024%20edition-orange.svg?style=flat-square)](https://www.rust-lang.org)

**English** | [Русский](README.ru.md)

An enterprise-grade, resilient HTTP client engine engineered for mission-critical API integrations, distributed scrapers, and high-concurrency microservices.

</div>

---

## 📑 Table of Contents

- [Overview](#-overview)
- [Key Features](#-key-features)
- [Installation](#-installation)
- [Quick Start](#-quick-start)
- [Core Concepts](#-core-concepts)
  - [1. Lifecycle Hooks (`ClientHook`)](#1-lifecycle-hooks-clienthook)
  - [2. Token Bucket Rate Limiting](#2-token-bucket-rate-limiting)
  - [3. Memory-Safe Bounded Responses (OOM Protection)](#3-memory-safe-bounded-responses-oom-protection)
  - [4. Zero-Allocation URL Macro (`build_url!`)](#4-zero-allocation-url-macro-build_url)
  - [5. Dedicated Split File Logging (`MoonLogger`)](#5-dedicated-split-file-logging-moonlogger)
  - [6. Project Fluent Localization (`i18n`)](#6-project-fluent-localization-i18n)
- [Feature Flags](#-feature-flags)
- [Architecture & Re-exports](#-architecture--re-exports)
- [License](#-license)

---

## 🌟 Overview

`moonclient` is designed from the ground up to eliminate the recurring boilerplate associated with building robust production API client libraries in Rust. It wraps `reqwest` and `reqwest-middleware` into a clean, thread-safe, decoupled engine that isolates transport mechanics, retry policies, rate limits, and memory safety from application-level business logic.

---

## ⚡ Key Features

- 🛡️ **Autonomous Rate Limiting:** Built-in Token Bucket traffic shaping powered by `governor`: granular configuration of multiple arbitrary time windows (`Duration`), quick RPS/RPM presets, and optional distributed Redis coordination.
- 🔁 **Hybrid Resilient Middleware:** Transparent exponential backoff retries via `reqwest-retry`, engineered to automatically pass through non-cloneable streaming multipart file uploads without panics.
- 🪝 **Pluggable Lifecycle Hooks:** Interceptor trait ([`ClientHook`]) supporting pre-request header injection (e.g., Bearer tokens, HMAC signatures) and reactive `401 Unauthorized` token renewal with request replaying.
- 🔒 **OOM Protection:** Bounded response buffer decoders safeguarding against unbounded streaming memory exhaustion attacks (e.g., unexpected multi-gigabyte server error pages).
- 📦 **Extractor Pattern:** Unified, strongly typed `.execute()` method returning `Json<T>`, `Xml<T>`, `Response`, or `()` (void).
- 🌍 **Native Project Fluent i18n:** Built-in localized diagnostics and network error reporting with thread-safe Task-Local locale resolution.
- 🚀 **Zero-Allocation Routing:** The [`build_url!`] macro converts primitives, strings, and custom enums into safe URL paths via `Cow<'_, str>`.

---

## 📦 Installation

Add `moonclient` to your `Cargo.toml`:

```toml
[dependencies]
moonclient = "0.1.0"
tokio = { version = "1.53", features = ["full"] }
serde = { version = "1.0", features = ["derive"] }
```

---

## 🚀 Quick Start

Here is a minimal standalone example demonstrating initialization, rate limiting, and typed JSON response extraction:

```rust
use moonclient::response::Json;
use moonclient::{ClientHook, MoonClient, Result};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Todo {
    id: u32,
    title: String,
    completed: bool,
}

// Minimal no-op lifecycle hook
struct DefaultHook;

#[async_trait::async_trait]
impl ClientHook for DefaultHook {
    type Error = std::convert::Infallible;
}

#[tokio::main]
async fn main() -> Result<()> {
    // 1. Build client with 5 Requests-Per-Second limit
    let client = MoonClient::builder(DefaultHook)
        .with_base_url("https://jsonplaceholder.typicode.com")
        .requests_per_second(5)
        .build();

    // 2. Safely construct the endpoint URL
    let url = moonclient::build_url!(client, "todos", 1)?;
    let request = client.get(url);

    // 3. Dispatch request and extract JSON payload
    let Json(todo): Json<Todo> = client.execute(request).await?;

    println!("Fetched Todo #{}: {} (completed: {})", todo.id, todo.title, todo.completed);
    Ok(())
}
```

---

## 🧠 Core Concepts

### 1. Lifecycle Hooks (`ClientHook`)

`MoonClient` decouples authentication and domain quirks through the [`ClientHook`] trait:

```rust
use async_trait::async_trait;
use moonclient::ClientHook;
use reqwest_middleware::RequestBuilder;
use reqwest::Response;

struct MyAuthHook {
    token: String,
}

#[async_trait]
impl ClientHook for MyAuthHook {
    type Error = std::convert::Infallible;

    // Injected immediately before dispatch
    async fn pre_request(&self, request: RequestBuilder) -> Result<RequestBuilder, Self::Error> {
        Ok(request.header("Authorization", format!("Bearer {}", self.token)))
    }

    // Intercepts 401 Unauthorized for background session refresh
    async fn handle_unauthorized(&self) -> Result<bool, Self::Error> {
        // Refresh token asynchronously...
        Ok(true) // Return true to replay the original request
    }
}
```

### 2. Token Bucket Rate Limiting

Rate limiting is applied transparently prior to network transmission:

```rust
let client = MoonClient::builder(DefaultHook)
    .add_limit(5, Duration::from_millis(200)) // Custom window: 5 requests per 200ms
    .requests_per_second(5)                   // Quick preset: 5 RPS
    .requests_per_minute(90)                  // Quick preset: 90 RPM
    .build();
```

For distributed multi-instance clusters, enable the `redis-limit` feature to coordinate limits via Redis.

### 3. Memory-Safe Bounded Responses (OOM Protection)

Standard HTTP libraries read the entire response body into memory. If an upstream proxy or faulty API returns an unbounded stream or a 500 MB HTML crash dump, your process can trigger an Out-Of-Memory (OOM) panic.

`moonclient` buffers responses with strict, configurable capacity limits:
- **`DEFAULT_ERROR_BODY_LIMIT` (64 KiB):** For HTTP 4xx/5xx error responses.
- **`DEFAULT_BODY_LIMIT` (16 MiB):** For successful payloads.
- **`set_max_response_size(bytes)`:** Runtime dynamic adjustment via an atomic counter.

### 4. Zero-Allocation URL Macro (`build_url!`)

The `build_url!` macro accepts any type implementing [`IntoSegment`]:

```rust
let user_id = 42_u32;
let route = "profile";

// Produces: https://api.site.com/users/42/profile
let url = moonclient::build_url!(client, "users", user_id, route)?;
```

### 5. Dedicated Split File Logging (`MoonLogger`)

`MoonLogger` provides non-blocking, multi-threaded logging with Mutex-poisoning recovery and split file sinks:

```rust
moonclient::logger::init()
    .with_level(log::LevelFilter::Debug)
    .with_split_files("logs/core.log", "logs/api.log")
    .setup()?;
```
- Logs matching target `moonclient*` are routed to `core.log`.
- External consumer domain logs are routed to `api.log`.

### 6. Project Fluent Localization (`i18n`)

All internal engine error diagnostics and logging strings are localized via Mozilla's **Project Fluent**:
- Automatic resolution hierarchy: Tokio Task-Local ➡️ Global Override ➡️ Host OS locale ➡️ Default (`en`).
- External crates can inject their own translation bundles via `moonclient::i18n::register_resource()`.

---

## 🚩 Feature Flags

| Feature | Description | Default |
| :--- | :--- | :---: |
| `xml` | Enables XML response payload extraction via `quick-xml`. | **Disabled** |
| `redis-limit` | Enables distributed token bucket rate limiting via Redis connection pools. | **Disabled** |

---

## 🏗️ Architecture & Re-exports

To prevent dependency version drift across your workspace, `moonclient` directly re-exports its fundamental HTTP networking stack:

```rust
pub use moonclient::reqwest;
pub use moonclient::reqwest_middleware;
pub use moonclient::reqwest_tracing;
```

---

## 📄 License

Licensed under either of:

- **Apache License, Version 2.0** ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- **MIT license** ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.
