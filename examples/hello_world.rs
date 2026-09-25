//! Standalone demonstration showcasing the core capabilities of [`MoonClient`].
//!
//! This example illustrates the complete lifecycle of configuring and executing requests
//! through the client engine:
//! 1. Initializing multi-sink logging via [`moonclient::logger`] with split file targets.
//! 2. Defining an interceptor hook implementing [`ClientHook`] for dynamic header injection.
//! 3. Configuring autonomous rate limiting via [`MoonClientBuilder::requests_per_second`].
//! 4. Strongly typed JSON payload extraction via [`Json`](moonclient::response::Json).
//! 5. Unit type extraction (`()`) for confirming successful HTTP 2xx operations.
//! 6. Streaming binary asset downloads and direct disk persistence via [`MoonClient::save_file`].

use async_trait::async_trait;
use reqwest::Response;
use reqwest_middleware::RequestBuilder;
use serde::{Deserialize, Serialize};
use std::convert::Infallible;

use moonclient::response::Json;
use moonclient::{ClientHook, MoonClient};

// Bind high-performance mimalloc memory allocator to accelerate Tokio worker threads
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Data transfer object modeling a Todo item returned by the JSONPlaceholder API.
#[derive(Debug, Deserialize, Serialize)]
struct Todo {
    /// Identifier of the user authoring this item.
    #[serde(rename = "userId")]
    user_id: u64,
    /// Unique numeric primary key of the task.
    id: u64,
    /// Textual title describing the task.
    title: String,
    /// Boolean flag indicating task completion status.
    completed: bool,
}

/// Demonstration plugin hook illustrating request and response lifecycle interception.
struct MockApiHook;

#[async_trait]
impl ClientHook for MockApiHook {
    /// Infallible error type indicating this demonstration hook never produces errors.
    type Error = Infallible;

    /// Intercepts the request prior to wire dispatch, injecting diagnostic authentication headers.
    ///
    /// # Arguments
    ///
    /// * `request` ([`RequestBuilder`]) — Mutable request builder prepared for dispatch.
    ///
    /// # Returns
    ///
    /// Returns `Ok(RequestBuilder)` containing the enriched HTTP headers.
    async fn pre_request(&self, request: RequestBuilder) -> Result<RequestBuilder, Self::Error> {
        log::info!("🔧 [Hook: Pre-request] Dynamically injecting custom authorization token...");

        let modified_request = request
            .header("X-Mock-Authorization", "Bearer moonclient-token-999")
            .header("X-Developer-Signature", "DinaMoon");

        Ok(modified_request)
    }

    /// Intercepts the wire response immediately upon receipt, logging diagnostic telemetry.
    ///
    /// # Arguments
    ///
    /// * `response` ([`Response`]) — Raw HTTP response received from the remote server.
    ///
    /// # Returns
    ///
    /// Returns `Ok(Response)` forwarding the response envelope to the payload extractor.
    async fn post_request(&self, response: Response) -> Result<Response, Self::Error> {
        log::info!(
            "🔧 [Hook: Post-response] Intercepted response status: {}. Validating payload integrity...",
            response.status()
        );
        Ok(response)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Configure smart split logging:
    // Core framework events route to 'logs/core.log', while application domain logs flow to 'logs/app.log'
    moonclient::logger::init()
        .with_level(log::LevelFilter::Debug)
        .with_split_files("logs/core.log", "logs/app.log")
        .setup()?;

    log::info!("🌅 Launching MoonClient demonstration example...");

    // 1. Instantiate the lifecycle hook
    let hook = MockApiHook;

    // 2. Build the client instance with a strict 2 RPS limit for demonstration
    let client = MoonClient::builder(hook)
        .with_base_url("https://jsonplaceholder.typicode.com")
        .requests_per_second(2)
        .build();

    // =========================================================================
    // 📬 1. JSON PAYLOAD EXTRACTION (Json<T> Extractor)
    // =========================================================================
    log::info!("📬 Executing asynchronous GET request to /todos/1...");

    let todo_url = moonclient::build_url!(client, "todos", 1)?;
    let get_request = client.get(todo_url);

    // Extract strongly typed model using the Json extractor
    let Json(todo): Json<Todo> = client.execute(get_request).await?;
    log::info!(
        "🎉 [Result] Successfully deserialized JSON payload: {:?}",
        todo
    );

    // =========================================================================
    // 📬 2. VOID REQUEST EXTRACTION (() Extractor)
    // =========================================================================
    log::info!("📬 Executing asynchronous POST request to create a Todo...");

    let create_url = moonclient::build_url!(client, "todos")?;
    let new_todo = Todo {
        user_id: 1,
        id: 201,
        title: "Build a flawless Rust library".to_string(),
        completed: false,
    };
    let post_request = client.post(create_url).json(&new_todo);

    // Extract unit type `()`, verifying an HTTP 2xx/3xx success status code
    let _: () = client.execute(post_request).await?;
    log::info!(
        "🎉 [Result] POST request successfully completed! (Void extractor verified 2xx status)"
    );

    // =========================================================================
    // 📬 3. BINARY ASSET STREAMING AND DISK PERSISTENCE
    // =========================================================================
    log::info!("📬 Testing binary file download directly to filesystem...");

    let rust_logo_url = "https://www.rust-lang.org/static/images/rust-logo-blk.svg";
    let download_request = client.get(rust_logo_url);

    // Stream and persist the binary logo into the local directory
    client
        .save_file(download_request, "logs/rust-logo.svg")
        .await?;
    log::info!(
        "🎉 [Result] Binary image successfully downloaded and persisted to 'logs/rust-logo.svg'!"
    );

    log::info!("🏁 All demonstration pipeline steps completed successfully!");
    Ok(())
}
