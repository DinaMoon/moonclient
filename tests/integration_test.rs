//! Comprehensive integration test suite for the universal [`MoonClient`] engine.
//!
//! Validates:
//! * URL segment conversions via [`IntoSegment`].
//! * Hierarchical URL construction via [`build_url!`].
//! * Rate-limiting Token Bucket throttling.
//! * Live network payload deserialization, void responses, and binary file streaming.

use async_trait::async_trait;
use pretty_assertions::assert_eq;
use reqwest::Response;
use reqwest_middleware::RequestBuilder;
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};

mod common;

use moonclient::response::Json;
use moonclient::{ClientHook, IntoSegment, MoonClient, build_url};

// Bind high-performance mimalloc memory allocator to accelerate Tokio worker threads
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Mock payload data structure for JSON extraction validation.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
struct MockTodo {
    /// Authoring user identifier.
    #[serde(rename = "userId")]
    user_id: u32,
    /// Unique task identifier.
    id: u32,
    /// Task title string.
    title: String,
    /// Task completion flag.
    completed: bool,
}

/// Minimalist lifecycle mock hook for transaction interception checks.
struct TestLifecycleHook;

#[async_trait]
impl ClientHook for TestLifecycleHook {
    type Error = std::convert::Infallible;

    /// Appends a diagnostic verification header to the outbound request.
    async fn pre_request(&self, request: RequestBuilder) -> Result<RequestBuilder, Self::Error> {
        log::info!("🔧 [TestHook] Appending test header during pre-request...");
        Ok(request.header("X-Test-Header", "MoonClientValidation"))
    }

    /// Intercepts and validates the raw wire response.
    async fn post_request(&self, response: Response) -> Result<Response, Self::Error> {
        log::info!("🔧 [TestHook] Intercepting response during post-response...");
        Ok(response)
    }
}

// =========================================================================
// 🧩 TEST SECTION: PATH SEGMENTS AND MACROS
// =========================================================================

/// Verifies zero-allocation type conversions into URL path segments via [`IntoSegment`].
///
/// Tests owned strings, string slices, unsigned integers, and negative signed integers.
#[test]
fn it_test_into_segment_conversions_ok() {
    common::setup_test_environment();

    let string_owned = "test_route".to_string();
    let string_ref = "test_route";
    let int_u32 = 42_u32;
    let int_i32 = -42_i32;

    assert_eq!(string_owned.to_segment(), "test_route");
    assert_eq!(string_ref.to_segment(), "test_route");
    assert_eq!(int_u32.to_segment(), "42");
    assert_eq!(int_i32.to_segment(), "-42");
}

/// Verifies hierarchical URL path construction via [`build_url!`] without redundant allocations.
#[test]
fn it_test_url_macro_building_ok() {
    common::setup_test_environment();

    // Minimalist client structure exposing the `.base_url()` contract
    struct MockClient {
        base_url: url::Url,
    }

    impl MockClient {
        fn base_url(&self) -> &url::Url {
            &self.base_url
        }
    }

    let client = MockClient {
        base_url: url::Url::parse("https://test.api.io/v1").unwrap(),
    };

    let target_id = 999_i32;
    let category = "comments";

    // Build the target URL on the fly
    let computed_url = build_url!(client, "target", category, target_id);

    assert_eq!(
        computed_url.as_str(),
        "https://test.api.io/v1/target/comments/999"
    );
}

// =========================================================================
// 📡 TEST SECTION: RATE LIMITING QUOTAS
// =========================================================================

/// Verifies rate limiter throttling under rapid, successive invocation cycles.
///
/// Asserts that three consecutive requests under a 2 RPS limit impose an elapsed delay of at least 500 ms.
#[tokio::test]
async fn it_test_client_rate_limiting_ok() -> std::result::Result<(), Box<dyn std::error::Error>> {
    common::setup_test_environment();

    let hook = TestLifecycleHook;
    // Configure strict rate limit quota: exactly 2 requests per second
    let client = MoonClient::builder(hook)
        .with_base_url("https://jsonplaceholder.typicode.com")
        .requests_per_second(2)
        .build();

    let start_time = Instant::now();

    // Execute three consecutive limiter quota reservations
    client.wait_for_limits().await?;
    client.wait_for_limits().await?;
    client.wait_for_limits().await?;

    let elapsed = start_time.elapsed();

    // Three dispatches under a 2 RPS limit must take at least 500 milliseconds
    assert!(
        elapsed >= Duration::from_millis(500),
        "Rate limiter throttling should delay execution. Elapsed duration: {:?}",
        elapsed
    );

    Ok(())
}

// =========================================================================
// 🌐 TEST SECTION: LIVE NETWORK CALLS (IGNORED FOR CONCURRENT TEST RUNNERS)
// =========================================================================

/// Verifies live network GET request execution with typed JSON payload extraction.
#[tokio::test]
#[ignore]
async fn it_test_network_get_json_ok() {
    common::setup_test_environment();

    let hook = TestLifecycleHook;
    let client = MoonClient::builder(hook)
        .with_base_url("https://jsonplaceholder.typicode.com")
        .build();

    let url = build_url!(client, "todos", 1);
    let request = client.get(url);

    // Extract typed model from the HTTP response
    let Json(todo): Json<MockTodo> = client
        .execute(request)
        .await
        .expect("Failed to execute GET request or deserialize JSON");

    assert_eq!(todo.id, 1);
    assert_eq!(todo.user_id, 1);
    assert!(!todo.title.is_empty());
}

/// Verifies live network POST request execution with empty response extraction (`()`).
#[tokio::test]
#[ignore]
async fn it_test_network_post_void_ok() {
    common::setup_test_environment();

    let hook = TestLifecycleHook;
    let client = MoonClient::builder(hook)
        .with_base_url("https://jsonplaceholder.typicode.com")
        .build();

    let url = build_url!(client, "todos");
    let payload = MockTodo {
        user_id: 10,
        id: 201,
        title: "Test task".to_string(),
        completed: false,
    };
    let request = client.post(url).json(&payload);

    // Extract unit type `()`, validating a 2xx/3xx response status code
    let _: () = client
        .execute(request)
        .await
        .expect("Failed to execute POST request with empty response body");
}

/// Verifies streaming binary asset download and local filesystem persistence.
#[tokio::test]
#[ignore]
async fn it_test_network_save_file_ok() {
    common::setup_test_environment();

    let hook = TestLifecycleHook;
    let client = MoonClient::builder(hook)
        .with_base_url("https://www.rust-lang.org")
        .build();

    let target_path = std::path::PathBuf::from("target/tests/rust-logo-test.svg");

    // Clean up residual artifacts from prior test executions
    if target_path.exists() {
        std::fs::remove_file(&target_path).ok();
    }

    let request = client.get("https://www.rust-lang.org/static/images/rust-logo-blk.svg");

    // Asynchronously stream and persist the file
    client
        .save_file(request, &target_path)
        .await
        .expect("Failed to download and persist test binary asset");

    // Verify the file was created and is non-empty
    assert!(target_path.exists(), "Target file must exist on disk");
    let file_size = std::fs::metadata(&target_path).unwrap().len();
    assert!(
        file_size > 0,
        "Persisted binary file size must be greater than zero"
    );

    // Clean up test file on disk
    std::fs::remove_file(&target_path).ok();
}
