# --- Timeouts ---
timeout-changed = ⏱️ Network timeout changed to: {$timeout}
timeout-disabled = ⏱️ Network timeout fully disabled.

# --- Limits ---
limit-rule-changed = 📡 Rate limit rule changed successfully: {$requests} req / {$window}
limit-invalid-ignored = 📡 Ignored invalid limit in bulk configuration: {$count} req / {$duration}
limit-strategy-reset = 📡 Rate limiting strategy completely overwritten.

# --- Redis Distributed Limiter ---
redis-limit-waiting = 📡 Global limit reached for key "{$key}". Throttling execution, waiting {$duration} ms...
redis-limit-error = ❌ Failed to communicate with Redis rate limiter: {$error}

# --- Network ---
network-request-sending = Sending request to network address: "{$url}"
network-response-received = Received response from network address: "{$url}"
network-download-started = Loading binary file from address: {$url}

# --- Network Retries ---
request-retrying = 🔄 Triggering request retry...

# --- Panics / Invariant Expects ---
expect-invalid-base-url = Client base URL is malformed. Please check settings.
expect-limit-greater-than-zero = Internal invariant: requests frequency limit must be greater than zero.
expect-invalid-period-calculation = Internal invariant: invalid rate-limiter period calculation.
expect-invalid-macro-base-url = Malformed client base URL. Please check initialization settings.

# --- System Errors ---
err-middleware = Middleware error: {$error}
err-network = Network error: {$error}
err-parse = JSON parsing error: {$error}
err-io = Input/output error (IO): {$error}
err-api-validation = API validation error: {$error}
err-api-error = Server returned error: {$code} - {$message}
err-config = Client configuration error: {$error}
err-response-decode = API response decoding error: {$error}\nRaw server response:\n{$raw}
err-unauthorized = Local authorization error: {$error}
err-validation-error = Validation error: {$error}
err-unknown = Unknown error
