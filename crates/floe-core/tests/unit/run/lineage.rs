use floe_core::config::LineageConfig;
use floe_core::lineage::OpenLineageObserver;
use floe_core::run::events::{RunEvent, RunObserver};

fn make_config(server_url: &str, max_failures: Option<u32>) -> LineageConfig {
    LineageConfig {
        url: server_url.to_string(),
        api_key: None,
        timeout_secs: Some(2),
        namespace: "test-ns".to_string(),
        producer: None,
        max_failures,
    }
}

fn run_started_event() -> RunEvent {
    RunEvent::RunStarted {
        run_id: "test-run-1".to_string(),
        config: "config.yml".to_string(),
        report_base: None,
        ts_ms: 1_000_000,
    }
}

// EntityStarted is used in tests where we need to trigger a POST without the
// RunStarted reset side-effect, matching the realistic per-run event sequence.
fn entity_started_event() -> RunEvent {
    RunEvent::EntityStarted {
        run_id: "test-run-1".to_string(),
        name: "orders".to_string(),
        ts_ms: 1_001_000,
    }
}

// Circuit opens after max_failures consecutive failures and stops hitting the server.
#[test]
fn circuit_opens_after_threshold() {
    let mut server = mockito::Server::new();
    let mock = server
        .mock("POST", "/api/v1/lineage")
        .with_status(500)
        // 1 failure call × 3 retry attempts = 3 server hits, then circuit opens.
        .expect(3)
        .create();

    let config = make_config(&server.url(), Some(1));
    let obs = OpenLineageObserver::new(&config, &[]).unwrap();

    // RunStarted: resets circuit state, then POSTs once → all 3 retries fail → circuit opens.
    obs.on_event(run_started_event());
    assert!(
        obs.is_circuit_open(),
        "circuit should be open after max_failures"
    );

    // Subsequent events in this run should not hit the server (circuit is open).
    obs.on_event(entity_started_event());
    obs.on_event(entity_started_event());

    mock.assert();
}

// A successful attempt resets the consecutive failure counter.
#[test]
fn success_resets_failure_counter() {
    let mut server = mockito::Server::new();
    // RunStarted + one EntityStarted = 2 successful POSTs.
    let _mock = server
        .mock("POST", "/api/v1/lineage")
        .with_status(200)
        .expect(2)
        .create();

    let config = make_config(&server.url(), Some(3));
    let obs = OpenLineageObserver::new(&config, &[]).unwrap();

    obs.on_event(run_started_event());
    obs.on_event(entity_started_event());

    assert!(
        !obs.is_circuit_open(),
        "circuit should remain closed on success"
    );
    assert_eq!(
        obs.consecutive_failures(),
        0,
        "failures should be reset on success"
    );
}

// A 4xx response (non-retryable) is counted as one failure without retrying.
#[test]
fn non_retryable_error_counts_without_retry() {
    let mut server = mockito::Server::new();
    // Exactly 1 request expected — no retries for 401.
    let mock = server
        .mock("POST", "/api/v1/lineage")
        .with_status(401)
        .expect(1)
        .create();

    let config = make_config(&server.url(), Some(3));
    let obs = OpenLineageObserver::new(&config, &[]).unwrap();

    obs.on_event(run_started_event());

    assert_eq!(obs.consecutive_failures(), 1);
    assert!(
        !obs.is_circuit_open(),
        "one 4xx should not open the circuit (threshold is 3)"
    );
    mock.assert();
}

// A 429 (Too Many Requests) is retryable and triggers all retry attempts.
#[test]
fn rate_limit_response_is_retried() {
    let mut server = mockito::Server::new();
    // 3 retry attempts expected for a 429.
    let mock = server
        .mock("POST", "/api/v1/lineage")
        .with_status(429)
        .expect(3)
        .create();

    let config = make_config(&server.url(), Some(5));
    let obs = OpenLineageObserver::new(&config, &[]).unwrap();

    obs.on_event(run_started_event());

    assert_eq!(obs.consecutive_failures(), 1);
    mock.assert();
}

// After a failure then a success within the same run, the circuit stays closed.
#[test]
fn circuit_stays_closed_after_recovery() {
    let mut server = mockito::Server::new();

    // RunStarted: 500 (all 3 retry attempts fail), failure counter = 1.
    let fail_mock = server
        .mock("POST", "/api/v1/lineage")
        .with_status(500)
        .expect(3)
        .create();

    let config = make_config(&server.url(), Some(3));
    let obs = OpenLineageObserver::new(&config, &[]).unwrap();

    obs.on_event(run_started_event());
    assert_eq!(obs.consecutive_failures(), 1);
    fail_mock.assert();

    // EntityStarted: 200 → failure counter resets to 0.
    let success_mock = server
        .mock("POST", "/api/v1/lineage")
        .with_status(200)
        .expect(1)
        .create();

    obs.on_event(entity_started_event());
    assert_eq!(
        obs.consecutive_failures(),
        0,
        "failure counter resets on success"
    );
    assert!(!obs.is_circuit_open());
    success_mock.assert();
}

// Circuit state is reset at the start of each new run (RunStarted) so a
// recovered endpoint is retried in subsequent runs within the same process.
#[test]
fn circuit_resets_on_new_run_started() {
    let mut server = mockito::Server::new();

    // Run 1: all events fail → circuit opens.
    let fail_mock = server
        .mock("POST", "/api/v1/lineage")
        .with_status(500)
        .expect(3)
        .create();

    let config = make_config(&server.url(), Some(1));
    let obs = OpenLineageObserver::new(&config, &[]).unwrap();

    obs.on_event(run_started_event());
    assert!(obs.is_circuit_open());
    fail_mock.assert();

    // Run 2: RunStarted resets the circuit; endpoint now returns 200.
    let success_mock = server
        .mock("POST", "/api/v1/lineage")
        .with_status(200)
        .expect(1)
        .create();

    obs.on_event(RunEvent::RunStarted {
        run_id: "test-run-2".to_string(),
        config: "config.yml".to_string(),
        report_base: None,
        ts_ms: 2_000_000,
    });
    assert!(
        !obs.is_circuit_open(),
        "circuit should reset for the new run"
    );
    assert_eq!(obs.consecutive_failures(), 0);
    success_mock.assert();
}
