use bytes::{BufMut, Bytes, BytesMut};
use h2::server;
use http::HeaderMap;
use integration_tests::pb::{
    client_stream_client::ClientStreamClient, test1_client::Test1Client, test_client::TestClient,
    Input, Input1,
};
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tonic::transport::{Channel, Endpoint};
use tonic::Code;

/// Minimal gRPC response handler for an empty Output message.
/// Drains the full request body and reports the total received bytes in
/// an `x-received-bytes` trailing header so tests can verify body integrity.
async fn handle_grpc(
    req: http::Request<h2::RecvStream>,
    mut respond: h2::server::SendResponse<Bytes>,
) {
    // Drain request body, counting total bytes received.
    let mut body = req.into_body();
    let mut total_bytes = 0usize;
    while let Some(chunk) = body.data().await {
        let chunk = chunk.unwrap();
        total_bytes += chunk.len();
        body.flow_control().release_capacity(chunk.len()).unwrap();
    }

    // Response headers
    let response = http::Response::builder()
        .status(200)
        .header("content-type", "application/grpc+proto")
        .body(())
        .unwrap();
    let mut send = respond.send_response(response, false).unwrap();

    // Empty gRPC message frame: 1 byte compression flag + 4 byte length (0)
    let mut buf = BytesMut::with_capacity(5);
    buf.put_u8(0);
    buf.put_u32(0);
    send.send_data(buf.freeze(), false).unwrap();

    // Trailers with grpc-status: 0 (OK) and received byte count.
    let mut trailers = HeaderMap::new();
    trailers.insert("grpc-status", "0".parse().unwrap());
    trailers.insert("x-received-bytes", total_bytes.to_string().parse().unwrap());
    send.send_trailers(trailers).unwrap();
}

/// h2 server that sends GOAWAY(NO_ERROR) after `goaway_after` requests on the
/// FIRST connection. Subsequent connections (after reconnect) have no limit.
/// Accepts new TCP connections in a loop so reconnects work.
///
/// `max_streams`: MAX_CONCURRENT_STREAMS setting. Limits how many streams the
/// client can have in flight, forcing excess requests into h2's pending_open
/// queue where they are vulnerable to GOAWAY rejection.
async fn h2_server(listener: TcpListener, goaway_after: usize, max_streams: Option<u32>) {
    let mut first = true;
    loop {
        let Ok((tcp, _peer)) = listener.accept().await else {
            break;
        };

        let limit = if first { Some(goaway_after) } else { None };
        first = false;

        tokio::spawn(async move {
            let mut builder = server::Builder::new();
            if let Some(max) = max_streams {
                builder.max_concurrent_streams(max);
            }
            let mut conn = builder.handshake(tcp).await.unwrap();
            let mut count = 0;

            while let Some(result) = conn.accept().await {
                let (req, respond) = result.unwrap();
                count += 1;
                tokio::spawn(handle_grpc(req, respond));

                if let Some(limit) = limit {
                    if count >= limit {
                        conn.graceful_shutdown();
                    }
                }
            }
        });
    }
}

/// Sequential requests after GOAWAY: the Reconnect layer should detect the
/// dead connection on the next poll_ready and transparently reconnect.
#[tokio::test]
async fn goaway_reconnects_sequential() {
    integration_tests::trace_init();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(h2_server(listener, 5, None));

    let channel = Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = TestClient::new(channel);

    // Send 20 sequential requests. Server sends GOAWAY after every 5 per
    // connection, so the client must reconnect multiple times.
    for i in 0..20 {
        let result = client.unary_call(tonic::Request::new(Input {})).await;
        assert!(
            result.is_ok(),
            "Sequential request {i} failed: {:?}",
            result.unwrap_err()
        );
    }
}

/// Concurrent requests with constrained MAX_CONCURRENT_STREAMS: the server
/// limits concurrent streams to 2, forcing excess requests into h2's
/// pending_open queue. When GOAWAY arrives, queued streams are rejected.
///
/// This reproduces the real-world scenario (e.g. nginx keepalive_requests)
/// where the client has more pending requests than the server will accept.
///
/// The rejected requests are all retried at once on a new connection that
/// also allows only 2 streams, so they must wait for its SETTINGS instead of
/// being refused again.
#[tokio::test]
async fn goaway_drops_in_flight_requests() {
    integration_tests::trace_init();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    // max_concurrent_streams=2: only 2 streams can be in flight at a time,
    // rest are queued in h2's pending_open.
    tokio::spawn(h2_server(listener, 5, Some(2)));

    let channel = Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let client = TestClient::new(channel);

    // Warm up: establish the connection with one successful request.
    let mut c = client.clone();
    c.unary_call(tonic::Request::new(Input {})).await.unwrap();

    // Fire many concurrent requests. The buffer dispatches them to h2 quickly,
    // but MAX_CONCURRENT_STREAMS=2 means only 2 have their HEADERS sent.
    // The rest are queued in pending_open. After 4 more requests (total 5),
    // the server sends GOAWAY. Queued streams with IDs > last_stream_id are
    // rejected by h2.
    let futs: Vec<_> = (0..50)
        .map(|_| {
            let mut c = client.clone();
            tokio::spawn(async move { c.unary_call(tonic::Request::new(Input {})).await })
        })
        .collect();

    let mut successes = 0;
    let mut failures = 0;
    for fut in futs {
        match fut.await.unwrap() {
            Ok(_) => successes += 1,
            Err(e) => {
                failures += 1;
                eprintln!("  failed: {e}");
            }
        }
    }

    eprintln!("concurrent: {successes} ok, {failures} failed");

    assert_eq!(
        failures, 0,
        "All requests should succeed: GOAWAY-rejected streams are transparently retried"
    );
}

/// After GOAWAY drops in-flight requests, the next batch of requests should
/// succeed on a fresh connection (reconnect works).
#[tokio::test]
async fn goaway_subsequent_requests_succeed() {
    integration_tests::trace_init();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(h2_server(listener, 5, Some(2)));

    let channel = Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let client = TestClient::new(channel);

    // First batch: some will fail due to GOAWAY
    let futs: Vec<_> = (0..20)
        .map(|_| {
            let mut c = client.clone();
            tokio::spawn(async move { c.unary_call(tonic::Request::new(Input {})).await })
        })
        .collect();
    for fut in futs {
        let _ = fut.await;
    }

    // Give time for the reconnect layer to establish a new connection
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Second batch: should all succeed on the new connection
    let mut client2 = client.clone();
    for i in 0..5 {
        let result = client2.unary_call(tonic::Request::new(Input {})).await;
        assert!(
            result.is_ok(),
            "Post-GOAWAY request {i} failed: {:?}",
            result.unwrap_err()
        );
    }
}

/// Client-streaming requests with GOAWAY: verifies that `SharedBody` correctly
/// buffers and replays multi-frame request bodies when the stream is rejected
/// before processing (GOAWAY / REFUSED_STREAM).
///
/// Each client-streaming call sends 10 empty `Input` messages (50 bytes of gRPC
/// frames). The h2 server reports the total received bytes in a trailing header,
/// allowing us to verify the full body was replayed on retry.
#[tokio::test]
async fn goaway_retries_client_streaming() {
    integration_tests::trace_init();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    // MAX_CONCURRENT_STREAMS=2, GOAWAY after 5 requests on first connection.
    tokio::spawn(h2_server(listener, 5, Some(2)));

    let channel = Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let client = TestClient::new(channel.clone());
    let stream_client = ClientStreamClient::new(channel);

    // Warm up: establish the connection with one successful request.
    let mut c = client.clone();
    c.unary_call(tonic::Request::new(Input {})).await.unwrap();

    // Fire concurrent client-streaming requests. With MAX_CONCURRENT_STREAMS=2,
    // most will be queued in h2's pending_open. After 4 more requests (total 5),
    // the server sends GOAWAY and queued streams are rejected. GoawayRetry
    // should replay the full multi-frame body on a fresh connection.
    let num_messages: usize = 10;
    let futs: Vec<_> = (0..50)
        .map(|_| {
            let mut c = stream_client.clone();
            tokio::spawn(async move {
                let messages = vec![Input {}; num_messages];
                let stream = tokio_stream::iter(messages);
                c.client_stream_call(tonic::Request::new(stream)).await
            })
        })
        .collect();

    let mut successes = 0;
    let mut failures = 0;
    for fut in futs {
        match fut.await.unwrap() {
            Ok(resp) => {
                successes += 1;
                // Verify the server received the complete body.
                // Each empty Input is a 5-byte gRPC frame (1B flag + 4B length).
                let received: usize = resp
                    .metadata()
                    .get("x-received-bytes")
                    .expect("server should report received bytes")
                    .to_str()
                    .unwrap()
                    .parse()
                    .unwrap();
                assert_eq!(
                    received,
                    num_messages * 5,
                    "server should receive all {num_messages} messages ({} bytes)",
                    num_messages * 5
                );
            }
            Err(e) => {
                failures += 1;
                eprintln!("  failed: {e}");
            }
        }
    }

    eprintln!("client-streaming: {successes} ok, {failures} failed");
    assert_eq!(
        failures, 0,
        "All client-streaming requests should succeed via GOAWAY retry"
    );
}

/// Serves the first `refusing` connections by taking the request and then
/// sending GOAWAY with last stream ID 0 and NO_ERROR, so the request was not
/// processed. Later connections answer with `handle_grpc`. Returns how many
/// requests reached the server.
fn refusing_server(listener: TcpListener, refusing: usize) -> Arc<AtomicUsize> {
    let requests = Arc::new(AtomicUsize::new(0));
    tokio::spawn({
        let requests = requests.clone();
        async move {
            for _ in 0..refusing {
                let (tcp, _) = listener.accept().await.unwrap();
                tokio::spawn(refuse_first_request(tcp, requests.clone()));
            }
            loop {
                let (tcp, _) = listener.accept().await.unwrap();
                let requests = requests.clone();
                tokio::spawn(async move {
                    let mut conn = server::handshake(tcp).await.unwrap();
                    while let Some(result) = conn.accept().await {
                        let (req, respond) = result.unwrap();
                        requests.fetch_add(1, Ordering::SeqCst);
                        tokio::spawn(handle_grpc(req, respond));
                    }
                });
            }
        }
    });
    requests
}

/// Speaks just enough HTTP/2 to take the first request, all of its body or
/// more than the client can replay, and send a GOAWAY that refuses it, which
/// h2's server can't do for a stream it has received.
async fn refuse_first_request(mut tcp: TcpStream, requests: Arc<AtomicUsize>) {
    const DATA: u8 = 0x0;
    const SETTINGS: u8 = 0x4;
    const PING: u8 = 0x6;
    const GOAWAY: u8 = 0x7;
    const WINDOW_UPDATE: u8 = 0x8;
    const ACK: u8 = 0x1;
    const END_STREAM: u8 = 0x1;
    const MAX_WINDOW: u32 = (1 << 31) - 1;

    async fn write_frame(
        tcp: &mut TcpStream,
        kind: u8,
        flags: u8,
        payload: &[u8],
    ) -> io::Result<()> {
        let mut frame = (payload.len() as u32).to_be_bytes()[1..].to_vec();
        frame.extend_from_slice(&[kind, flags, 0, 0, 0, 0]);
        frame.extend_from_slice(payload);
        tcp.write_all(&frame).await
    }

    let mut preface = [0; 24];
    tcp.read_exact(&mut preface).await.unwrap();
    // Let the client send its whole body without waiting for window updates.
    let mut initial_window_size = vec![0x00, 0x04];
    initial_window_size.extend_from_slice(&MAX_WINDOW.to_be_bytes());
    write_frame(&mut tcp, SETTINGS, 0, &initial_window_size)
        .await
        .unwrap();
    write_frame(
        &mut tcp,
        WINDOW_UPDATE,
        0,
        &(MAX_WINDOW - 65_535).to_be_bytes(),
    )
    .await
    .unwrap();

    let mut body_received = 0;
    let mut refused = false;
    let mut header = [0; 9];
    // Read until the client closes the connection, closing it here first with
    // unread data would reset it instead.
    while tcp.read_exact(&mut header).await.is_ok() {
        let len = u32::from_be_bytes([0, header[0], header[1], header[2]]) as usize;
        let mut payload = vec![0; len];
        if tcp.read_exact(&mut payload).await.is_err() {
            break;
        }
        let written = match (header[3], header[4]) {
            (SETTINGS, flags) if flags & ACK == 0 => {
                write_frame(&mut tcp, SETTINGS, ACK, &[]).await
            }
            (PING, flags) if flags & ACK == 0 => write_frame(&mut tcp, PING, ACK, &payload).await,
            (DATA, flags) if !refused => {
                body_received += len;
                if flags & END_STREAM != 0 || body_received > 512 * 1024 {
                    refused = true;
                    requests.fetch_add(1, Ordering::SeqCst);
                    // Last stream ID 0 and NO_ERROR.
                    write_frame(&mut tcp, GOAWAY, 0, &[0; 8]).await
                } else {
                    Ok(())
                }
            }
            _ => Ok(()),
        };
        if written.is_err() {
            break;
        }
    }
}

fn connect(listener: &TcpListener) -> Channel {
    let addr = listener.local_addr().unwrap();
    Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect_lazy()
}

/// gRFC A6: a request the server refused is sent again once.
#[tokio::test]
async fn goaway_refused_request_is_retried_once() {
    integration_tests::trace_init();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let channel = connect(&listener);
    let requests = refusing_server(listener, 1);

    let mut client = TestClient::new(channel);
    client.unary_call(Input {}).await.unwrap();
    assert_eq!(requests.load(Ordering::SeqCst), 2);
}

/// gRFC A6: only the first attempt is retried, a refused retry fails with
/// UNAVAILABLE.
#[tokio::test]
async fn goaway_refused_retry_is_unavailable() {
    integration_tests::trace_init();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let channel = connect(&listener);
    let requests = refusing_server(listener, 2);

    let mut client = TestClient::new(channel);
    let status = client.unary_call(Input {}).await.unwrap_err();
    assert_eq!(status.code(), Code::Unavailable, "{status:?}");
    assert_eq!(requests.load(Ordering::SeqCst), 2);
}

/// gRFC A6: a request that sent more body than the client buffers to replay
/// it is committed and not retried.
#[tokio::test]
async fn goaway_refused_large_request_is_unavailable() {
    integration_tests::trace_init();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let channel = connect(&listener);
    let requests = refusing_server(listener, 1);

    let mut client = Test1Client::new(channel);
    let input = Input1 {
        buf: vec![1; 1024 * 1024],
    };
    let status = client.unary_call(input).await.unwrap_err();
    assert_eq!(status.code(), Code::Unavailable, "{status:?}");
    assert_eq!(requests.load(Ordering::SeqCst), 1);
}

/// gRFC A6: a stream reset with REFUSED_STREAM is sent again once.
#[tokio::test]
async fn refused_stream_is_retried_once() {
    integration_tests::trace_init();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let channel = connect(&listener);
    let requests = Arc::new(AtomicUsize::new(0));
    tokio::spawn({
        let requests = requests.clone();
        async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut conn = server::handshake(tcp).await.unwrap();
            while let Some(result) = conn.accept().await {
                let (req, mut respond) = result.unwrap();
                if requests.fetch_add(1, Ordering::SeqCst) == 0 {
                    respond.send_reset(h2::Reason::REFUSED_STREAM);
                } else {
                    tokio::spawn(handle_grpc(req, respond));
                }
            }
        }
    });

    let mut client = TestClient::new(channel);
    client.unary_call(Input {}).await.unwrap();
    assert_eq!(requests.load(Ordering::SeqCst), 2);
}
