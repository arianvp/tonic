use crate::{
    BoxError,
    body::{self, Body},
    status::Status,
};
use bytes::{Buf, Bytes};
use http::{HeaderMap, Request, Response};
use http_body::{Frame, SizeHint};
use http_body_util::combinators::UnsyncBoxBody;
use std::{
    error::Error as StdError,
    future, mem,
    pin::Pin,
    sync::Arc,
    task::{self, Poll, ready},
};
use tokio::sync::{Mutex, OwnedMutexGuard};
use tower_service::Service;

type BoxFuture = Pin<Box<dyn future::Future<Output = Result<Response<Body>, BoxError>> + Send>>;

/// How much of a request body is kept to send the request again, like
/// grpc-go's default `MaxRetryRPCBufferSize`. A request that sends more is
/// committed to its current attempt.
const MAX_REPLAY_BUFFER_SIZE: usize = 256 * 1024;

/// How many times a request that never left the client is sent again. gRFC A6
/// retries those until the RPC's deadline, which a tonic request doesn't
/// necessarily have.
const MAX_NOT_SENT_RETRIES: usize = 16;

/// Why a request failed without being processed, the failures gRFC A6 retries
/// transparently.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Unprocessed {
    /// The request never left the client: the connection went away before
    /// hyper sent it.
    NotSent,
    /// The server refused the request without processing it: a GOAWAY's last
    /// stream ID is below the request's stream, or the stream was reset with
    /// REFUSED_STREAM.
    Refused,
}

fn unprocessed(err: &BoxError) -> Option<Unprocessed> {
    // The hyper::Error may be wrapped, e.g. in a ConnectError.
    let mut source: Option<&(dyn StdError + 'static)> = Some(err.as_ref());
    while let Some(err) = source {
        if let Some(err) = err.downcast_ref::<hyper::Error>() {
            if err.is_canceled() {
                return Some(Unprocessed::NotSent);
            }
            if err.is_protocol_nack() {
                return Some(Unprocessed::Refused);
            }
        }
        source = err.source();
    }
    None
}

/// Service layer that transparently retries requests the server never
/// processed, like grpc-go does (gRFC A6, "Transparent Retries"):
///
/// - A request that never left the client is sent again.
/// - A request the server refused is sent again once, if it was the first
///   attempt. Any retry ends the first attempt.
/// - A request is committed to its attempt once it sent more than
///   [`MAX_REPLAY_BUFFER_SIZE`] of body, or got its response headers, and is
///   not sent again after that.
///
/// A refused request that is not retried fails with `UNAVAILABLE`, see
/// `Status::from_hyper_error`.
///
/// Request bodies are buffered behind [`BodyShared`] so they can be replayed.
/// The buffer is released once the request is committed.
pub(crate) struct GoawayRetry<S> {
    inner: S,
}

impl<S: Clone> Clone for GoawayRetry<S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<S> GoawayRetry<S> {
    pub(crate) fn new(inner: S) -> Self {
        Self { inner }
    }
}

impl<S> Service<Request<Body>> for GoawayRetry<S>
where
    S: Service<Request<Body>, Response = Response<Body>, Error = BoxError> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = Response<Body>;
    type Error = BoxError;
    type Future = BoxFuture;

    fn poll_ready(&mut self, cx: &mut task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        // Make the body cloneable for potential retry.
        let (parts, body) = req.into_parts();
        let mut svc = {
            let src = self.inner.clone();
            mem::replace(&mut self.inner, src)
        };
        let body_shared = Arc::new(Mutex::new(Some(BodyShared::new(body))));

        Box::pin(async move {
            let mut first_attempt = true;
            let mut not_sent_retries = 0;
            let mut shared = body_shared.clone().lock_owned().await;

            loop {
                let req = Request::from_parts(
                    parts.clone(),
                    Body::new(BodyRef::Uncommitted(Uncommitted {
                        shared,
                        read_offset: Some(ReadOffset { index: 0, bytes: 0 }),
                    })),
                );

                let ret = svc.call(req).await;

                let Err(err) = &ret else {
                    return ret;
                };
                let retry = match unprocessed(err) {
                    Some(Unprocessed::NotSent) => not_sent_retries < MAX_NOT_SENT_RETRIES,
                    Some(Unprocessed::Refused) => first_attempt,
                    None => false,
                };
                tracing::debug!(error = %err, first_attempt, retry, "goaway_retry: call failed");
                if !retry {
                    return ret;
                }

                // The body is gone once the request is committed.
                shared = body_shared.clone().lock_owned().await;
                if shared.is_none() {
                    tracing::debug!("goaway_retry: request committed, not retrying");
                    return ret;
                }

                if unprocessed(err) == Some(Unprocessed::NotSent) {
                    not_sent_retries += 1;
                }
                first_attempt = false;
                future::poll_fn(|cx| svc.poll_ready(cx)).await?;
            }
        })
    }
}

struct BodyShared {
    frames: Vec<Bytes>,
    byte_count: usize,
    trailers: Option<HeaderMap>,
    body: Option<UnsyncBoxBody<Bytes, Status>>,
}

impl BodyShared {
    fn new(body: Body) -> Self {
        let body = match body.kind {
            body::Kind::Empty => None,
            body::Kind::Wrap(body) => Some(body),
        };

        Self {
            frames: vec![],
            byte_count: 0,
            trailers: None,
            body,
        }
    }
}

enum BodyRef {
    Uncommitted(Uncommitted),
    Owned(Option<UnsyncBoxBody<Bytes, Status>>),
}

struct Uncommitted {
    shared: OwnedMutexGuard<Option<BodyShared>>,
    read_offset: Option<ReadOffset>,
}

#[derive(Default)]
struct ReadOffset {
    index: usize,
    bytes: usize,
}

impl Unpin for BodyRef {}

impl http_body::Body for BodyRef {
    type Data = Bytes;
    type Error = Status;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let state = self.get_mut();

        let uncommitted = loop {
            let uncommitted = match state {
                BodyRef::Owned(Some(body)) => {
                    return Pin::new(body).poll_frame(cx);
                }
                BodyRef::Owned(None) => {
                    return Poll::Ready(None);
                }
                BodyRef::Uncommitted(uncommitted) => uncommitted,
            };

            let shared = uncommitted.shared.as_mut().unwrap();

            if uncommitted
                .read_offset
                .take_if(|o| o.index == shared.frames.len())
                .is_some()
                && let Some(trailers) = &shared.trailers
            {
                return Poll::Ready(Some(Ok(Frame::trailers(trailers.clone()))));
            }

            if let Some(offset) = &mut uncommitted.read_offset {
                let buf = shared.frames[offset.index].clone();
                offset.index += 1;
                offset.bytes += buf.len();
                return Poll::Ready(Some(Ok(Frame::data(buf))));
            }

            if Arc::strong_count(OwnedMutexGuard::mutex(&uncommitted.shared)) == 1 {
                // commit: we hold the last reference
                *state = BodyRef::Owned(uncommitted.shared.take().unwrap().body);
                continue;
            }

            break uncommitted;
        };

        let shared = uncommitted.shared.as_mut().unwrap();

        let frame = match &mut shared.body {
            None => None,
            Some(body) => ready!(Pin::new(body).poll_frame(cx)),
        };

        let frame = match frame {
            Some(Ok(frame)) => {
                if shared.body.as_ref().is_some_and(|b| b.is_end_stream()) {
                    // body exhausted, drop it
                    shared.body = None;
                }

                frame
            }
            Some(Err(err)) => {
                // commit: body errored
                *state = BodyRef::Owned(uncommitted.shared.take().unwrap().body);
                return Poll::Ready(Some(Err(err)));
            }
            None => {
                // body exhausted, drop it
                shared.body = None;
                return Poll::Ready(None);
            }
        };

        if let Some(data) = frame.data_ref() {
            if let Some(new_len) = data.remaining().checked_add(shared.byte_count)
                && new_len <= MAX_REPLAY_BUFFER_SIZE
            {
                shared.byte_count = new_len;
                shared.frames.push(data.clone());
            } else {
                // commit: exceeded max buffer size
                *state = BodyRef::Owned(uncommitted.shared.take().unwrap().body);
            }
        } else if let Some(trailers) = frame.trailers_ref() {
            assert!(shared.trailers.is_none());
            shared.trailers = Some(trailers.clone());
        } else {
            // commit: unknown frame type
            *state = BodyRef::Owned(uncommitted.shared.take().unwrap().body);
        }

        Poll::Ready(Some(Ok(frame)))
    }

    fn is_end_stream(&self) -> bool {
        match self {
            BodyRef::Uncommitted(uncommitted) => {
                let shared = uncommitted.shared.as_ref().unwrap();
                uncommitted.read_offset.is_none()
                    && shared.body.as_ref().is_none_or(|b| b.is_end_stream())
            }
            BodyRef::Owned(Some(body)) => body.is_end_stream(),
            BodyRef::Owned(None) => true,
        }
    }

    fn size_hint(&self) -> http_body::SizeHint {
        let (bytes, body) = match self {
            BodyRef::Uncommitted(uncommitted) => {
                let shared = uncommitted.shared.as_ref().unwrap();
                let buffered = match &uncommitted.read_offset {
                    Some(offset) => shared.byte_count - offset.bytes,
                    None => 0,
                };
                (buffered as u64, &shared.body)
            }
            BodyRef::Owned(body) => (0, body),
        };

        let body = body
            .as_ref()
            .map_or(SizeHint::with_exact(0), |b| b.size_hint());

        let mut hint = SizeHint::new();

        hint.set_lower(body.lower().saturating_add(bytes));
        if let Some(body_upper) = body.upper()
            && let Some(upper) = body_upper.checked_add(bytes)
        {
            hint.set_upper(upper);
        }

        hint
    }
}
