//! The Lotse native protocol.
//!
//! A frame is what a `lotse_rt::Conn` carries in one piece; its length comes from the transport (a `u32` prefix on TCP), so
//! the layouts here start after it:
//!
//! ```text
//! Request:  api_key u16 | api_version u16 | correlation_id u32 | flags u16 | tagged_fields | body
//! Response: correlation_id u32 | flags u16 | tagged_fields | body
//! ```
//!
//! Requests on a connection are pipelined; responses carry the request's
//! correlation id and may come back in another order. Every structure ends
//! with tagged fields, so later versions can add fields old readers skip.
//! Field conventions are in [`codec`].
//!
//! The first cut has `ApiVersions`, `Metadata`, `Produce` and `Fetch`, all
//! at version 0.

#![warn(missing_docs)]

pub mod codec;
mod messages;

pub use messages::*;

use bytes::Bytes;
use codec::{Reader, Writer};

/// A frame that cannot be decoded.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DecodeError {
    /// The frame ends inside a field.
    #[error("frame is truncated")]
    Truncated,
    /// A field holds something no version allows.
    #[error("malformed frame: {0}")]
    Malformed(&'static str),
    /// The request header is fine, but this node does not know the API key
    /// or version: answer with [`ErrorCode::UNSUPPORTED_VERSION`].
    #[error("unsupported API key {} version {}", .0.api_key.0, .0.api_version)]
    Unsupported(RequestHeader),
}

/// An API key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ApiKey(pub u16);

impl ApiKey {
    /// Versions and features of the node.
    pub const API_VERSIONS: ApiKey = ApiKey(0);
    /// Nodes, resources and partition leaders.
    pub const METADATA: ApiKey = ApiKey(1);
    /// Reserved: the coordinator of a group or transaction.
    pub const FIND_COORDINATOR: ApiKey = ApiKey(2);
    /// Append batches to stream partitions.
    pub const PRODUCE: ApiKey = ApiKey(3);
    /// Read batches from stream partitions.
    pub const FETCH: ApiKey = ApiKey(4);

    /// The keys this crate encodes, with their version range.
    pub const SUPPORTED: [ApiVersionRange; 4] = [
        ApiVersionRange { api_key: ApiKey::API_VERSIONS, min: 0, max: 0 },
        ApiVersionRange { api_key: ApiKey::METADATA, min: 0, max: 0 },
        ApiVersionRange { api_key: ApiKey::PRODUCE, min: 0, max: 0 },
        ApiVersionRange { api_key: ApiKey::FETCH, min: 0, max: 0 },
    ];
}

/// The versions a node speaks for one API key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApiVersionRange {
    /// The key.
    pub api_key: ApiKey,
    /// Oldest version.
    pub min: u16,
    /// Newest version.
    pub max: u16,
}

/// An error code; every partition in a response carries its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ErrorCode(pub u16);

impl ErrorCode {
    /// Success.
    pub const NONE: ErrorCode = ErrorCode(0);
    /// Wrong node; the response carries a [`LeaderHint`] if one is known.
    pub const NOT_LEADER: ErrorCode = ErrorCode(1);
    /// The client's leader term is outdated: reload metadata.
    pub const FENCED_LEADER_TERM: ErrorCode = ErrorCode(2);
    /// The id does not exist (any more).
    pub const UNKNOWN_RESOURCE: ErrorCode = ErrorCode(3);
    /// Offset before the log start or after the leader's log end.
    pub const OFFSET_OUT_OF_RANGE: ErrorCode = ErrorCode(4);
    /// No quorum reachable.
    pub const NOT_ENOUGH_REPLICAS: ErrorCode = ErrorCode(5);
    /// No quorum confirmation within `timeout_ms`; the batch may or may not
    /// have been written.
    pub const REQUEST_TIMED_OUT: ErrorCode = ErrorCode(6);
    /// Gap in the producer sequence.
    pub const OUT_OF_ORDER_SEQUENCE: ErrorCode = ErrorCode(7);
    /// A newer producer epoch is active.
    pub const PRODUCER_FENCED: ErrorCode = ErrorCode(8);
    /// The operation does not fit the transaction's state.
    pub const INVALID_TXN_STATE: ErrorCode = ErrorCode(9);
    /// The tenant's storage quota is used up.
    pub const QUOTA_EXCEEDED: ErrorCode = ErrorCode(10);
    /// The node's hard storage limit is reached.
    pub const STORAGE_FULL: ErrorCode = ErrorCode(11);
    /// CRC or format invalid.
    pub const CORRUPT_BATCH: ErrorCode = ErrorCode(12);
    /// Access denied.
    pub const UNAUTHORIZED: ErrorCode = ErrorCode(13);
    /// API version outside the node's range.
    pub const UNSUPPORTED_VERSION: ErrorCode = ErrorCode(14);

    /// The name used in documentation and logs.
    pub fn name(self) -> &'static str {
        const NAMES: [&str; 15] = [
            "NONE",
            "NOT_LEADER",
            "FENCED_LEADER_TERM",
            "UNKNOWN_RESOURCE",
            "OFFSET_OUT_OF_RANGE",
            "NOT_ENOUGH_REPLICAS",
            "REQUEST_TIMED_OUT",
            "OUT_OF_ORDER_SEQUENCE",
            "PRODUCER_FENCED",
            "INVALID_TXN_STATE",
            "QUOTA_EXCEEDED",
            "STORAGE_FULL",
            "CORRUPT_BATCH",
            "UNAUTHORIZED",
            "UNSUPPORTED_VERSION",
        ];
        NAMES.get(usize::from(self.0)).copied().unwrap_or("UNKNOWN")
    }

    /// Whether the same request may succeed later (after reloading metadata).
    pub fn retriable(self) -> bool {
        matches!(
            self,
            ErrorCode::NOT_LEADER
                | ErrorCode::FENCED_LEADER_TERM
                | ErrorCode::NOT_ENOUGH_REPLICAS
                | ErrorCode::REQUEST_TIMED_OUT
        )
    }
}

impl std::fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({})", self.name(), self.0)
    }
}

/// Frame flags.
pub mod flags {
    /// A push frame (delivery to a queue consumer), not a response.
    pub const PUSH: u16 = 1;
    /// A response whose body is only an error code: the request could not be
    /// served at all.
    pub const ERROR: u16 = 2;
}

/// The header of a request frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestHeader {
    /// What is asked.
    pub api_key: ApiKey,
    /// In which version.
    pub api_version: u16,
    /// Echoed in the response; `0` is reserved for push frames.
    pub correlation_id: u32,
    /// [`flags`].
    pub flags: u16,
}

/// A request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request {
    /// [`ApiKey::API_VERSIONS`].
    ApiVersions,
    /// [`ApiKey::METADATA`].
    Metadata(MetadataRequest),
    /// [`ApiKey::PRODUCE`].
    Produce(ProduceRequest),
    /// [`ApiKey::FETCH`].
    Fetch(FetchRequest),
}

impl Request {
    /// The request's key.
    pub fn api_key(&self) -> ApiKey {
        match self {
            Request::ApiVersions => ApiKey::API_VERSIONS,
            Request::Metadata(_) => ApiKey::METADATA,
            Request::Produce(_) => ApiKey::PRODUCE,
            Request::Fetch(_) => ApiKey::FETCH,
        }
    }
}

/// A response; which variant follows from the request it answers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Response {
    /// To [`Request::ApiVersions`].
    ApiVersions(ApiVersionsResponse),
    /// To [`Request::Metadata`].
    Metadata(MetadataResponse),
    /// To [`Request::Produce`].
    Produce(ProduceResponse),
    /// To [`Request::Fetch`].
    Fetch(FetchResponse),
    /// The request could not be served at all (unsupported version, or a
    /// body that does not decode); the node closes the connection after it.
    Error(ErrorCode),
}

/// Encodes a version-0 request.
pub fn encode_request(correlation_id: u32, request: &Request) -> Bytes {
    let mut w = Writer::new();
    w.u16(request.api_key().0);
    w.u16(0);
    w.u32(correlation_id);
    w.u16(0);
    w.no_tags();
    match request {
        Request::ApiVersions => w.no_tags(),
        Request::Metadata(r) => r.put(&mut w),
        Request::Produce(r) => r.put(&mut w),
        Request::Fetch(r) => r.put(&mut w),
    }
    w.finish()
}

/// Decodes a request. A header this node does not speak gives
/// [`DecodeError::Unsupported`], so it can still answer.
pub fn decode_request(frame: Bytes) -> Result<(RequestHeader, Request), DecodeError> {
    let mut r = Reader::new(frame);
    let header =
        RequestHeader { api_key: ApiKey(r.u16()?), api_version: r.u16()?, correlation_id: r.u32()?, flags: r.u16()? };
    r.tags()?;
    let supported =
        ApiKey::SUPPORTED.iter().any(|v| v.api_key == header.api_key && (v.min..=v.max).contains(&header.api_version));
    if !supported {
        return Err(DecodeError::Unsupported(header));
    }
    let request = match header.api_key {
        ApiKey::API_VERSIONS => {
            r.tags()?;
            Request::ApiVersions
        }
        ApiKey::METADATA => Request::Metadata(MetadataRequest::get(&mut r)?),
        ApiKey::PRODUCE => Request::Produce(ProduceRequest::get(&mut r)?),
        ApiKey::FETCH => Request::Fetch(FetchRequest::get(&mut r)?),
        _ => unreachable!("checked against SUPPORTED"),
    };
    r.finish()?;
    Ok((header, request))
}

/// Encodes a response to the request with `correlation_id`.
pub fn encode_response(correlation_id: u32, response: &Response) -> Bytes {
    let mut w = Writer::new();
    w.u32(correlation_id);
    w.u16(if matches!(response, Response::Error(_)) { flags::ERROR } else { 0 });
    w.no_tags();
    match response {
        Response::ApiVersions(r) => r.put(&mut w),
        Response::Metadata(r) => r.put(&mut w),
        Response::Produce(r) => r.put(&mut w),
        Response::Fetch(r) => r.put(&mut w),
        Response::Error(code) => {
            w.u16(code.0);
            w.no_tags();
        }
    }
    w.finish()
}

/// The correlation id of a response frame, to find the request it answers.
pub fn response_correlation_id(frame: &[u8]) -> Result<u32, DecodeError> {
    let id = frame.get(..4).ok_or(DecodeError::Truncated)?;
    Ok(u32::from_le_bytes(id.try_into().expect("four bytes")))
}

/// Decodes a response to a request of type `api_key`.
pub fn decode_response(frame: Bytes, api_key: ApiKey) -> Result<(u32, Response), DecodeError> {
    let mut r = Reader::new(frame);
    let correlation_id = r.u32()?;
    let flags = r.u16()?;
    r.tags()?;
    if flags & flags::ERROR != 0 {
        let code = ErrorCode(r.u16()?);
        r.tags()?;
        r.finish()?;
        return Ok((correlation_id, Response::Error(code)));
    }
    let response = match api_key {
        ApiKey::API_VERSIONS => Response::ApiVersions(ApiVersionsResponse::get(&mut r)?),
        ApiKey::METADATA => Response::Metadata(MetadataResponse::get(&mut r)?),
        ApiKey::PRODUCE => Response::Produce(ProduceResponse::get(&mut r)?),
        ApiKey::FETCH => Response::Fetch(FetchResponse::get(&mut r)?),
        _ => return Err(DecodeError::Malformed("unknown API key")),
    };
    r.finish()?;
    Ok((correlation_id, response))
}

#[cfg(test)]
mod tests;
