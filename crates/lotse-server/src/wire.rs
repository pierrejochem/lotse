//! The node-to-node wire format for Raft.
//!
//! One frame carries every message one node has for another in a round, for
//! all groups (Multi-Raft batching):
//!
//! ```text
//! frame    = version:u8 (1)  from:u64  count:u32  message*
//! message  = group:u64  kind:u8  body
//! Vote        term last_index last_term:u64  flags:u8 (1 = pre, 2 = transfer)
//! VoteReply   term:u64  flags:u8 (1 = granted, 2 = pre)
//! Append      term prev_index prev_term commit seq:u64  n:u32  (len:u32 batch)*
//! AppendReply term seq:u64  tag:u8 (0 = accepted: last:u64,
//!             1 = rejected: prev_index last_index:u64 has_conflict:u8 term first:u64)
//! Snapshot    term seq index snapshot_term:u64  len:u32 membership  len:u32 data
//! TimeoutNow  term:u64
//! ```
//!
//! Integers are little-endian. Entries travel as their stored record batches
//! (stamped with offset and term): the sender passes them on as read from
//! its log, the receiver parses them in place, so record bytes are never
//! re-encoded. A snapshot must fit in one frame for now.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use lotse_raft::{AppendResult, Membership, Message, NodeId, SnapshotMeta};
use lotse_record::Batch;

use crate::replica::{to_batch, to_entry};

/// A Raft group on the wire.
pub type GroupId = u64;

const VERSION: u8 = 1;

/// A frame that cannot be decoded.
#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    /// The frame ends inside a field.
    #[error("frame is truncated")]
    Truncated,
    /// Unknown version or message kind, or a malformed field.
    #[error("malformed frame: {0}")]
    Malformed(&'static str),
    /// An entry is not a valid batch.
    #[error("bad entry: {0}")]
    Entry(String),
}

/// Encodes the messages `from` has for one destination.
pub fn encode(from: NodeId, messages: &[(GroupId, Message)]) -> Bytes {
    let mut b = BytesMut::with_capacity(13 + messages.len() * 48);
    b.put_u8(VERSION);
    b.put_u64_le(from);
    b.put_u32_le(messages.len() as u32);
    for (group, msg) in messages {
        b.put_u64_le(*group);
        match msg {
            Message::Vote { term, last_index, last_term, pre, transfer } => {
                b.put_u8(1);
                b.put_u64_le(*term);
                b.put_u64_le(*last_index);
                b.put_u64_le(*last_term);
                b.put_u8(u8::from(*pre) | u8::from(*transfer) << 1);
            }
            Message::VoteReply { term, granted, pre } => {
                b.put_u8(2);
                b.put_u64_le(*term);
                b.put_u8(u8::from(*granted) | u8::from(*pre) << 1);
            }
            Message::Append { term, prev_index, prev_term, entries, commit, seq } => {
                b.put_u8(3);
                for v in [*term, *prev_index, *prev_term, *commit, *seq] {
                    b.put_u64_le(v);
                }
                b.put_u32_le(entries.len() as u32);
                for e in entries {
                    let batch = to_batch(e).expect("an entry the driver produced encodes as a batch");
                    b.put_u32_le(batch.len() as u32);
                    b.put_slice(&batch);
                }
            }
            Message::AppendReply { term, result, seq } => {
                b.put_u8(4);
                b.put_u64_le(*term);
                b.put_u64_le(*seq);
                match result {
                    AppendResult::Accepted { last } => {
                        b.put_u8(0);
                        b.put_u64_le(*last);
                    }
                    AppendResult::Rejected { prev_index, last_index, conflict } => {
                        b.put_u8(1);
                        b.put_u64_le(*prev_index);
                        b.put_u64_le(*last_index);
                        b.put_u8(u8::from(conflict.is_some()));
                        let (t, f) = conflict.unwrap_or((0, 0));
                        b.put_u64_le(t);
                        b.put_u64_le(f);
                    }
                }
            }
            Message::Snapshot { term, meta, data, seq } => {
                b.put_u8(5);
                for v in [*term, *seq, meta.index, meta.term] {
                    b.put_u64_le(v);
                }
                let m = meta.membership.encode();
                b.put_u32_le(m.len() as u32);
                b.put_slice(&m);
                b.put_u32_le(data.len() as u32);
                b.put_slice(data);
            }
            Message::TimeoutNow { term } => {
                b.put_u8(6);
                b.put_u64_le(*term);
            }
        }
    }
    b.freeze()
}

struct Reader(Bytes);

impl Reader {
    fn need(&self, n: usize) -> Result<(), DecodeError> {
        if self.0.remaining() < n { Err(DecodeError::Truncated) } else { Ok(()) }
    }
    fn u8(&mut self) -> Result<u8, DecodeError> {
        self.need(1)?;
        Ok(self.0.get_u8())
    }
    fn u32(&mut self) -> Result<u32, DecodeError> {
        self.need(4)?;
        Ok(self.0.get_u32_le())
    }
    fn u64(&mut self) -> Result<u64, DecodeError> {
        self.need(8)?;
        Ok(self.0.get_u64_le())
    }
    fn bytes(&mut self) -> Result<Bytes, DecodeError> {
        let n = self.u32()? as usize;
        self.need(n)?;
        Ok(self.0.split_to(n))
    }
}

/// Decodes a frame: the sender and its messages.
pub fn decode(frame: Bytes) -> Result<(NodeId, Vec<(GroupId, Message)>), DecodeError> {
    let mut r = Reader(frame);
    if r.u8()? != VERSION {
        return Err(DecodeError::Malformed("unknown version"));
    }
    let from = r.u64()?;
    let count = r.u32()? as usize;
    // Every message takes at least 17 bytes; refuse counts the frame cannot hold.
    if count > r.0.remaining() / 17 + 1 {
        return Err(DecodeError::Malformed("message count exceeds the frame"));
    }
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        let group = r.u64()?;
        let msg = match r.u8()? {
            1 => {
                let (term, last_index, last_term) = (r.u64()?, r.u64()?, r.u64()?);
                let flags = r.u8()?;
                Message::Vote { term, last_index, last_term, pre: flags & 1 != 0, transfer: flags & 2 != 0 }
            }
            2 => {
                let term = r.u64()?;
                let flags = r.u8()?;
                Message::VoteReply { term, granted: flags & 1 != 0, pre: flags & 2 != 0 }
            }
            3 => {
                let (term, prev_index, prev_term, commit, seq) = (r.u64()?, r.u64()?, r.u64()?, r.u64()?, r.u64()?);
                let n = r.u32()? as usize;
                let mut entries = Vec::with_capacity(n.min(1024));
                for _ in 0..n {
                    let bytes = r.bytes()?;
                    let (batch, rest) = Batch::parse(&bytes).map_err(|e| DecodeError::Entry(e.to_string()))?;
                    if !rest.is_empty() {
                        return Err(DecodeError::Malformed("trailing bytes after an entry"));
                    }
                    entries.push(to_entry(&batch).map_err(|e| DecodeError::Entry(e.to_string()))?);
                }
                Message::Append { term, prev_index, prev_term, entries, commit, seq }
            }
            4 => {
                let (term, seq) = (r.u64()?, r.u64()?);
                let result = match r.u8()? {
                    0 => AppendResult::Accepted { last: r.u64()? },
                    1 => {
                        let (prev_index, last_index) = (r.u64()?, r.u64()?);
                        let has = r.u8()? != 0;
                        let (t, f) = (r.u64()?, r.u64()?);
                        AppendResult::Rejected { prev_index, last_index, conflict: has.then_some((t, f)) }
                    }
                    _ => return Err(DecodeError::Malformed("unknown append result")),
                };
                Message::AppendReply { term, result, seq }
            }
            5 => {
                let (term, seq, index, snapshot_term) = (r.u64()?, r.u64()?, r.u64()?, r.u64()?);
                let membership = Membership::decode(&r.bytes()?).ok_or(DecodeError::Malformed("bad membership"))?;
                let data = r.bytes()?;
                Message::Snapshot { term, meta: SnapshotMeta { index, term: snapshot_term, membership }, data, seq }
            }
            6 => Message::TimeoutNow { term: r.u64()? },
            _ => return Err(DecodeError::Malformed("unknown message kind")),
        };
        out.push((group, msg));
    }
    if r.0.has_remaining() {
        return Err(DecodeError::Malformed("trailing bytes after the last message"));
    }
    Ok((from, out))
}

#[cfg(test)]
mod tests {
    use lotse_raft::{Entry, EntryKind};
    use lotse_record::{BatchBuilder, BatchOptions};

    use super::*;

    fn data_entry(first: u64, records: u64, term: u64) -> Entry {
        let mut b = BatchBuilder::new(BatchOptions::default());
        for _ in 0..records {
            b.push(0, None, Some(b"v"), &[]);
        }
        let mut raw = BytesMut::from(&b.finish().unwrap()[..]);
        lotse_record::stamp(&mut raw, first - 1, term).unwrap();
        Entry { first, last: first + records - 1, term, kind: EntryKind::Normal, data: raw.freeze() }
    }

    fn every_message() -> Vec<(GroupId, Message)> {
        let membership = Membership::of_voters([1, 2, 3]);
        let entries = vec![
            Entry { first: 1, last: 1, term: 2, kind: EntryKind::Noop, data: Bytes::new() },
            data_entry(2, 3, 2),
            Entry { first: 5, last: 5, term: 2, kind: EntryKind::Config, data: membership.encode() },
        ];
        vec![
            (7, Message::Vote { term: 3, last_index: 9, last_term: 2, pre: true, transfer: false }),
            (7, Message::Vote { term: 4, last_index: 9, last_term: 2, pre: false, transfer: true }),
            (8, Message::VoteReply { term: 3, granted: true, pre: true }),
            (9, Message::Append { term: 2, prev_index: 0, prev_term: 0, entries, commit: 1, seq: 11 }),
            (9, Message::Append { term: 2, prev_index: 5, prev_term: 2, entries: vec![], commit: 5, seq: 12 }),
            (1, Message::AppendReply { term: 2, result: AppendResult::Accepted { last: 5 }, seq: 12 }),
            (
                1,
                Message::AppendReply {
                    term: 2,
                    result: AppendResult::Rejected { prev_index: 4, last_index: 3, conflict: Some((1, 2)) },
                    seq: 13,
                },
            ),
            (
                1,
                Message::AppendReply {
                    term: 2,
                    result: AppendResult::Rejected { prev_index: 4, last_index: 3, conflict: None },
                    seq: 0,
                },
            ),
            (
                2,
                Message::Snapshot {
                    term: 5,
                    meta: SnapshotMeta { index: 40, term: 4, membership },
                    data: Bytes::from_static(b"state"),
                    seq: 3,
                },
            ),
            (3, Message::TimeoutNow { term: 6 }),
        ]
    }

    #[test]
    fn every_message_round_trips() {
        let messages = every_message();
        let (from, back) = decode(encode(42, &messages)).unwrap();
        assert_eq!(from, 42);
        assert_eq!(back, messages);
    }

    #[test]
    fn stamped_entries_are_sent_without_copying() {
        let e = data_entry(10, 2, 3);
        let ptr = e.data.as_ptr();
        assert_eq!(to_batch(&e).unwrap().as_ptr(), ptr, "the batch as read from the log goes out as is");
    }

    #[test]
    fn a_batch_stamped_otherwise_is_restamped_with_the_entry() {
        // A client may send a batch that already carries the right offsets
        // but another term (or other offsets); it must go out as the entry says.
        let mut e = data_entry(10, 2, 3);
        e.term = 4;
        let sent = to_batch(&e).unwrap();
        let (b, _) = Batch::parse(&sent).unwrap();
        assert_eq!((b.base_offset(), b.last_offset(), b.term()), (9, 10, 4));
        let mut e = data_entry(10, 2, 3);
        e.first = 20;
        e.last = 21;
        let (b, _) = Batch::parse(&to_batch(&e).unwrap()).unwrap();
        assert_eq!((b.base_offset(), b.last_offset(), b.term()), (19, 20, 3));
    }

    #[test]
    fn damaged_frames_are_refused_not_misread() {
        let frame = encode(1, &every_message());
        for cut in [0, 1, 5, 12, 20, frame.len() / 2, frame.len() - 1] {
            assert!(decode(frame.slice(..cut)).is_err(), "cut at {cut}");
        }
        let mut bad = BytesMut::from(&frame[..]);
        bad[0] = 9;
        assert!(matches!(decode(bad.freeze()), Err(DecodeError::Malformed(_))));
        let mut huge = BytesMut::from(&encode(1, &[])[..]);
        huge[9..13].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(decode(huge.freeze()).is_err(), "an absurd count is refused before allocating");
        let mut longer = BytesMut::from(&frame[..]);
        longer.put_u8(0);
        assert!(decode(longer.freeze()).is_err(), "trailing bytes");
    }
}
