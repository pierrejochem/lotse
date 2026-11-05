//! `ListOffsets` (ADR 0059): where a partition starts, where a reader's view
//! of it ends, and where a point in time falls in it.
//!
//! Only the leader answers, as for `Fetch`, with the same bounds: the log
//! start, the high watermark and, for read_committed, the last stable
//! offset. A time is found through the segments' newest timestamps, which
//! the log keeps anyway, and a scan of batch headers from the first segment
//! that may hold it; there is no time index.

use std::collections::BTreeMap;
use std::sync::Arc;

use lotse_log::Log;
use lotse_proto::{
    ErrorCode, ListOffsetsPartitionResponse, ListOffsetsRequest, ListOffsetsResourceResponse, ListOffsetsResponse,
    Response, isolation, list_offsets,
};
use lotse_raft::Role;
use lotse_record::{Batch, BatchKind};
use lotse_rt::{Disk, Runtime};

use super::{Clients, ClusterView, Outbox, hint};
use crate::replica::{Error, Replica, StateMachine};
use crate::wire::GroupId;

/// Bytes read from the log at a time while looking for a timestamp.
const SCAN_BYTES: usize = 1 << 20;

impl Clients {
    pub(super) async fn list_offsets<R: Runtime, S: StateMachine>(
        &mut self,
        outbox: Arc<Outbox>,
        id: u32,
        req: ListOffsetsRequest,
        view: &ClusterView<'_>,
        groups: &BTreeMap<GroupId, Replica<R, S>>,
    ) -> Result<(), Error> {
        let mut response = ListOffsetsResponse { resources: Vec::new() };
        for res in req.resources {
            let mut partitions = Vec::new();
            for p in res.partitions {
                let mut out = ListOffsetsPartitionResponse {
                    partition: p.partition,
                    error: ErrorCode::NONE,
                    offset: 0,
                    timestamp: -1,
                    leader_hint: None,
                };
                let group = view.stream_group_of(res.resource_id, p.partition);
                match group.and_then(|g| groups.get(&g)) {
                    None => out.error = view.absent(res.resource_id, group),
                    Some(replica) if replica.raft_ref().role() != Role::Leader => {
                        out.error = ErrorCode::NOT_LEADER;
                        out.leader_hint = hint(replica.raft_ref());
                    }
                    Some(_) if p.timestamp < list_offsets::EARLIEST => out.error = ErrorCode::INVALID_REQUEST,
                    Some(replica) => {
                        // The same bounds as `Fetch`.
                        let hw = replica.raft_ref().commit();
                        let lso = replica.txns().last_stable_offset(replica.applied()).min(hw);
                        let end = if req.isolation == isolation::COMMITTED { lso } else { hw };
                        let start = replica.log().start_offset();
                        match p.timestamp {
                            list_offsets::EARLIEST => out.offset = start,
                            list_offsets::LATEST => out.offset = end,
                            at => match by_time(replica.log(), at, end).await? {
                                Some((offset, timestamp)) => (out.offset, out.timestamp) = (offset, timestamp),
                                None => out.offset = end,
                            },
                        }
                    }
                }
                partitions.push(out);
            }
            response.resources.push(ListOffsetsResourceResponse { resource_id: res.resource_id, partitions });
        }
        outbox.push(id, &Response::ListOffsets(response));
        Ok(())
    }
}

/// The first data batch below `end` whose newest record is at or after `at`:
/// its first offset and that record's timestamp. Segments whose newest
/// record is older are skipped whole.
pub(crate) async fn by_time<D: Disk>(log: &Log<D>, at: i64, end: u64) -> Result<Option<(u64, i64)>, Error> {
    let start = log.start_offset();
    let candidates: Vec<_> =
        log.segments().filter(|s| s.max_timestamp >= at && s.next_offset > start && s.base_offset < end).collect();
    for seg in candidates {
        let mut offset = seg.base_offset.max(start);
        while offset < seg.next_offset.min(end) {
            let mut bytes = log.read(offset, SCAN_BYTES).await?;
            if bytes.is_empty() {
                break;
            }
            while !bytes.is_empty() {
                let (batch, rest) = Batch::parse(&bytes)?;
                let first = batch.base_offset();
                if first >= end {
                    return Ok(None);
                }
                let readable = !batch.attributes().control
                    && matches!(batch.attributes().kind, BatchKind::Data | BatchKind::DeadLetter);
                if readable && first >= start && batch.max_timestamp() >= at {
                    return Ok(Some((first, batch.max_timestamp())));
                }
                offset = first + u64::from(batch.record_count());
                bytes = rest;
            }
        }
    }
    Ok(None)
}
