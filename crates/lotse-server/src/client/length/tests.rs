use super::*;

#[test]
fn what_a_new_leader_has_not_read_counts_as_much_as_it_can_be() {
    let mut q = lotse_queue::QueueLog::new();
    let mut b = lotse_record::BatchBuilder::new(lotse_record::BatchOptions::default());
    for _ in 0..10 {
        b.push(0, None, Some(b"m"), &[]);
    }
    let mut raw = bytes::BytesMut::from(&b.finish().unwrap()[..]);
    lotse_record::stamp(&mut raw, 0, 1).unwrap();
    q.apply(&Batch::parse(&raw.freeze()).unwrap().0).unwrap();
    let mut ack = bytes::BytesMut::from(&lotse_record::ack_batch(&[(2, 5)]).unwrap()[..]);
    lotse_record::stamp(&mut ack, 10, 1).unwrap();
    q.apply(&Batch::parse(&ack.freeze()).unwrap().0).unwrap();
    // Segments [0, 8) of 300 bytes and [8, 20) of 500.
    let segments = || [(8, 300), (20, 500)].into_iter();
    // Nothing read, offsets 0..11 applied (three settled, and the ack),
    // 11..20 appended.
    assert_eq!(unread_of(0, 11, 20, &q, segments()), Unread { messages: 7 + 9, bytes: 800 });
    // Read up to 9: the second segment and what follows.
    assert_eq!(unread_of(9, 11, 20, &q, segments()), Unread { messages: 1 + 9, bytes: 500 });
    // Read past what is applied.
    assert_eq!(unread_of(15, 11, 20, &q, segments()), Unread { messages: 5, bytes: 500 });
    // Everything read.
    assert_eq!(unread_of(20, 11, 20, &q, segments()), Unread::default());
}
