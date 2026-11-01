//! Writes the golden vectors the client SDKs test against.
//!
//! ```text
//! cargo run -p lotse-proto --features samples --example vectors -- sdk/conformance
//! ```
//!
//! The output is deterministic: the same code gives the same bytes, which is
//! what `make sdk-vectors-check` relies on. Not part of the broker.
//!
//! Files (UTF-8 text, one vector per line, `#` starts a comment):
//!
//! ```text
//! requests.hex      <label> <hex of the frame, without the length prefix>
//! responses.hex     <label> <api key> <hex>
//! pushes.hex        <label> <hex>
//! batches.hex       <label> <hex of one batch as stored>
//! batch-fields.txt  <label> <field>=<value> ...
//! batch-records.txt <label> <index> <offset> <timestamp> <key> <value> <headers>
//! batch-ops.txt     <op> <arguments> <input hex> <output hex>
//! fetch-records.hex <label> <hex of several batches in a row>
//! varints.txt       <kind> <value> <hex>
//! crc32c.txt        <label> <hex of the input> <crc as 8 hex digits>
//! error-codes.txt   <code> <name> <retriable>
//! scram.txt         <label> <user hex> <password> <salt hex> <iterations> <client nonce> <server nonce>
//!                   <client first hex> <server first hex> <client final hex> <server final hex>
//!                   <stored key hex> <server key hex>
//! ```

use std::fmt::Write as _;
use std::path::PathBuf;

use bytes::Bytes;
use lotse_auth::scram::{ScramClient, ScramCredential, ScramServer};
use lotse_proto::samples;
use lotse_proto::{ApiKey, ErrorCode, Response, decode_request, encode_push, encode_request, encode_response};
use lotse_record::{
    Attributes, Batch, BatchBuilder, BatchKind, BatchOptions, Compression, ControlType, Record, dead_letter, varint,
};

fn hex(b: &[u8]) -> String {
    let mut s = String::with_capacity(b.len() * 2);
    for x in b {
        write!(s, "{x:02x}").expect("writing to a string");
    }
    s
}

/// `-` for null, `=` for empty, `=<hex>` for bytes.
fn nullable(b: Option<&[u8]>) -> String {
    match b {
        None => "-".to_string(),
        Some(b) => format!("={}", hex(b)),
    }
}

/// The variant name of a debug string: `Metadata(..)` gives `Metadata`.
fn variant(debug: &str) -> String {
    let end = debug.find(['(', ' ', '{']).unwrap_or(debug.len());
    debug[..end].to_string()
}

struct Out {
    dir: PathBuf,
}

impl Out {
    fn write(&self, name: &str, comment: &str, lines: &[String]) {
        let mut text = String::new();
        for line in comment.lines() {
            writeln!(text, "# {line}").expect("writing to a string");
        }
        for line in lines {
            writeln!(text, "{line}").expect("writing to a string");
        }
        std::fs::write(self.dir.join(name), text).unwrap_or_else(|e| panic!("writing {name}: {e}"));
    }
}

const TS: i64 = 1_700_000_000_000;

fn options(transactional: bool, producer_id: u64, producer_epoch: u16, base_sequence: i32) -> BatchOptions {
    BatchOptions {
        attributes: Attributes { compression: Compression::None, transactional, control: false, kind: BatchKind::Data },
        producer_id,
        producer_epoch,
        base_sequence,
    }
}

fn stamped(batch: Bytes, base_offset: u64, term: u64) -> Bytes {
    let mut v = batch.to_vec();
    lotse_record::stamp(&mut v, base_offset, term).expect("a whole batch");
    Bytes::from(v)
}

fn batches() -> Vec<(String, Bytes)> {
    let mut out: Vec<(String, Bytes)> = Vec::new();
    let mut add = |label: &str, b: Bytes| out.push((label.to_string(), b));

    // Keys, values, headers, tombstones, empty fields and a timestamp before the base.
    let mut b = BatchBuilder::new(BatchOptions::default());
    b.push(TS, Some(b"k1"), Some(b"v1"), &[(b"h", Some(b"x")), (b"n", None)]);
    b.push(TS + 5, None, None, &[]);
    b.push(TS - 1, Some(b""), Some(b""), &[(b"", Some(b""))]);
    add("data-stamped", stamped(b.finish().expect("batch"), 100, 7));

    let mut b = BatchBuilder::new(BatchOptions::default());
    b.push(TS, None, Some(b"only"), &[]);
    add("data-single", b.finish().expect("batch"));

    let mut b = BatchBuilder::new(options(false, 17, 2, 5));
    for i in 0..3u8 {
        b.push(TS + i64::from(i), Some(&[i]), Some(&[i, i]), &[]);
    }
    add("idempotent", stamped(b.finish().expect("batch"), 3, 1));

    let mut b = BatchBuilder::new(options(true, 17, 2, 8));
    b.push(TS, Some(b"t"), Some(b"transactional"), &[]);
    add("transactional", stamped(b.finish().expect("batch"), 200, 2));

    // Multi-byte lengths: a key and a value that need two and three varint bytes.
    let (key, value) = (vec![0xabu8; 200], vec![0xcdu8; 20_000]);
    let mut b = BatchBuilder::new(BatchOptions::default());
    b.push(TS, Some(&key), Some(&value), &[]);
    b.push(-TS, Some(b"negative"), None, &[]);
    add("data-large", b.finish().expect("batch"));

    add(
        "control-commit",
        stamped(lotse_record::control_batch(17, 3, ControlType::Commit, 9, TS).expect("batch"), 204, 2),
    );
    add(
        "control-abort",
        stamped(lotse_record::control_batch(18, 0, ControlType::Abort, 0, TS).expect("batch"), 205, 2),
    );
    add(
        "raft-noop",
        stamped(lotse_record::raft_control_batch(lotse_record::RaftControl::Noop, b"").expect("batch"), 0, 1),
    );
    add(
        "raft-config",
        stamped(
            lotse_record::raft_control_batch(lotse_record::RaftControl::Config, b"\x01\x02\x03").expect("batch"),
            1,
            1,
        ),
    );
    add(
        "internal-metadata",
        lotse_record::internal_control_batch(lotse_record::InternalControl::Metadata, b"payload").expect("batch"),
    );
    add("ack", stamped(lotse_record::ack_batch(&[(0, 3), (5, 6), (1 << 40, (1 << 40) + 2)]).expect("batch"), 10, 1));
    add("queue-state", lotse_record::queue_state_batch(&[(4, 2), (9, 65_535)]).expect("batch"));
    let entries = [(4, dead_letter::Reason::MaxDeliveries, 5), (9, dead_letter::Reason::Expired, 1)];
    add("retire-out", lotse_record::retire_out_batch(77, &entries).expect("batch"));
    add("retire-done", lotse_record::retire_done_batch(&[4, 9]).expect("batch"));

    // A dead-lettered message: the item carries its origin, the batch joins items.
    let mut b = BatchBuilder::new(BatchOptions::default());
    b.push(TS, Some(b"order-1"), Some(b"payload"), &[(b"trace", Some(b"abc"))]);
    let (source, _) = Batch::parse(&b.finish().expect("batch")).expect("parses");
    let record = source.records().expect("records").remove(0);
    let item = lotse_record::dead_letter_item(
        &record,
        lotse_record::DeadLetterInfo {
            queue: "jobs",
            queue_id: 1 << 40,
            offset: 12,
            floor: 10,
            reason: dead_letter::Reason::Expired,
            deliveries: 3,
        },
    )
    .expect("item");
    let (item, _) = Batch::parse(&item).expect("parses");
    add("dead-letter", lotse_record::dead_letter_batch(&[item], TS + 1000).expect("batch"));
    out
}

fn main() {
    let dir = PathBuf::from(std::env::args().nth(1).expect("usage: vectors <output directory>"));
    std::fs::create_dir_all(&dir).expect("creating the output directory");
    let out = Out { dir };

    let requests: Vec<String> = samples::requests()
        .iter()
        .enumerate()
        .map(|(i, req)| {
            let frame = encode_request(i as u32 + 1, req);
            assert_eq!(decode_request(frame.clone()).expect("decodes").1, *req);
            format!("req-{i:03}-{} {}", variant(&format!("{req:?}")), hex(&frame))
        })
        // Node-internal keys (43, 44, 1000) are not part of the client protocol.
        .zip(samples::requests())
        .filter(|(_, req)| {
            !matches!(req.api_key(), ApiKey::SETTLE_VOTERS | ApiKey::DEAD_LETTER | ApiKey::WRITE_TXN_MARKERS)
        })
        .map(|(line, _)| line)
        .collect();
    out.write(
        "requests.hex",
        "Request frames (version 0), without the length prefix. Correlation id = index + 1.\nlabel hex",
        &requests,
    );

    let responses: Vec<String> = samples::responses()
        .iter()
        .enumerate()
        .filter(|(_, (key, _))| {
            !matches!(*key, ApiKey::WRITE_TXN_MARKERS | ApiKey::SETTLE_VOTERS | ApiKey::DEAD_LETTER)
        })
        .map(|(i, (key, resp))| {
            let frame = encode_response(i as u32, resp);
            let name = match resp {
                Response::Error(_) => "Error".to_string(),
                Response::Refused { .. } => "Refused".to_string(),
                other => variant(&format!("{other:?}")),
            };
            format!("res-{i:03}-{name} {} {}", key.0, hex(&frame))
        })
        .collect();
    out.write(
        "responses.hex",
        "Response frames, without the length prefix, with the API key of the request they answer.\nCorrelation id = index in the sample list.\nlabel api_key hex",
        &responses,
    );

    let pushes: Vec<String> =
        samples::pushes().iter().enumerate().map(|(i, p)| format!("push-{i:03} {}", hex(&encode_push(p)))).collect();
    out.write("pushes.hex", "Push frames (queue deliveries), without the length prefix.\nlabel hex", &pushes);

    let all = batches();
    let lines: Vec<String> = all.iter().map(|(l, b)| format!("{l} {}", hex(b))).collect();
    out.write("batches.hex", "Record batches as stored and sent.\nlabel hex", &lines);

    let mut fields = Vec::new();
    let mut records = Vec::new();
    for (label, bytes) in &all {
        let (b, rest) = Batch::parse(bytes).expect("a valid batch");
        assert!(rest.is_empty());
        let a = b.attributes();
        fields.push(format!(
            "{label} base_offset={} term={} attributes={} compression={} transactional={} control={} kind={} \
             last_offset_delta={} base_ts={} max_ts={} producer_id={} producer_epoch={} base_sequence={} \
             record_count={} len={}",
            b.base_offset(),
            b.term(),
            a.to_bits(),
            match a.compression {
                Compression::None => 0,
                Compression::Zstd => 1,
                Compression::Lz4 => 2,
            },
            a.transactional,
            a.control,
            a.to_bits() >> 5 & 7,
            b.last_offset_delta(),
            b.base_timestamp(),
            b.max_timestamp(),
            b.producer_id(),
            b.producer_epoch(),
            b.base_sequence(),
            b.record_count(),
            b.len(),
        ));
        for (i, r) in b.records().expect("records").iter().enumerate() {
            let Record { offset, timestamp, key, value, headers } = r;
            let headers = if headers.is_empty() {
                "-".to_string()
            } else {
                headers
                    .iter()
                    .map(|h| format!("{}:{}", hex(&h.key), nullable(h.value.as_deref())))
                    .collect::<Vec<_>>()
                    .join(",")
            };
            // Large fields are shortened to their length and a checksum, to keep the file readable.
            let shorten = |v: &Option<Bytes>| match v {
                Some(b) if b.len() > 64 => format!("#{}:{:08x}", b.len(), crc32c::crc32c(b)),
                other => nullable(other.as_deref()),
            };
            records.push(format!("{label} {i} {offset} {timestamp} {} {} {headers}", shorten(key), shorten(value)));
        }
    }
    out.write(
        "batch-fields.txt",
        "Header fields of each batch in batches.hex. attributes is the packed u16; kind is bits 5..7.",
        &fields,
    );
    out.write(
        "batch-records.txt",
        "Records of each batch. Fields: null is -, empty is =, bytes are =<hex>; a field longer than 64 bytes is\n#<length>:<crc32c>. Headers are key:value pairs separated by commas, - for none.\nlabel index offset timestamp key value headers",
        &records,
    );

    // In-place edits: each recomputes the checksum.
    let by = |l: &str| all.iter().find(|(x, _)| x == l).expect("known label").1.to_vec();
    let mut ops = Vec::new();
    let mut op = |name: &str, args: &str, input: Vec<u8>, apply: &dyn Fn(&mut [u8])| {
        let mut output = input.clone();
        apply(&mut output);
        ops.push(format!("{name} {args} {} {}", hex(&input), hex(&output)));
    };
    op("set_producer", "99,4,12", by("data-single"), &|b| lotse_record::set_producer(b, 99, 4, 12).expect("edits"));
    op("set_producer", "18446744073709551615,65535,-1", by("data-stamped"), &|b| {
        lotse_record::set_producer(b, u64::MAX, u16::MAX, -1).expect("edits");
    });
    op("set_transactional", "true", by("data-single"), &|b| lotse_record::set_transactional(b, true).expect("edits"));
    op("set_transactional", "false", by("transactional"), &|b| {
        lotse_record::set_transactional(b, false).expect("edits");
    });
    op("set_base_timestamp", "1800000000000", by("data-stamped"), &|b| {
        lotse_record::set_base_timestamp(b, 1_800_000_000_000).expect("edits");
    });
    op("set_base_timestamp", "0", by("data-large"), &|b| lotse_record::set_base_timestamp(b, 0).expect("edits"));
    out.write(
        "batch-ops.txt",
        "In-place edits of a batch, each recomputing the checksum. Arguments are comma-separated.\nop arguments input_hex output_hex",
        &ops,
    );

    // A fetch response's records: several batches back to back.
    let join = |labels: &[&str]| labels.iter().flat_map(|l| by(l)).collect::<Vec<u8>>();
    let fetches = [
        ("fetch-data", join(&["data-stamped", "idempotent", "transactional", "control-commit"])),
        ("fetch-mixed", join(&["control-abort", "raft-noop", "ack", "data-large", "dead-letter"])),
    ]
    .map(|(l, b)| format!("{l} {}", hex(&b)));
    out.write("fetch-records.hex", "Batches in a row, as in the records of a fetch response.\nlabel hex", &fetches);

    let mut v = Vec::new();
    for x in [
        0i64,
        -1,
        1,
        -2,
        2,
        63,
        -64,
        64,
        -65,
        8191,
        -8192,
        i64::from(i32::MAX),
        i64::from(i32::MIN),
        i64::MAX,
        i64::MIN,
    ] {
        let mut b = Vec::new();
        varint::put_i64(&mut b, x);
        v.push(format!("zigzag {x} {}", hex(&b)));
    }
    for x in [0u64, 127, 128, 16_383, 16_384, u64::from(u32::MAX), u64::MAX] {
        let mut b = Vec::new();
        varint::put_u64(&mut b, x);
        v.push(format!("unsigned {x} {}", hex(&b)));
    }
    // What the reference reader does with input that is not canonical.
    v.push(format!("lenient 0 {}", hex(&[0x80, 0x00])));
    v.push(format!("lenient 1 {}", hex(&[0x81, 0x80, 0x00])));
    for (name, bytes) in [
        ("truncated", vec![0x80u8, 0x80]),
        ("truncated", vec![]),
        ("overflow", vec![0xffu8; 10]),
        ("overflow", [vec![0xffu8; 9], vec![0x02]].concat()),
        ("overflow", vec![0xffu8; 11]),
    ] {
        let expected = match varint::get_u64(&bytes) {
            Err(lotse_record::Error::Truncated) => "truncated",
            Err(lotse_record::Error::VarintOverflow) => "overflow",
            other => panic!("{name}: {other:?}"),
        };
        assert_eq!(expected, name);
        v.push(format!("bad {name} {}", if bytes.is_empty() { "-".to_string() } else { hex(&bytes) }));
    }
    out.write(
        "varints.txt",
        "ZigZag and plain LEB128 varints.\nzigzag <i64> <hex>; unsigned <u64> <hex> (the raw LEB128 behind it);\nlenient <u64> <hex>: accepted by the reference reader although a writer never produces it;\nbad <truncated|overflow> <hex>: refused.\nkind value hex",
        &v,
    );

    let mut crc = Vec::new();
    assert_eq!(crc32c::crc32c(b"123456789"), 0xE306_9283, "the standard check value");
    let inputs: Vec<(&str, Vec<u8>)> = vec![
        ("empty", vec![]),
        ("check", b"123456789".to_vec()),
        ("zeros-32", vec![0u8; 32]),
        ("ones-32", vec![0xffu8; 32]),
        ("ascending-32", (0..32u8).collect()),
        ("descending-32", (0..32u8).rev().collect()),
    ];
    for (label, input) in inputs {
        crc.push(format!(
            "{label} {} {:08x}",
            if input.is_empty() { "-".to_string() } else { hex(&input) },
            crc32c::crc32c(&input)
        ));
    }
    out.write("crc32c.txt", "CRC32C (Castagnoli).\nlabel input_hex crc", &crc);

    let codes: Vec<String> = (0..=28u16)
        .map(|c| {
            let e = ErrorCode(c);
            format!("{c} {} {}", e.name(), e.retriable())
        })
        .chain([format!("999 {} {}", ErrorCode(999).name(), ErrorCode(999).retriable())])
        .collect();
    out.write(
        "error-codes.txt",
        "Error codes, their names and whether the same request may succeed later.\ncode name retriable",
        &codes,
    );

    // Whole SCRAM-SHA-512 exchanges with fixed nonces, from the broker's own
    // implementation, so a client in another language can check itself
    // byte for byte.
    #[allow(clippy::type_complexity)]
    let cases: [(&str, &str, &str, &[u8], u32, &str, &str); 5] = [
        ("plain", "alice", "s3cret", b"0123456789abcdef", 4096, "clientnonce", "servernonce"),
        ("escaped-user", "a,b=c", "pw", b"saltsaltsaltsalt", 4096, "cn", "sn"),
        ("utf8-password", "bob", "p\u{e4}ssw\u{f6}rd \u{1f510}", b"\x00\x01\x02\xfe\xff-salt-bytes", 5000, "c1", "s1"),
        ("empty-password", "carol", "", b"0123456789abcdef", 4096, "c2", "s2"),
        ("long-salt", "dave", "a long password, with spaces and punctuation: =,", &[7u8; 64], 4097, "c3", "s3"),
    ];
    let scram: Vec<String> = cases
        .iter()
        .map(|(label, user, password, salt, iterations, cn, sn)| {
            let credential = ScramCredential::new(password, salt, *iterations);
            let mut client = ScramClient::new(user, password, cn);
            let mut server = ScramServer::new();
            let client_first = client.first();
            let server_first = server.first(&client_first, |_| Some(credential.clone()), sn).expect("server first");
            let client_final = client.respond(&server_first).expect("client final");
            let (who, server_final) = server.last(&client_final).expect("server final");
            assert_eq!(who, *user);
            client.verify(&server_final).expect("the server proves itself");
            format!(
                "{label} {} {} {} {iterations} {cn} {sn} {} {} {} {} {} {}",
                hex(user.as_bytes()),
                nullable(Some(password.as_bytes())),
                hex(salt),
                hex(&client_first),
                hex(&server_first),
                hex(&client_final),
                hex(&server_final),
                hex(&credential.stored_key),
                hex(&credential.server_key),
            )
        })
        .collect();
    out.write(
        "scram.txt",
        "SCRAM-SHA-512 exchanges with fixed nonces (no SASLprep: the password is its UTF-8 bytes), and the\ncredential for AlterUser. The password is written as = and its hex, so an empty one reads as =.\nlabel user_hex password salt_hex iterations client_nonce server_nonce client_first server_first client_final server_final stored_key server_key",
        &scram,
    );
}
