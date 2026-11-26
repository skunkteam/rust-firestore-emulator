use std::collections::HashMap;

use bytes::{BufMut, Bytes};
use googleapis::{
    MAX_TIMESTAMP_SECONDS, MIN_TIMESTAMP_SECONDS,
    google::{firestore::v1::Value, protobuf::Timestamp, r#type::LatLng},
};
use prost::Message;

use super::*;
use crate::{
    config::{DatabaseConcurrencyMode, DatabaseEdition, DatabaseSettings},
    reference::CollectionRef,
};

fn decode_bytes(bytes: &[u8], chunk_size: usize) -> Result<Vec<PreparedDatabase>, SnapshotError> {
    let mut decoder = SnapshotDecoder::new();
    for chunk in bytes.chunks(chunk_size.max(1)) {
        decoder.feed(chunk)?;
    }
    decoder.finish()
}

pub(crate) fn encode_single_db_bytes(
    root: &RootRef,
    settings: DatabaseSettings,
    docs: &[(&DocumentRef, &HashMap<String, Value>)],
) -> Vec<u8> {
    let mut encoder = SnapshotEncoder::new(Timestamp {
        seconds: 1_700_000_000,
        nanos:   123_456_789,
    });
    encoder.start_database(root, settings).unwrap();
    for (doc_ref, fields) in docs {
        encoder.write_document(doc_ref, fields).unwrap();
    }
    encoder.finish().unwrap();
    let mut out = Vec::new();
    encoder.encode_to_writer(&mut out).unwrap();
    out
}

fn finalize_raw_payload(body_before_trailer: &[u8]) -> Vec<u8> {
    let mut hasher = Hasher::new();
    hasher.update(body_before_trailer);
    let crc = hasher.finalize();

    let mut out = Vec::with_capacity(body_before_trailer.len() + TRAILER_LEN);
    out.extend_from_slice(body_before_trailer);
    out.put_u32_le(crc);
    out.extend_from_slice(&EOF_MAGIC);
    out
}

fn push_raw_frame(dst: &mut Vec<u8>, tag: u8, payload: &[u8]) {
    dst.push(tag);
    dst.put_u32_le(payload.len() as u32);
    dst.extend_from_slice(payload);
}

fn build_with_frames(frames: &[(u8, &[u8])]) -> Vec<u8> {
    let mut raw = Vec::new();
    raw.extend_from_slice(&SNAPSHOT_MAGIC);
    raw.put_u16_le(FORMAT_MAJOR);
    for (tag, payload) in frames {
        push_raw_frame(&mut raw, *tag, payload);
    }
    finalize_raw_payload(&raw)
}

fn all_eleven_value_types_fields(root: &RootRef) -> HashMap<String, Value> {
    let min_ts = Timestamp {
        seconds: MIN_TIMESTAMP_SECONDS,
        nanos:   0,
    };
    let max_ts = Timestamp {
        seconds: MAX_TIMESTAMP_SECONDS,
        nanos:   999_999_999,
    };
    HashMap::from([
        ("null_val".to_string(), Value::null()),
        ("bool_true".to_string(), Value::boolean(true)),
        ("bool_false".to_string(), Value::boolean(false)),
        ("int_min".to_string(), Value::integer(i64::MIN)),
        ("int_max".to_string(), Value::integer(i64::MAX)),
        ("neg_zero".to_string(), Value::double(-0.0)),
        ("pos_zero".to_string(), Value::double(0.0)),
        ("nan_val".to_string(), Value::double(f64::NAN)),
        ("pos_inf".to_string(), Value::double(f64::INFINITY)),
        ("neg_inf".to_string(), Value::double(f64::NEG_INFINITY)),
        ("ts_min".to_string(), Value::timestamp(min_ts)),
        ("ts_max".to_string(), Value::timestamp(max_ts)),
        ("bytes_empty".to_string(), Value::bytes(Bytes::new())),
        (
            "bytes_bin".to_string(),
            Value::bytes(Bytes::from_static(b"\x00\x01\xfe\xffhello")),
        ),
        ("str_unicode".to_string(), Value::string("héllo 🌍")),
        (
            "ref_val".to_string(),
            Value::reference(format!("{root}/other/doc")),
        ),
        (
            "geo_val".to_string(),
            Value::from(LatLng {
                latitude:  -37.8136,
                longitude: 144.9631,
            }),
        ),
        (
            "arr_val".to_string(),
            Value::array(vec![
                Value::null(),
                Value::integer(42),
                Value::double(-0.0),
                Value::vector([1.5, -2.5, 0.0]),
            ]),
        ),
        ("empty_map".to_string(), Value::map(HashMap::new())),
        (
            "nested_map".to_string(),
            Value::map(HashMap::from([(
                "vec".to_string(),
                Value::vector([0.25, -0.5, f64::INFINITY]),
            )])),
        ),
        ("top_vec".to_string(), Value::vector([1.0, 2.0, 3.0])),
    ])
}

#[test]
fn lossless_roundtrip_all_value_types_zero_field_subcol_empty_db_and_zero_db() {
    // 1. Zero-database snapshot round-trips to an empty Vec.
    let mut empty_encoder = SnapshotEncoder::new(Timestamp {
        seconds: 100,
        nanos:   0,
    });
    empty_encoder.finish().unwrap();
    let mut empty_bytes = Vec::new();
    empty_encoder.encode_to_writer(&mut empty_bytes).unwrap();
    let decoded_zero = decode_bytes(&empty_bytes, 1).unwrap();
    assert!(decoded_zero.is_empty());

    // 2. Multi-database snapshot with all 11 types, zero-field doc, phantom-parent subcollection,
    //    and empty database with Enterprise/Pessimistic settings.
    let root_a = RootRef::try_new("proj-a", "(default)").unwrap();
    let root_b = RootRef::try_new("proj-b", "empty-db").unwrap();

    let doc_all = DocumentRef::new(CollectionRef::new(root_a.clone(), "all"), "doc1");
    let doc_zero = DocumentRef::new(CollectionRef::new(root_a.clone(), "flags"), "ready");
    let doc_sub = DocumentRef::new(
        CollectionRef::new(root_a.clone(), "rooms/r1/messages"),
        "m1",
    );

    let all_fields = all_eleven_value_types_fields(&root_a);
    let zero_fields = HashMap::new();
    let sub_fields = HashMap::from([("body".to_string(), Value::string("hi"))]);

    let mut encoder = SnapshotEncoder::new(Timestamp {
        seconds: 1_700_000_123,
        nanos:   456,
    });
    encoder
        .start_database(
            &root_a,
            DatabaseSettings::for_edition(DatabaseEdition::Standard),
        )
        .unwrap();
    encoder.write_document(&doc_all, &all_fields).unwrap();
    encoder.write_document(&doc_zero, &zero_fields).unwrap();
    encoder.write_document(&doc_sub, &sub_fields).unwrap();

    encoder
        .start_database(
            &root_b,
            DatabaseSettings {
                edition: DatabaseEdition::Enterprise,
                concurrency_mode: DatabaseConcurrencyMode::Pessimistic,
            },
        )
        .unwrap();
    encoder.finish().unwrap();

    let mut encoded = Vec::new();
    encoder.encode_to_writer(&mut encoded).unwrap();

    for chunk_size in [1, 7, 64 * 1024, encoded.len()] {
        let decoded = decode_bytes(&encoded, chunk_size).unwrap();
        assert_eq!(decoded.len(), 2);

        let db_a = &decoded[0];
        assert_eq!(db_a.name, root_a);
        assert_eq!(
            db_a.settings,
            DatabaseSettings::for_edition(DatabaseEdition::Standard)
        );
        assert_eq!(db_a.document_count, 3);

        let (_, got_all) = &db_a.collections[&DefaultAtom::from("all")][&DefaultAtom::from("doc1")];
        assert!(Value::fields_storage_eq(got_all, &all_fields));
        assert_eq!(
            got_all["neg_zero"].as_double().unwrap().to_bits(),
            (-0.0_f64).to_bits()
        );

        let (_, got_zero) =
            &db_a.collections[&DefaultAtom::from("flags")][&DefaultAtom::from("ready")];
        assert!(got_zero.is_empty());

        let (_, got_sub) =
            &db_a.collections[&DefaultAtom::from("rooms/r1/messages")][&DefaultAtom::from("m1")];
        assert!(Value::fields_storage_eq(got_sub, &sub_fields));

        let db_b = &decoded[1];
        assert_eq!(db_b.name, root_b);
        assert_eq!(
            db_b.settings,
            DatabaseSettings {
                edition: DatabaseEdition::Enterprise,
                concurrency_mode: DatabaseConcurrencyMode::Pessimistic,
            }
        );
        assert_eq!(db_b.document_count, 0);
        assert!(db_b.collections.is_empty());
    }
}

#[test]
fn encoder_slices_multi_64kib_frames_and_rejects_oversized_frames() {
    let root = RootRef::try_new("proj", "(default)").unwrap();
    let doc_ref = DocumentRef::new(CollectionRef::new(root.clone(), "blobs"), "large");
    let large_bytes = vec![0xABu8; 200 * 1024];
    let fields = HashMap::from([("payload".to_string(), Value::bytes(large_bytes))]);

    let mut encoder = SnapshotEncoder::new(Timestamp {
        seconds: 10,
        nanos:   0,
    });
    encoder
        .start_database(
            &root,
            DatabaseSettings::for_edition(DatabaseEdition::Standard),
        )
        .unwrap();
    encoder.write_document(&doc_ref, &fields).unwrap();
    encoder.finish().unwrap();

    let mut chunks = Vec::new();
    while let Some(chunk) = encoder.take_chunk() {
        assert!(!chunk.is_empty());
        assert!(chunk.len() <= EXPORT_CHUNK_BYTES);
        chunks.push(chunk);
    }
    assert!(chunks.len() >= 4);

    let mut decoder = SnapshotDecoder::new();
    for chunk in &chunks {
        decoder.feed(chunk).unwrap();
    }
    let decoded = decoder.finish().unwrap();
    assert_eq!(decoded.len(), 1);
    assert!(Value::fields_storage_eq(
        &decoded[0].collections[&DefaultAtom::from("blobs")][&DefaultAtom::from("large")].1,
        &fields
    ));

    // Frame exceeding MAX_FRAME_BYTES is rejected by encoder before encoding.
    let oversized_fields = HashMap::from([(
        "huge".to_string(),
        Value::bytes(vec![0u8; MAX_FRAME_BYTES + 1]),
    )]);
    let mut oversized_enc = SnapshotEncoder::new(Timestamp {
        seconds: 10,
        nanos:   0,
    });
    oversized_enc
        .start_database(
            &root,
            DatabaseSettings::for_edition(DatabaseEdition::Standard),
        )
        .unwrap();
    let err = oversized_enc
        .write_document(&doc_ref, &oversized_fields)
        .unwrap_err();
    assert!(matches!(
        err,
        SnapshotError::FrameTooLarge {
            max: MAX_FRAME_BYTES,
            ..
        }
    ));
}

#[test]
fn decoder_rejects_truncation_and_single_bit_corruption_at_every_byte_offset() {
    let root = RootRef::try_new("proj", "(default)").unwrap();
    let doc = DocumentRef::new(CollectionRef::new(root.clone(), "c"), "d");
    let fields = HashMap::from([("k".to_string(), Value::string("value"))]);
    let bytes = encode_single_db_bytes(
        &root,
        DatabaseSettings::for_edition(DatabaseEdition::Enterprise),
        &[(&doc, &fields)],
    );

    // Truncating at every byte offset `0..bytes.len()` returns `SnapshotError::Truncated`.
    for cut in 0..bytes.len() {
        let err = decode_bytes(&bytes[..cut], 17).unwrap_err();
        assert!(
            matches!(err, SnapshotError::Truncated(_)),
            "expected Truncated at offset {cut}/{}, got {err:?}",
            bytes.len()
        );
    }

    // Flipping any bit in any byte fails cleanly (never succeeds).
    for idx in 0..bytes.len() {
        let mut corrupted = bytes.clone();
        corrupted[idx] ^= 0x01;
        let err = decode_bytes(&corrupted, 64).unwrap_err();
        assert!(
            matches!(
                err,
                SnapshotError::InvalidFormat(_)
                    | SnapshotError::UnsupportedVersion { .. }
                    | SnapshotError::FrameTooLarge { .. }
                    | SnapshotError::Validation(_)
                    | SnapshotError::Truncated(_)
            ),
            "unexpected error at flipped byte {idx}/{}: {err:?}",
            bytes.len()
        );
    }
}

#[test]
fn decoder_rejects_invalid_headers_versions_tags_enums_trailing_and_oversized_prefixes() {
    let root = RootRef::try_new("proj", "(default)").unwrap();
    let doc = DocumentRef::new(CollectionRef::new(root.clone(), "c"), "d");
    let fields = HashMap::from([("k".to_string(), Value::integer(1))]);
    let valid = encode_single_db_bytes(
        &root,
        DatabaseSettings::for_edition(DatabaseEdition::Standard),
        &[(&doc, &fields)],
    );

    // Wrong magic
    let mut bad_magic = valid.clone();
    bad_magic[0] = b'X';
    assert!(matches!(
        decode_bytes(&bad_magic, 64).unwrap_err(),
        SnapshotError::InvalidFormat(_)
    ));

    // Unsupported major version (0 and 2)
    for bad_major in [0u16, 2u16] {
        let mut bad_ver = valid.clone();
        bad_ver[8..10].copy_from_slice(&bad_major.to_le_bytes());
        assert_eq!(
            decode_bytes(&bad_ver, 64).unwrap_err(),
            SnapshotError::UnsupportedVersion {
                found_major:     bad_major,
                supported_major: FORMAT_MAJOR,
            }
        );
    }

    // Trailing bytes after trailer
    let mut trailing = valid.clone();
    trailing.push(0x00);
    assert!(matches!(
        decode_bytes(&trailing, 64).unwrap_err(),
        SnapshotError::InvalidFormat(_)
    ));

    // Truncated input with a frame prefix claiming > MAX_FRAME_BYTES fails immediately with
    // FrameTooLarge without waiting for full payload or hitting OOM.
    let mut oversized_prefix = Vec::new();
    oversized_prefix.extend_from_slice(&SNAPSHOT_MAGIC);
    oversized_prefix.put_u16_le(FORMAT_MAJOR);
    oversized_prefix.push(TAG_SNAPSHOT_HEADER);
    oversized_prefix.put_u32_le((MAX_FRAME_BYTES + 1) as u32);
    oversized_prefix.extend_from_slice(&[0u8; 32]);
    assert_eq!(
        decode_bytes(&oversized_prefix, 64).unwrap_err(),
        SnapshotError::FrameTooLarge {
            len: MAX_FRAME_BYTES + 1,
            max: MAX_FRAME_BYTES,
        }
    );

    let header = SnapshotHeader::default().encode_to_vec();

    // Unknown frame tag
    let full = build_with_frames(&[(TAG_SNAPSHOT_HEADER, &header), (0x7f, b"abc")]);
    assert!(matches!(
        decode_bytes(&full, 64).unwrap_err(),
        SnapshotError::InvalidFormat(_)
    ));

    // Unspecified (0) and unknown wire enum discriminants
    for (ed, mode) in [(0, 1), (99, 1), (1, 0), (1, 99)] {
        let db_frame = DatabaseStart {
            project_id: "p".to_string(),
            database_id: "d".to_string(),
            database_edition: ed,
            concurrency_mode: mode,
        }
        .encode_to_vec();
        let eos = EndOfSnapshot {
            total_databases: 1,
            total_documents: 0,
        }
        .encode_to_vec();
        let full = build_with_frames(&[
            (TAG_SNAPSHOT_HEADER, &header),
            (TAG_DATABASE_START, &db_frame),
            (TAG_END_OF_SNAPSHOT, &eos),
        ]);
        assert!(matches!(
            decode_bytes(&full, 64).unwrap_err(),
            SnapshotError::InvalidFormat(_)
        ));
    }

    // EndOfSnapshot count mismatch
    let eos_mismatch = EndOfSnapshot {
        total_databases: 1,
        total_documents: 0,
    }
    .encode_to_vec();
    let full = build_with_frames(&[
        (TAG_SNAPSHOT_HEADER, &header),
        (TAG_END_OF_SNAPSHOT, &eos_mismatch),
    ]);
    assert!(matches!(
        decode_bytes(&full, 64).unwrap_err(),
        SnapshotError::InvalidFormat(_)
    ));
}

#[test]
fn decoder_rejects_duplicates_invalid_paths_timestamps_and_grammar_violations() {
    let header = SnapshotHeader::default().encode_to_vec();
    let db_a = DatabaseStart {
        project_id: "p".to_string(),
        database_id: "a".to_string(),
        database_edition: DatabaseEditionWire::Standard as i32,
        concurrency_mode: ConcurrencyModeWire::Pessimistic as i32,
    }
    .encode_to_vec();
    let db_b = DatabaseStart {
        project_id: "p".to_string(),
        database_id: "b".to_string(),
        database_edition: DatabaseEditionWire::Enterprise as i32,
        concurrency_mode: ConcurrencyModeWire::Optimistic as i32,
    }
    .encode_to_vec();
    let doc_1 = DocumentRecord {
        document_path: "c/d1".to_string(),
        fields: HashMap::new(),
    }
    .encode_to_vec();
    let doc_2 = DocumentRecord {
        document_path: "c/d2".to_string(),
        fields: HashMap::new(),
    }
    .encode_to_vec();

    // Unsorted database order (b before a) and unsorted document order (d2 before d1) succeed.
    let eos_2_2 = EndOfSnapshot {
        total_databases: 2,
        total_documents: 2,
    }
    .encode_to_vec();
    let unsorted = build_with_frames(&[
        (TAG_SNAPSHOT_HEADER, &header),
        (TAG_DATABASE_START, &db_b),
        (TAG_DOCUMENT_RECORD, &doc_2),
        (TAG_DOCUMENT_RECORD, &doc_1),
        (TAG_DATABASE_START, &db_a),
        (TAG_END_OF_SNAPSHOT, &eos_2_2),
    ]);
    let decoded = decode_bytes(&unsorted, 32).unwrap();
    assert_eq!(decoded.len(), 2);
    assert_eq!(&*decoded[0].name.database_id, "b");
    assert_eq!(decoded[0].document_count, 2);
    assert_eq!(&*decoded[1].name.database_id, "a");

    // Duplicate database
    let eos_2_0 = EndOfSnapshot {
        total_databases: 2,
        total_documents: 0,
    }
    .encode_to_vec();
    let dup_db = build_with_frames(&[
        (TAG_SNAPSHOT_HEADER, &header),
        (TAG_DATABASE_START, &db_a),
        (TAG_DATABASE_START, &db_a),
        (TAG_END_OF_SNAPSHOT, &eos_2_0),
    ]);
    assert!(matches!(
        decode_bytes(&dup_db, 64).unwrap_err(),
        SnapshotError::InvalidFormat(_)
    ));

    // Duplicate document
    let eos_1_2 = EndOfSnapshot {
        total_databases: 1,
        total_documents: 2,
    }
    .encode_to_vec();
    let dup_doc = build_with_frames(&[
        (TAG_SNAPSHOT_HEADER, &header),
        (TAG_DATABASE_START, &db_a),
        (TAG_DOCUMENT_RECORD, &doc_1),
        (TAG_DOCUMENT_RECORD, &doc_1),
        (TAG_END_OF_SNAPSHOT, &eos_1_2),
    ]);
    assert!(matches!(
        decode_bytes(&dup_doc, 64).unwrap_err(),
        SnapshotError::InvalidFormat(_)
    ));

    // Grammar violations (missing header, document before database, frame after EndOfSnapshot)
    let missing_header = build_with_frames(&[(TAG_DATABASE_START, &db_a)]);
    assert!(matches!(
        decode_bytes(&missing_header, 64).unwrap_err(),
        SnapshotError::InvalidFormat(_)
    ));
    let doc_before_db = build_with_frames(&[
        (TAG_SNAPSHOT_HEADER, &header),
        (TAG_DOCUMENT_RECORD, &doc_1),
    ]);
    assert!(matches!(
        decode_bytes(&doc_before_db, 64).unwrap_err(),
        SnapshotError::InvalidFormat(_)
    ));

    // Invalid DatabaseStart segments -> SnapshotError::Validation
    for (bad_proj, bad_db) in [
        ("p", "d/documents"),
        ("p", "."),
        ("p", ".."),
        ("p", "__reserved__"),
        ("", "d"),
    ] {
        let bad_db_frame = DatabaseStart {
            project_id: bad_proj.to_string(),
            database_id: bad_db.to_string(),
            database_edition: DatabaseEditionWire::Standard as i32,
            concurrency_mode: ConcurrencyModeWire::Pessimistic as i32,
        }
        .encode_to_vec();
        let bad = build_with_frames(&[
            (TAG_SNAPSHOT_HEADER, &header),
            (TAG_DATABASE_START, &bad_db_frame),
        ]);
        assert!(matches!(
            decode_bytes(&bad, 64).unwrap_err(),
            SnapshotError::Validation(GenericDatabaseError::InvalidReference("database", _))
        ));
    }

    // Invalid DocumentRecord.document_path (leading/trailing slash, collection-only, reserved)
    for bad_path in ["", "/c/d", "c/d/", "c", "c/__bad__", "../d"] {
        let bad_doc_frame = DocumentRecord {
            document_path: bad_path.to_string(),
            fields: HashMap::new(),
        }
        .encode_to_vec();
        let bad = build_with_frames(&[
            (TAG_SNAPSHOT_HEADER, &header),
            (TAG_DATABASE_START, &db_a),
            (TAG_DOCUMENT_RECORD, &bad_doc_frame),
        ]);
        assert!(matches!(
            decode_bytes(&bad, 64).unwrap_err(),
            SnapshotError::Validation(GenericDatabaseError::InvalidReference(_, _))
        ));
    }

    // Out-of-range timestamp inside DocumentRecord -> SnapshotError::Validation
    let bad_ts_doc = DocumentRecord {
        document_path: "c/d".to_string(),
        fields: HashMap::from([(
            "ts".to_string(),
            Value::timestamp(Timestamp {
                seconds: MIN_TIMESTAMP_SECONDS - 1,
                nanos:   0,
            }),
        )]),
    }
    .encode_to_vec();
    let bad_ts = build_with_frames(&[
        (TAG_SNAPSHOT_HEADER, &header),
        (TAG_DATABASE_START, &db_a),
        (TAG_DOCUMENT_RECORD, &bad_ts_doc),
    ]);
    assert!(matches!(
        decode_bytes(&bad_ts, 64).unwrap_err(),
        SnapshotError::Validation(GenericDatabaseError::InvalidArgument(_))
    ));
}

#[test]
fn borrowed_document_record_encoding_matches_prost_message() {
    let root = RootRef::try_new("proj", "(default)").unwrap();
    let all_eleven = all_eleven_value_types_fields(&root);
    let cases = [
        (
            "items",
            "single",
            HashMap::from([("k".to_string(), Value::string("v"))]),
        ),
        ("flags", "empty", HashMap::new()),
        (
            "edge",
            "empty_key",
            HashMap::from([(String::new(), Value::integer(7))]),
        ),
        ("all", "doc1", all_eleven),
    ];

    for (col_id, doc_id, fields) in cases {
        let document_path = format!("{col_id}/{doc_id}");
        let expected = DocumentRecord {
            document_path: document_path.clone(),
            fields: fields.clone(),
        }
        .encode_to_vec();

        let parts = [col_id, "/", doc_id];
        assert_eq!(
            wire::document_record_encoded_len_parts(&parts, &fields),
            expected.len()
        );
        let mut direct_buf = BytesMut::new();
        wire::encode_document_record_parts(&parts, &fields, &mut direct_buf);
        assert_eq!(direct_buf.as_ref(), expected.as_slice());

        assert_eq!(
            wire::document_record_encoded_len_parts(&[document_path.as_str()], &fields),
            expected.len()
        );
        let mut single_str_buf = BytesMut::new();
        wire::encode_document_record_parts(&[document_path.as_str()], &fields, &mut single_str_buf);
        assert_eq!(single_str_buf.as_ref(), expected.as_slice());

        // Also verify SnapshotEncoder::write_document emits the identical payload bytes.
        let mut enc = SnapshotEncoder::new(Timestamp {
            seconds: 100,
            nanos:   0,
        });
        enc.start_database(
            &root,
            DatabaseSettings::for_edition(DatabaseEdition::Standard),
        )
        .unwrap();
        let doc_offset = enc.out_buf.len();
        let doc_ref = DocumentRef::new(CollectionRef::new(root.clone(), col_id), doc_id);
        enc.write_document(&doc_ref, &fields).unwrap();
        let frame_slice = &enc.out_buf[doc_offset..];
        assert_eq!(frame_slice[0], TAG_DOCUMENT_RECORD);
        let payload_len = u32::from_le_bytes(frame_slice[1..5].try_into().unwrap()) as usize;
        assert_eq!(payload_len, expected.len());
        assert_eq!(&frame_slice[5..], expected.as_slice());
    }
}

#[test]
fn decoder_prefix_stage_rejects_invalid_frames_enforces_precedence_cap_and_failed_state() {
    const TEN_MIB: u32 = 10 * 1024 * 1024;
    let mut file_header = Vec::with_capacity(FILE_HEADER_LEN);
    file_header.extend_from_slice(&SNAPSHOT_MAGIC);
    file_header.put_u16_le(FORMAT_MAJOR);

    let header_frame = {
        let payload = SnapshotHeader::default().encode_to_vec();
        let mut frame = Vec::new();
        push_raw_frame(&mut frame, TAG_SNAPSHOT_HEADER, &payload);
        frame
    };

    let assert_terminal_failed = |mut decoder: SnapshotDecoder| {
        assert!(matches!(decoder.phase, DecodePhase::Failed));
        assert!(decoder.small_buf.is_empty());
        assert_eq!(
            decoder.feed(&[]),
            Err(SnapshotError::InvalidFormat(
                "snapshot decoder is in a failed state".to_string()
            ))
        );
        assert_eq!(
            decoder.feed(b"trailing"),
            Err(SnapshotError::InvalidFormat(
                "snapshot decoder is in a failed state".to_string()
            ))
        );
        assert_eq!(
            decoder.finish().unwrap_err(),
            SnapshotError::InvalidFormat("snapshot decoder is in a failed state".to_string())
        );
    };

    // 1. Unknown tag and grammar violations with a 10 MiB payload reject immediately at FramePrefix
    //    without buffering or allocating FramePayload.
    for (feed_header_frame, bad_tag) in [
        (true, 0x7f),
        (false, TAG_DATABASE_START),
        (false, TAG_DOCUMENT_RECORD),
        (false, TAG_END_OF_SNAPSHOT),
        (true, TAG_SNAPSHOT_HEADER),
        (true, TAG_DOCUMENT_RECORD),
    ] {
        let mut decoder = SnapshotDecoder::new();
        decoder.feed(&file_header).unwrap();
        if feed_header_frame {
            decoder.feed(&header_frame).unwrap();
        }
        let mut prefix = [0u8; FRAME_PREFIX_LEN];
        prefix[0] = bad_tag;
        prefix[1..].copy_from_slice(&TEN_MIB.to_le_bytes());
        let err = decoder.feed(&prefix).unwrap_err();
        assert!(
            matches!(err, SnapshotError::InvalidFormat(_)),
            "expected InvalidFormat for tag={bad_tag:#04x}, \
             feed_header_frame={feed_header_frame}, got {err:?}"
        );
        assert_terminal_failed(decoder);
    }

    // 2. FrameTooLarge takes precedence over invalid tag/grammar when payload_len >
    //    MAX_FRAME_BYTES.
    for (feed_header_frame, bad_tag) in [
        (true, 0x7f),
        (false, TAG_DOCUMENT_RECORD),
        (true, TAG_DOCUMENT_RECORD),
    ] {
        let mut decoder = SnapshotDecoder::new();
        decoder.feed(&file_header).unwrap();
        if feed_header_frame {
            decoder.feed(&header_frame).unwrap();
        }
        let mut prefix = [0u8; FRAME_PREFIX_LEN];
        prefix[0] = bad_tag;
        prefix[1..].copy_from_slice(&((MAX_FRAME_BYTES + 1) as u32).to_le_bytes());
        assert_eq!(
            decoder.feed(&prefix).unwrap_err(),
            SnapshotError::FrameTooLarge {
                len: MAX_FRAME_BYTES + 1,
                max: MAX_FRAME_BYTES,
            }
        );
        assert_terminal_failed(decoder);
    }

    // 3. Terminal Failed state also holds after trailing bytes past Done or header errors.
    let root = RootRef::try_new("proj", "(default)").unwrap();
    let valid = encode_single_db_bytes(
        &root,
        DatabaseSettings::for_edition(DatabaseEdition::Standard),
        &[],
    );
    let mut done_dec = SnapshotDecoder::new();
    done_dec.feed(&valid).unwrap();
    assert!(matches!(
        done_dec.feed(&[0x00]),
        Err(SnapshotError::InvalidFormat(_))
    ));
    assert_terminal_failed(done_dec);

    // 4. MAX_SNAPSHOT_DATABASES succeeds while MAX_SNAPSHOT_DATABASES + 1 rejects at FramePrefix.
    let mut decoder = SnapshotDecoder::new();
    decoder.feed(&file_header).unwrap();
    decoder.feed(&header_frame).unwrap();
    for i in 0..MAX_SNAPSHOT_DATABASES {
        let mut frame = Vec::new();
        push_raw_frame(
            &mut frame,
            TAG_DATABASE_START,
            &DatabaseStart {
                project_id: "p".to_string(),
                database_id: format!("db-{i}"),
                database_edition: DatabaseEditionWire::Standard as i32,
                concurrency_mode: ConcurrencyModeWire::Pessimistic as i32,
            }
            .encode_to_vec(),
        );
        decoder.feed(&frame).unwrap();
    }
    assert_eq!(decoder.total_databases, MAX_SNAPSHOT_DATABASES as u64);

    // Feeding a 10,001st TAG_DATABASE_START prefix (even claiming a 10 MiB payload) fails
    // immediately in FramePrefix before allocating FramePayload.
    let mut overflow_prefix = [0u8; FRAME_PREFIX_LEN];
    overflow_prefix[0] = TAG_DATABASE_START;
    overflow_prefix[1..].copy_from_slice(&TEN_MIB.to_le_bytes());
    assert!(matches!(
        decoder.feed(&overflow_prefix).unwrap_err(),
        SnapshotError::InvalidFormat(_)
    ));
    assert_terminal_failed(decoder);

    // Exactly MAX_SNAPSHOT_DATABASES completes cleanly with EndOfSnapshot + trailer.
    let mut max_raw = Vec::new();
    max_raw.extend_from_slice(&file_header);
    max_raw.extend_from_slice(&header_frame);
    for i in 0..MAX_SNAPSHOT_DATABASES {
        push_raw_frame(
            &mut max_raw,
            TAG_DATABASE_START,
            &DatabaseStart {
                project_id: "p".to_string(),
                database_id: format!("db-{i}"),
                database_edition: DatabaseEditionWire::Standard as i32,
                concurrency_mode: ConcurrencyModeWire::Pessimistic as i32,
            }
            .encode_to_vec(),
        );
    }
    push_raw_frame(
        &mut max_raw,
        TAG_END_OF_SNAPSHOT,
        &EndOfSnapshot {
            total_databases: MAX_SNAPSHOT_DATABASES as u64,
            total_documents: 0,
        }
        .encode_to_vec(),
    );
    let max_full = finalize_raw_payload(&max_raw);
    let decoded_max = decode_bytes(&max_full, 64 * 1024).unwrap();
    assert_eq!(decoded_max.len(), MAX_SNAPSHOT_DATABASES);
}

#[test]
fn golden_v1_snapshot_decodes_and_matches_encoder() {
    // Frozen v1 snapshot bytes:
    // - FileHeader: "FSEMUSNP", major = 1 (LE)
    // - SnapshotHeader: emulator_version = "1.0.0", snapshot_time = {1_700_000_000, 123_456_000}
    // - DatabaseStart: project_id = "golden-proj", database_id = "(default)",
    //   Enterprise/Pessimistic
    // - DocumentRecord: document_path = "items/doc1", fields = {"k": StringValue("v")}
    // - EndOfSnapshot: total_databases = 1, total_documents = 1
    // - Trailer: LE CRC-32 (0xf7d0f42a), "FSEMUEOF"
    const GOLDEN_V1_SNAPSHOT: &[u8] = &[
        0x46, 0x53, 0x45, 0x4d, 0x55, 0x53, 0x4e, 0x50, 0x01, 0x00, 0x01, 0x14, 0x00, 0x00, 0x00,
        0x0a, 0x05, 0x31, 0x2e, 0x30, 0x2e, 0x30, 0x12, 0x0b, 0x08, 0x80, 0xe2, 0xcf, 0xaa, 0x06,
        0x10, 0x80, 0x94, 0xef, 0x3a, 0x02, 0x1c, 0x00, 0x00, 0x00, 0x0a, 0x0b, 0x67, 0x6f, 0x6c,
        0x64, 0x65, 0x6e, 0x2d, 0x70, 0x72, 0x6f, 0x6a, 0x12, 0x09, 0x28, 0x64, 0x65, 0x66, 0x61,
        0x75, 0x6c, 0x74, 0x29, 0x18, 0x02, 0x20, 0x01, 0x03, 0x17, 0x00, 0x00, 0x00, 0x0a, 0x0a,
        0x69, 0x74, 0x65, 0x6d, 0x73, 0x2f, 0x64, 0x6f, 0x63, 0x31, 0x12, 0x09, 0x0a, 0x01, 0x6b,
        0x12, 0x04, 0x8a, 0x01, 0x01, 0x76, 0x04, 0x04, 0x00, 0x00, 0x00, 0x08, 0x01, 0x10, 0x01,
        0x2a, 0xf4, 0xd0, 0xf7, 0x46, 0x53, 0x45, 0x4d, 0x55, 0x45, 0x4f, 0x46,
    ];

    let root = RootRef::try_new("golden-proj", "(default)").unwrap();
    let expected_settings = DatabaseSettings {
        edition: DatabaseEdition::Enterprise,
        concurrency_mode: DatabaseConcurrencyMode::Pessimistic,
    };
    let expected_fields = HashMap::from([("k".to_string(), Value::string("v"))]);

    for chunk_size in [1, 7, GOLDEN_V1_SNAPSHOT.len()] {
        let decoded = decode_bytes(GOLDEN_V1_SNAPSHOT, chunk_size).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].name, root);
        assert_eq!(decoded[0].settings, expected_settings);
        assert_eq!(decoded[0].document_count, 1);
        let col_docs = &decoded[0].collections[&DefaultAtom::from("items")];
        let (got_ref, got_fields) = &col_docs[&DefaultAtom::from("doc1")];
        assert_eq!(
            got_ref,
            &DocumentRef::new(CollectionRef::new(root.clone(), "items"), "doc1")
        );
        assert!(Value::fields_storage_eq(got_fields, &expected_fields));
    }

    let mut encoder = SnapshotEncoder::new(Timestamp {
        seconds: 1_700_000_000,
        nanos:   123_456_000,
    });
    encoder.start_database(&root, expected_settings).unwrap();
    let doc_ref = DocumentRef::new(CollectionRef::new(root.clone(), "items"), "doc1");
    encoder.write_document(&doc_ref, &expected_fields).unwrap();
    encoder.finish().unwrap();

    let mut encoded = Vec::new();
    encoder.encode_to_writer(&mut encoded).unwrap();

    let header_payload_len = u32::from_le_bytes(
        encoded[FILE_HEADER_LEN + 1..FILE_HEADER_LEN + 5]
            .try_into()
            .unwrap(),
    ) as usize;
    let body_start = FILE_HEADER_LEN + FRAME_PREFIX_LEN + header_payload_len;

    assert_eq!(&encoded[..FILE_HEADER_LEN], &GOLDEN_V1_SNAPSHOT[..10]);
    assert_eq!(
        &encoded[body_start..body_start + 33],
        &GOLDEN_V1_SNAPSHOT[35..68]
    );
    assert_eq!(
        &encoded[body_start + 33..body_start + 61],
        &GOLDEN_V1_SNAPSHOT[68..96]
    );
    assert_eq!(
        &encoded[body_start + 61..body_start + 70],
        &GOLDEN_V1_SNAPSHOT[96..105]
    );
    let trailer_start = encoded.len() - TRAILER_LEN;
    let mut expected_crc = Hasher::new();
    expected_crc.update(&encoded[..trailer_start]);
    assert_eq!(
        &encoded[trailer_start..trailer_start + 4],
        &expected_crc.finalize().to_le_bytes()
    );
    assert_eq!(&encoded[trailer_start + 4..], &GOLDEN_V1_SNAPSHOT[109..117]);
    if env!("CARGO_PKG_VERSION") == "1.0.0" {
        assert_eq!(encoded.as_slice(), GOLDEN_V1_SNAPSHOT);
    }
}

mod proptests {
    use std::collections::HashMap;

    use bytes::Bytes;
    use googleapis::{
        MAX_TIMESTAMP_SECONDS, MIN_TIMESTAMP_SECONDS,
        google::{firestore::v1::Value, protobuf::Timestamp, r#type::LatLng},
    };
    use proptest::{prelude::*, sample::select};

    use super::*;

    const ID_VOCAB: &[&str] = &[
        "users",
        "orders",
        "items",
        "alpha",
        "beta",
        "d1",
        "d2",
        "(default)",
    ];

    fn arb_writable_value() -> impl Strategy<Value = Value> {
        let leaf = prop_oneof![
            Just(Value::null()),
            any::<bool>().prop_map(Value::boolean),
            prop_oneof![Just(i64::MIN), Just(i64::MAX), -1000_i64..=1000_i64]
                .prop_map(Value::integer),
            prop_oneof![
                Just(-0.0),
                Just(0.0),
                Just(f64::NAN),
                Just(f64::INFINITY),
                Just(f64::NEG_INFINITY),
                (-100_i64..=100_i64).prop_map(|n| n as f64 * 0.5),
            ]
            .prop_map(Value::double),
            (
                MIN_TIMESTAMP_SECONDS..=MAX_TIMESTAMP_SECONDS,
                0_i32..1_000_000_000_i32
            )
                .prop_map(|(seconds, nanos)| Value::timestamp(Timestamp { seconds, nanos })),
            "[a-z0-9_ ]{0,12}".prop_map(Value::string),
            prop::collection::vec(any::<u8>(), 0..16).prop_map(|b| Value::bytes(Bytes::from(b))),
            Just(Value::reference(
                "projects/p/databases/(default)/documents/c/d".to_string()
            )),
            (-90_i32..=90_i32, -180_i32..=180_i32).prop_map(|(lat, lon)| {
                Value::from(LatLng {
                    latitude:  f64::from(lat),
                    longitude: f64::from(lon),
                })
            }),
            prop::collection::vec(-10.0_f64..=10.0_f64, 1..=4).prop_map(Value::vector),
        ];
        leaf.clone().prop_recursive(2, 12, 4, move |inner| {
            prop_oneof![
                prop::collection::hash_map("[a-z]{1,4}", inner, 0..4).prop_map(Value::map),
                prop::collection::vec(leaf.clone(), 0..4).prop_map(Value::array),
            ]
        })
    }

    fn arb_doc_fields() -> impl Strategy<Value = HashMap<String, Value>> {
        prop::collection::hash_map("[a-z][a-z0-9_]{0,5}", arb_writable_value(), 0..5)
    }

    proptest! {
        #[test]
        fn proptest_snapshot_roundtrip_across_arbitrary_chunk_splits(
            enterprise in any::<bool>(),
            optimistic in any::<bool>(),
            doc_specs in prop::collection::vec(
                (select(&["c1", "c2", "c1/p/sub"]), select(ID_VOCAB), arb_doc_fields()),
                0..=6
            ),
            chunk_size in 1_usize..=256_usize,
            cut_ratio in 0.0_f64..1.0_f64,
        ) {
            let root = RootRef::try_new("proj", "(default)").unwrap();
            let settings = DatabaseSettings {
                edition: if enterprise {
                    DatabaseEdition::Enterprise
                } else {
                    DatabaseEdition::Standard
                },
                concurrency_mode: if optimistic {
                    DatabaseConcurrencyMode::Optimistic
                } else {
                    DatabaseConcurrencyMode::Pessimistic
                },
            };

            let mut deduped = HashMap::new();
            for (col, doc_id, fields) in doc_specs {
                let doc_ref = DocumentRef::new(CollectionRef::new(root.clone(), col), doc_id);
                deduped.entry(doc_ref).or_insert(fields);
            }

            let mut encoder = SnapshotEncoder::new(Timestamp {
                seconds: 1_700_000_000,
                nanos:   0,
            });
            encoder.start_database(&root, settings).unwrap();
            for (doc_ref, fields) in &deduped {
                encoder.write_document(doc_ref, fields).unwrap();
            }
            encoder.finish().unwrap();

            let mut encoded = Vec::new();
            encoder.encode_to_writer(&mut encoded).unwrap();

            let decoded = decode_bytes(&encoded, chunk_size).unwrap();
            prop_assert_eq!(decoded.len(), 1);
            prop_assert_eq!(&decoded[0].name, &root);
            prop_assert_eq!(decoded[0].settings, settings);
            prop_assert_eq!(decoded[0].document_count, deduped.len() as u64);

            for (doc_ref, expected_fields) in &deduped {
                let col_map = &decoded[0].collections[&doc_ref.collection_ref.collection_id];
                let (got_ref, got_fields) = &col_map[&doc_ref.document_id];
                prop_assert_eq!(got_ref, doc_ref);
                prop_assert!(Value::fields_storage_eq(got_fields, expected_fields));
            }

            // Truncating at any byte offset always yields `Truncated`.
            let cut = ((encoded.len() as f64) * cut_ratio) as usize;
            let cut = cut.min(encoded.len().saturating_sub(1));
            let trunc_err = decode_bytes(&encoded[..cut], chunk_size).unwrap_err();
            prop_assert!(matches!(trunc_err, SnapshotError::Truncated(_)));
        }
    }
}
