use nbd_protocol::constants;
use nbd_protocol::handshake::{
    decode_client_flags, encode_client_flags, encode_server_handshake, SERVER_HANDSHAKE_FLAGS,
};
use nbd_protocol::option::{
    encode_abort_request, encode_ack_reply, encode_export_info_reply, encode_go_request,
    encode_unknown_export_reply, encode_unsupported_option_reply, parse_option_reply,
    parse_option_reply_header, parse_option_request, OptionReply, OptionRequest,
    OPTION_REPLY_HEADER_BYTES,
};
use nbd_protocol::transmission::{
    encode_disconnect_request, encode_flush_request, encode_read_reply, encode_read_request,
    encode_success_reply, encode_write_request, parse_read_reply, parse_request,
    parse_request_header, parse_simple_reply, ReadReply, SimpleReply, TransmissionRequest,
    MAX_WRITE_PAYLOAD_BYTES, REQUEST_HEADER_BYTES, SIMPLE_REPLY_BYTES,
};
use nbd_protocol::wire::{
    write_u16, write_u32, write_u64, NbdCommandFlags, NbdCommandType, NbdCookie, NbdOptionCode,
    WireReader,
};
use nbd_protocol::ProtocolError;

#[test]
fn known_wire_constants_match_the_nbd_protocol() {
    assert_eq!(constants::INIT_PASSWD, 0x4e42_444d_4147_4943);
    assert_eq!(constants::IHAVEOPT_MAGIC, 0x4948_4156_454f_5054);
    assert_eq!(constants::OPTION_REPLY_MAGIC, 0x0003_e889_0455_65a9);
    assert_eq!(constants::NBD_REQUEST_MAGIC, 0x2560_9513);
    assert_eq!(constants::NBD_SIMPLE_REPLY_MAGIC, 0x6744_6698);
}

#[test]
fn wire_reader_and_writers_use_big_endian_layout() {
    let mut bytes = Vec::new();
    write_u16(&mut bytes, 0x0102);
    write_u32(&mut bytes, 0x0304_0506);
    write_u64(&mut bytes, 0x0708_090a_0b0c_0d0e);

    assert_eq!(
        bytes,
        [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,],
    );

    let mut reader = WireReader::new(&bytes);
    assert_eq!(reader.read_u16().unwrap(), 0x0102);
    assert_eq!(reader.read_u32().unwrap(), 0x0304_0506);
    assert_eq!(reader.read_u64().unwrap(), 0x0708_090a_0b0c_0d0e);
    assert_eq!(reader.position(), bytes.len());
    assert_eq!(reader.remaining(), 0);

    assert_eq!(
        reader.read_u16(),
        Err(ProtocolError::UnexpectedEof {
            needed: 2,
            remaining: 0,
        }),
    );
}

#[test]
fn typed_wire_wrappers_preserve_raw_protocol_values() {
    assert_eq!(
        NbdCookie::new(0xaabb_ccdd_eeff_0011).raw(),
        0xaabb_ccdd_eeff_0011
    );
    assert_eq!(NbdCommandFlags::new(0x1234).raw(), 0x1234);
    assert_eq!(
        NbdCommandType::new(constants::NBD_CMD_READ).raw(),
        constants::NBD_CMD_READ
    );
    assert_eq!(
        NbdOptionCode::new(constants::NBD_OPT_GO).raw(),
        constants::NBD_OPT_GO
    );
}

#[test]
fn fixed_newstyle_server_handshake_matches_wire_layout() {
    assert_eq!(
        SERVER_HANDSHAKE_FLAGS,
        constants::NBD_FLAG_FIXED_NEWSTYLE | constants::NBD_FLAG_NO_ZEROES,
    );

    assert_eq!(
        encode_server_handshake(),
        [
            0x4e, 0x42, 0x44, 0x4d, 0x41, 0x47, 0x49, 0x43, 0x49, 0x48, 0x41, 0x56, 0x45, 0x4f,
            0x50, 0x54, 0x00, 0x03,
        ],
    );
}

#[test]
fn client_flags_accept_fixed_newstyle_and_no_zeroes() {
    let raw = constants::NBD_FLAG_C_FIXED_NEWSTYLE | constants::NBD_FLAG_C_NO_ZEROES;
    let encoded = encode_client_flags(true);
    assert_eq!(encoded, raw.to_be_bytes());

    let flags = decode_client_flags(&encoded).unwrap();

    assert_eq!(flags.raw(), raw);
    assert!(flags.no_zeroes());
}

#[test]
fn client_flags_reject_missing_fixed_newstyle_or_unknown_bits() {
    assert_eq!(
        decode_client_flags(&constants::NBD_FLAG_C_NO_ZEROES.to_be_bytes()),
        Err(ProtocolError::MissingClientFlag {
            flag: "NBD_FLAG_C_FIXED_NEWSTYLE",
        }),
    );

    let raw = constants::NBD_FLAG_C_FIXED_NEWSTYLE | 0x8000_0000;
    assert_eq!(
        decode_client_flags(&raw.to_be_bytes()),
        Err(ProtocolError::UnsupportedClientFlags {
            raw,
            unsupported: 0x8000_0000,
        }),
    );
}

#[test]
fn option_requests_parse_go_and_abort_wire_frames() {
    let go = encode_go_request(
        "default",
        &[constants::NBD_INFO_EXPORT, constants::NBD_INFO_BLOCK_SIZE],
    )
    .unwrap();
    match parse_option_request(&go).unwrap() {
        OptionRequest::Go(go) => {
            assert_eq!(go.export_name(), "default");
            assert_eq!(
                go.info_requests(),
                &[constants::NBD_INFO_EXPORT, constants::NBD_INFO_BLOCK_SIZE,],
            );
        }
        other => panic!("expected GO request, got {other:?}"),
    }

    assert_eq!(
        parse_option_request(&encode_abort_request(b"ignored").unwrap()).unwrap(),
        OptionRequest::Abort {
            payload: b"ignored".to_vec(),
        },
    );
}

#[test]
fn option_replies_match_fixed_newstyle_wire_layout() {
    let option = NbdOptionCode::new(constants::NBD_OPT_GO);
    let transmission_flags = constants::NBD_FLAG_HAS_FLAGS | constants::NBD_FLAG_SEND_FLUSH;

    assert_eq!(
        encode_export_info_reply(option, 0x0400_0000, transmission_flags).unwrap(),
        [
            0x00, 0x03, 0xe8, 0x89, 0x04, 0x55, 0x65, 0xa9, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00,
            0x00, 0x03, 0x00, 0x00, 0x00, 0x0c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00,
            0x00, 0x00, 0x00, 0x05,
        ],
    );
    assert_eq!(
        parse_option_reply(
            &encode_export_info_reply(option, 0x0400_0000, transmission_flags).unwrap()
        )
        .unwrap(),
        OptionReply::InfoExport {
            option,
            export_size_bytes: 0x0400_0000,
            transmission_flags,
        },
    );

    assert_eq!(
        encode_ack_reply(option).unwrap(),
        [
            0x00, 0x03, 0xe8, 0x89, 0x04, 0x55, 0x65, 0xa9, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00,
            0x00, 0x01, 0x00, 0x00, 0x00, 0x00,
        ],
    );
    assert_eq!(
        parse_option_reply(&encode_ack_reply(option).unwrap()).unwrap(),
        OptionReply::Ack { option },
    );

    let unsupported =
        encode_unsupported_option_reply(NbdOptionCode::new(99), b"unsupported").unwrap();
    assert_eq!(
        unsupported,
        [
            0x00, 0x03, 0xe8, 0x89, 0x04, 0x55, 0x65, 0xa9, 0x00, 0x00, 0x00, 0x63, 0x80, 0x00,
            0x00, 0x01, 0x00, 0x00, 0x00, 0x0b, b'u', b'n', b's', b'u', b'p', b'p', b'o', b'r',
            b't', b'e', b'd',
        ],
    );
    assert_eq!(
        parse_option_reply(&unsupported).unwrap(),
        OptionReply::Error {
            option: NbdOptionCode::new(99),
            reply_type: constants::NBD_REP_ERR_UNSUP,
            message: b"unsupported".to_vec(),
        },
    );

    let unknown = encode_unknown_export_reply(option, b"missing").unwrap();
    assert_eq!(
        parse_option_reply(&unknown).unwrap(),
        OptionReply::Error {
            option,
            reply_type: constants::NBD_REP_ERR_UNKNOWN,
            message: b"missing".to_vec(),
        },
    );

    let header = parse_option_reply_header(&unknown[..OPTION_REPLY_HEADER_BYTES]).unwrap();
    assert_eq!(header.option(), option);
    assert_eq!(header.reply_type(), constants::NBD_REP_ERR_UNKNOWN);
    assert_eq!(header.payload_len(), 7);
}

#[test]
fn happy_path_protocol_script_round_trips_supported_frames() {
    assert_eq!(encode_server_handshake().len(), 18);
    assert_eq!(SIMPLE_REPLY_BYTES, 16);

    assert!(decode_client_flags(&encode_client_flags(true))
        .unwrap()
        .no_zeroes());

    let go =
        parse_option_request(&encode_go_request("disk", &[constants::NBD_INFO_EXPORT]).unwrap())
            .unwrap();
    assert_eq!(go.code(), NbdOptionCode::new(constants::NBD_OPT_GO));

    let option = NbdOptionCode::new(constants::NBD_OPT_GO);
    assert_eq!(
        encode_export_info_reply(option, 0x4000, constants::NBD_FLAG_SEND_FLUSH)
            .unwrap()
            .len(),
        32,
    );
    assert_eq!(encode_ack_reply(option).unwrap().len(), 20);

    let write_cookie = NbdCookie::new(0x0102_0304_0506_0708);
    let write = encode_write_request(write_cookie, 4096, b"hello").unwrap();
    let write_header = parse_request_header(&write[..REQUEST_HEADER_BYTES]).unwrap();
    assert_eq!(
        write_header.payload_len(MAX_WRITE_PAYLOAD_BYTES).unwrap(),
        5
    );

    assert_eq!(
        parse_request(&write, MAX_WRITE_PAYLOAD_BYTES).unwrap(),
        TransmissionRequest::Write {
            cookie: write_cookie,
            offset: 4096,
            data: b"hello".to_vec(),
        },
    );
    assert_eq!(
        encode_success_reply(write_cookie),
        [
            0x67, 0x44, 0x66, 0x98, 0x00, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06,
            0x07, 0x08,
        ],
    );
    assert_eq!(
        parse_simple_reply(&encode_success_reply(write_cookie)).unwrap(),
        SimpleReply {
            cookie: write_cookie,
            error: 0,
        },
    );

    let read_cookie = NbdCookie::new(0x1112_1314_1516_1718);
    assert_eq!(
        parse_request(
            &encode_read_request(read_cookie, 4096, 5).unwrap(),
            MAX_WRITE_PAYLOAD_BYTES
        )
        .unwrap(),
        TransmissionRequest::Read {
            cookie: read_cookie,
            offset: 4096,
            length: 5,
        },
    );
    assert_eq!(
        encode_read_reply(read_cookie, b"hello"),
        [
            0x67, 0x44, 0x66, 0x98, 0x00, 0x00, 0x00, 0x00, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16,
            0x17, 0x18, b'h', b'e', b'l', b'l', b'o',
        ],
    );
    assert_eq!(
        parse_read_reply(&encode_read_reply(read_cookie, b"hello"), 5).unwrap(),
        ReadReply {
            cookie: read_cookie,
            error: 0,
            data: b"hello".to_vec(),
        },
    );

    let flush_cookie = NbdCookie::new(0x2122_2324_2526_2728);
    assert_eq!(
        parse_request(
            &encode_flush_request(flush_cookie).unwrap(),
            MAX_WRITE_PAYLOAD_BYTES
        )
        .unwrap(),
        TransmissionRequest::Flush {
            cookie: flush_cookie,
        },
    );

    let disconnect_cookie = NbdCookie::new(0x3132_3334_3536_3738);
    assert_eq!(
        parse_request(
            &encode_disconnect_request(disconnect_cookie).unwrap(),
            MAX_WRITE_PAYLOAD_BYTES,
        )
        .unwrap(),
        TransmissionRequest::Disconnect {
            cookie: disconnect_cookie,
        },
    );
}

#[test]
fn transmission_rejects_invalid_supported_request_shapes() {
    let oversized = MAX_WRITE_PAYLOAD_BYTES + 1;
    let cases = [
        (
            "bad magic",
            bad_magic_request(),
            ProtocolError::InvalidMagic {
                context: "transmission request",
                expected: constants::NBD_REQUEST_MAGIC as u64,
                actual: 0,
            },
        ),
        (
            "unsupported flags",
            request_header(1, constants::NBD_CMD_READ, 1, 0, 1),
            ProtocolError::UnsupportedCommandFlags { raw: 1 },
        ),
        (
            "zero-length read",
            request_header(0, constants::NBD_CMD_READ, 1, 0, 0),
            ProtocolError::InvalidRequest {
                command: "NBD_CMD_READ",
                reason: "zero length is unsupported",
            },
        ),
        (
            "zero-length write",
            request_header(0, constants::NBD_CMD_WRITE, 1, 0, 0),
            ProtocolError::InvalidRequest {
                command: "NBD_CMD_WRITE",
                reason: "zero length is unsupported",
            },
        ),
        (
            "range overflow",
            request_header(0, constants::NBD_CMD_READ, 1, u64::MAX, 1),
            ProtocolError::LengthOverflow {
                offset: u64::MAX,
                length: 1,
            },
        ),
        (
            "oversized write payload",
            request_header(0, constants::NBD_CMD_WRITE, 1, 0, oversized),
            ProtocolError::LengthTooLarge {
                field: "write payload",
                len: oversized as usize,
                max: MAX_WRITE_PAYLOAD_BYTES as usize,
            },
        ),
    ];

    for (name, bytes, expected) in cases {
        assert_eq!(
            parse_request(&bytes, MAX_WRITE_PAYLOAD_BYTES),
            Err(expected),
            "{name}",
        );
    }
}

fn request_header(flags: u16, command: u16, cookie: u64, offset: u64, length: u32) -> Vec<u8> {
    let mut bytes = Vec::new();
    write_u32(&mut bytes, constants::NBD_REQUEST_MAGIC);
    write_u16(&mut bytes, flags);
    write_u16(&mut bytes, command);
    write_u64(&mut bytes, cookie);
    write_u64(&mut bytes, offset);
    write_u32(&mut bytes, length);
    bytes
}

fn bad_magic_request() -> Vec<u8> {
    let mut bytes = Vec::new();
    write_u32(&mut bytes, 0);
    write_u16(&mut bytes, 0);
    write_u16(&mut bytes, constants::NBD_CMD_READ);
    write_u64(&mut bytes, 1);
    write_u64(&mut bytes, 0);
    write_u32(&mut bytes, 1);
    bytes
}
