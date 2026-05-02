use nbd_protocol::constants;
use nbd_protocol::handshake::{
    decode_client_flags, encode_server_handshake, SERVER_HANDSHAKE_FLAGS,
};
use nbd_protocol::option::{
    encode_ack_reply, encode_export_info_reply, encode_unsupported_option_reply,
    parse_option_request, OptionRequest,
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
    let flags = decode_client_flags(&raw.to_be_bytes()).unwrap();

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
    let mut go_payload = Vec::new();
    write_u32(&mut go_payload, 7);
    go_payload.extend_from_slice(b"default");
    write_u16(&mut go_payload, 2);
    write_u16(&mut go_payload, constants::NBD_INFO_EXPORT);
    write_u16(&mut go_payload, constants::NBD_INFO_BLOCK_SIZE);

    match parse_option_request(&option_request_bytes(constants::NBD_OPT_GO, &go_payload)).unwrap() {
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
        parse_option_request(&option_request_bytes(constants::NBD_OPT_ABORT, b"ignored")).unwrap(),
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
        encode_ack_reply(option).unwrap(),
        [
            0x00, 0x03, 0xe8, 0x89, 0x04, 0x55, 0x65, 0xa9, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00,
            0x00, 0x01, 0x00, 0x00, 0x00, 0x00,
        ],
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
}

fn option_request_bytes(option: u32, payload: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::new();
    write_u64(&mut bytes, constants::IHAVEOPT_MAGIC);
    write_u32(&mut bytes, option);
    write_u32(&mut bytes, payload.len() as u32);
    bytes.extend_from_slice(payload);
    bytes
}
