use nbd_protocol::constants;
use nbd_protocol::handshake::{
    decode_client_flags, encode_server_handshake, SERVER_HANDSHAKE_FLAGS,
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
