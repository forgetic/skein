//! CRC-32C (Castagnoli), reflected polynomial.

#![expect(clippy::as_conversions, reason = "table index is always below 256 in this const function")]
#![expect(clippy::cast_possible_truncation, reason = "table index is always below 256")]
#![expect(clippy::arithmetic_side_effects, reason = "table loops increment only within their fixed bounds")]
#![expect(clippy::indexing_slicing, reason = "table index is checked against the 256-element bound")]

const POLY: u32 = 0x82f6_3b78;

const fn table() -> [u32; 256] {
    let mut result = [0_u32; 256];
    let mut index: usize = 0;
    while index < result.len() {
        let mut value = index as u32;
        let mut bit: u32 = 0;
        while bit < 8 {
            value = if value & 1 == 0 { value >> 1_u32 } else { (value >> 1_u32) ^ POLY };
            bit += 1;
        }
        result[index] = value;
        index += 1;
    }
    result
}

const TABLE: [u32; 256] = table();

#[must_use]
pub(crate) fn crc32c(bytes: &[u8]) -> u32 {
    let mut value = !0_u32;
    for &byte in bytes {
        let index = usize::try_from((value ^ u32::from(byte)) & 0xff).expect("a byte fits usize");
        value = (value >> 8_u32) ^ TABLE.get(index).copied().expect("CRC table has 256 entries");
    }
    !value
}
