//! SHA-256 content versions for conditional whole-file stores (io.md, section 5).
//!
//! No bytes are kept: the caller owns a bounded file buffer. Arithmetic in
//! the compression rounds is intentionally modulo 2^32, as SHA-256 defines.

#![expect(
    clippy::indexing_slicing,
    reason = "fixed 64-word schedule and 64-round constants are indexed only within their ranges"
)]
#![expect(
    clippy::arithmetic_side_effects,
    reason = "SHA schedule indices are proven by the fixed 16..64 and 0..64 ranges"
)]
#![expect(clippy::many_single_char_names, reason = "SHA-256 names its eight working words a through h")]

use core::cmp::Ordering;

/// A content digest represented as four big-endian 64-bit words.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Digest(pub [u64; 4]);

const INITIAL: [u32; 8] =
    [0x6a09_e667, 0xbb67_ae85, 0x3c6e_f372, 0xa54f_f53a, 0x510e_527f, 0x9b05_688c, 0x1f83_d9ab, 0x5be0_cd19];

const ROUND: [u32; 64] = [
    0x428a_2f98,
    0x7137_4491,
    0xb5c0_fbcf,
    0xe9b5_dba5,
    0x3956_c25b,
    0x59f1_11f1,
    0x923f_82a4,
    0xab1c_5ed5,
    0xd807_aa98,
    0x1283_5b01,
    0x2431_85be,
    0x550c_7dc3,
    0x72be_5d74,
    0x80de_b1fe,
    0x9bdc_06a7,
    0xc19b_f174,
    0xe49b_69c1,
    0xefbe_4786,
    0x0fc1_9dc6,
    0x240c_a1cc,
    0x2de9_2c6f,
    0x4a74_84aa,
    0x5cb0_a9dc,
    0x76f9_88da,
    0x983e_5152,
    0xa831_c66d,
    0xb003_27c8,
    0xbf59_7fc7,
    0xc6e0_0bf3,
    0xd5a7_9147,
    0x06ca_6351,
    0x1429_2967,
    0x27b7_0a85,
    0x2e1b_2138,
    0x4d2c_6dfc,
    0x5338_0d13,
    0x650a_7354,
    0x766a_0abb,
    0x81c2_c92e,
    0x9272_2c85,
    0xa2bf_e8a1,
    0xa81a_664b,
    0xc24b_8b70,
    0xc76c_51a3,
    0xd192_e819,
    0xd699_0624,
    0xf40e_3585,
    0x106a_a070,
    0x19a4_c116,
    0x1e37_6c08,
    0x2748_774c,
    0x34b0_bcb5,
    0x391c_0cb3,
    0x4ed8_aa4a,
    0x5b9c_ca4f,
    0x682e_6ff3,
    0x748f_82ee,
    0x78a5_636f,
    0x84c8_7814,
    0x8cc7_0208,
    0x90be_fffa,
    0xa450_6ceb,
    0xbef9_a3f7,
    0xc671_78f2,
];

/// Digests one bounded file's bytes.
#[must_use]
pub fn digest(bytes: &[u8]) -> Digest {
    let mut state = INITIAL;
    let length = u64::try_from(bytes.len()).expect("file length fits in u64");
    let bit_length = length.checked_mul(8).expect("file bit length fits in u64");
    let padded = bytes.len().checked_add(9).expect("padding fits");
    let blocks = padded.checked_add(63).expect("padding fits").checked_div(64).expect("positive block size");
    for block in 0..blocks {
        let mut words = [0_u32; 64];
        for (index, word) in words.iter_mut().take(16).enumerate() {
            let mut value = 0_u32;
            for byte in 0..4 {
                let block_offset = block.checked_mul(64).expect("block offset fits");
                let word_offset = index.checked_mul(4).expect("word offset fits");
                let offset = block_offset
                    .checked_add(word_offset)
                    .expect("offset fits")
                    .checked_add(byte)
                    .expect("byte offset fits");
                let next = match offset.cmp(&bytes.len()) {
                    Ordering::Less => *bytes.get(offset).expect("within the input"),
                    Ordering::Equal => 0x80,
                    Ordering::Greater => {
                        let end = blocks.checked_mul(64).expect("padded length fits");
                        let tail = end.checked_sub(8).expect("one block has room for length");
                        if offset >= tail {
                            let shift = end
                                .checked_sub(1)
                                .expect("positive padded length")
                                .checked_sub(offset)
                                .expect("offset within padding")
                                .checked_mul(8)
                                .expect("length shift fits");
                            u8::try_from((bit_length >> shift) & 0xff).expect("one byte")
                        } else {
                            0
                        }
                    }
                };
                value = value.checked_shl(8).expect("eight-bit shift").wrapping_add(u32::from(next));
            }
            *word = value;
        }
        for index in 16..64 {
            let left = words[index - 15];
            let right = words[index - 2];
            let s0 = left.rotate_right(7) ^ left.rotate_right(18) ^ (left >> 3_u32);
            let s1 = right.rotate_right(17) ^ right.rotate_right(19) ^ (right >> 10_u32);
            words[index] = words[index - 16].wrapping_add(s0).wrapping_add(words[index - 7]).wrapping_add(s1);
        }
        let mut a = state[0];
        let mut b = state[1];
        let mut c = state[2];
        let mut d = state[3];
        let mut e = state[4];
        let mut f = state[5];
        let mut g = state[6];
        let mut h = state[7];
        for index in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let choose = (e & f) ^ ((!e) & g);
            let first = h.wrapping_add(s1).wrapping_add(choose).wrapping_add(ROUND[index]).wrapping_add(words[index]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let majority = (a & b) ^ (a & c) ^ (b & c);
            let second = s0.wrapping_add(majority);
            h = g;
            g = f;
            f = e;
            e = d.wrapping_add(first);
            d = c;
            c = b;
            b = a;
            a = first.wrapping_add(second);
        }
        state[0] = state[0].wrapping_add(a);
        state[1] = state[1].wrapping_add(b);
        state[2] = state[2].wrapping_add(c);
        state[3] = state[3].wrapping_add(d);
        state[4] = state[4].wrapping_add(e);
        state[5] = state[5].wrapping_add(f);
        state[6] = state[6].wrapping_add(g);
        state[7] = state[7].wrapping_add(h);
    }
    Digest([
        u64::from(state[0]) << 32 | u64::from(state[1]),
        u64::from(state[2]) << 32 | u64::from(state[3]),
        u64::from(state[4]) << 32 | u64::from(state[5]),
        u64::from(state[6]) << 32 | u64::from(state[7]),
    ])
}

#[cfg(test)]
mod tests {
    use super::{Digest, digest};

    #[test]
    fn published_sha256_vectors() {
        assert_eq!(
            digest(b""),
            Digest([0xe3b0_c442_98fc_1c14, 0x9afb_f4c8_996f_b924, 0x27ae_41e4_649b_934c, 0xa495_991b_7852_b855])
        );
        assert_eq!(
            digest(b"abc"),
            Digest([0xba78_16bf_8f01_cfea, 0x4141_40de_5dae_2223, 0xb003_61a3_9617_7a9c, 0xb410_ff61_f200_15ad])
        );
    }
}
