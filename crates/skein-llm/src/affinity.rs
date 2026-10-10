//! Fixed-size caller identity rendering; no clocks, randomness or allocations.
//! Contract: llm.md, sections 2.1 and 4.6.

use crate::Affinity;

pub(crate) fn rendered(affinity: Affinity) -> [[u8; 36]; 2] {
    let mut thread = affinity.key;
    let thread_bytes = affinity.thread.to_be_bytes();
    for (key, byte) in thread.get_mut(12..16).expect("fixed UUID tail").iter_mut().zip(thread_bytes) {
        *key ^= byte;
    }
    [uuid(&affinity.key), uuid(&thread)]
}

pub(crate) fn uuid(key: &[u8; 16]) -> [u8; 36] {
    let mut out = [b'-'; 36];
    let mut at = 0_usize;
    for (index, byte) in key.iter().enumerate() {
        if index == 4 || index == 6 || index == 8 || index == 10 {
            at = at.checked_add(1).expect("fixed UUID separator");
        }
        *out.get_mut(at).expect("fixed UUID high nibble") = hex(byte >> 4);
        at = at.checked_add(1).expect("fixed UUID width");
        *out.get_mut(at).expect("fixed UUID low nibble") = hex(byte & 15);
        at = at.checked_add(1).expect("fixed UUID width");
    }
    out
}

fn hex(nibble: u8) -> u8 {
    *b"0123456789abcdef".get(usize::from(nibble)).expect("one hexadecimal nibble")
}

#[cfg(test)]
mod tests {
    use super::rendered;
    use crate::Affinity;

    #[test]
    fn fixed_key_and_big_endian_thread_render_without_uuid_bit_rewriting() {
        let key = [0x00, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x10, 0x32, 0x54, 0x76, 0x98, 0xba, 0xdc];
        let zero = rendered(Affinity { key, thread: 0 });
        assert_eq!(zero[0], *b"00012345-6789-abcd-ef10-32547698badc");
        assert_eq!(zero[0], zero[1]);
        let one = rendered(Affinity { key, thread: 1 });
        let many = rendered(Affinity { key, thread: 0x0102_0304 });
        assert_eq!(one[0], zero[0]);
        assert_eq!(one[1], *b"00012345-6789-abcd-ef10-32547698badd");
        assert_eq!(many[1], *b"00012345-6789-abcd-ef10-3254779ab9d8");
    }
}
