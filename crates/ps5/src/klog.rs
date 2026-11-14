//! Kernel log (`/dev/klog`), readable with a klog client on port 3232.

use crate::buf::Buf;

/// Writes one line to the kernel log, stripping interior NULs and
/// clamping >1020-byte lines cleanly on a UTF-8 boundary.
pub fn log(line: &str) {
    const MAX_LEN: usize = 1020;
    const ELLIPSIS: &[u8] = b"...";
    let sanitized_len: usize = line
        .chars()
        .filter(|&c| c != '\0')
        .map(char::len_utf8)
        .sum();
    let truncated = sanitized_len > MAX_LEN;
    let limit = if truncated {
        MAX_LEN - ELLIPSIS.len()
    } else {
        MAX_LEN
    };

    let mut s = Buf::<1024>::new();
    let mut used = 0usize;
    for ch in line.chars().filter(|&c| c != '\0') {
        let mut utf8 = [0u8; 4];
        let bytes = ch.encode_utf8(&mut utf8).as_bytes();
        if used + bytes.len() > limit {
            break;
        }
        s.push(bytes);
        used += bytes.len();
    }
    if truncated {
        s.push(ELLIPSIS);
    }
    unsafe { ps5_sys::klog_puts(s.as_ptr()) };
}
