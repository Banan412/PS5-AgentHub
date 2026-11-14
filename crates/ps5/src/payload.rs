//! The payload's own lifecycle.

pub use ps5_sys::payload_args_t as Args;

/// Arguments the loader passed to `_start()`.
pub fn args() -> &'static Args {
    // The SDK keeps these in a static that lives for the whole payload.
    unsafe { &*ps5_sys::payload_get_args() }
}

/// Exits the payload. In a hijacked process, that process resumes.
pub fn exit(code: i32) -> ! {
    unsafe { ps5_sys::payload_exit(code) };
    loop {}
}

const MAXCOMLEN: usize = 19;
const SYS_THR_SET_NAME: u64 = 464;

/// Sets the current thread/process name (`thr_set_name(-1, name)`).
///
/// Truncates `name` to FreeBSD's `MAXCOMLEN` (19 bytes, stopping at any NUL byte).
/// Returns `0` on success or `-errno` on failure.
pub fn set_process_name(name: &str) -> i32 {
    let mut buf = [0u8; MAXCOMLEN + 1];
    for (dst, &b) in buf[..MAXCOMLEN].iter_mut().zip(name.as_bytes().iter()) {
        if b == 0 {
            break;
        }
        *dst = b;
    }

    #[cfg(target_arch = "x86_64")]
    {
        let ret: i64;
        let failed: u8;
        // Safety: SYS_thr_set_name(-1, buf.as_ptr()) reads a NUL-terminated string
        // from `buf`, which is always NUL-terminated within `[u8; 20]`.
        unsafe {
            core::arch::asm!(
                "syscall",
                "setc {failed}",
                failed = out(reg_byte) failed,
                inlateout("rax") SYS_THR_SET_NAME as i64 => ret,
                in("rdi") (-1i64) as u64,
                in("rsi") buf.as_ptr() as u64,
                lateout("rcx") _,
                lateout("r11") _,
                options(nostack),
            );
        }
        if failed == 0 { 0 } else { -(ret as i32) }
    }

    #[cfg(not(target_arch = "x86_64"))]
    {
        let _ = buf;
        0
    }
}
