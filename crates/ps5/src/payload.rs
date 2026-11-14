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

/// Returns the kernel boot time in Unix epoch seconds (`CTL_KERN` / `KERN_BOOTTIME`),
/// or `None` if unavailable or unsupported.
pub fn kernel_boot_time_secs() -> Option<i64> {
    #[cfg(target_vendor = "sie")]
    {
        let mib: [core::ffi::c_int; 2] = [
            ps5_sys::CTL_KERN as core::ffi::c_int,
            ps5_sys::KERN_BOOTTIME as core::ffi::c_int,
        ];
        let mut tv: ps5_sys::timeval = unsafe { core::mem::zeroed() };
        let expected = core::mem::size_of::<ps5_sys::timeval>();
        let mut len = expected;
        // Safety: `mib` has 2 `c_int` entries and `tv` is sized for `struct timeval`.
        let ret = unsafe {
            ps5_sys::sysctl(
                mib.as_ptr(),
                mib.len() as _,
                (&mut tv as *mut ps5_sys::timeval).cast(),
                &mut len,
                core::ptr::null(),
                0,
            )
        };
        if ret == 0 && len == expected && (tv.tv_sec as i64) > 0 {
            Some(tv.tv_sec as i64)
        } else {
            None
        }
    }

    #[cfg(not(target_vendor = "sie"))]
    {
        None
    }
}
