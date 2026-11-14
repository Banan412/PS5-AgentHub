//! Safe wrappers over the PS5 payload SDK. Allocation-free unless the
//! `alloc` feature is on (heap + threads).
//!
//! Anything not wrapped here is still reachable, unsafely, via [`sys`].
//! Modules behind Cargo features (`notify`, `hw`, `http`, ...) link the
//! Sony libraries they need; `full` enables all of them.
#![no_std]
#![cfg_attr(not(target_vendor = "sie"), allow(unused))]

#[cfg(target_vendor = "sie")]
mod buf;
#[cfg(target_vendor = "sie")]
pub mod fmt;
#[cfg(target_vendor = "sie")]
pub mod klog;
#[cfg(target_vendor = "sie")]
pub mod net;
#[cfg(target_vendor = "sie")]
pub mod payload;

#[cfg(all(target_vendor = "sie", feature = "alloc"))]
extern crate alloc;
#[cfg(all(target_vendor = "sie", feature = "alloc"))]
pub mod heap;
#[cfg(all(target_vendor = "sie", feature = "alloc"))]
pub mod thread;
#[cfg(all(target_vendor = "sie", feature = "app"))]
pub mod app;
#[cfg(all(target_vendor = "sie", feature = "http"))]
pub mod http;
#[cfg(all(target_vendor = "sie", feature = "hw"))]
pub mod hw;
#[cfg(all(target_vendor = "sie", feature = "notify"))]
pub mod notify;
#[cfg(all(target_vendor = "sie", feature = "random"))]
pub mod random;
#[cfg(all(target_vendor = "sie", feature = "system"))]
pub mod system;

#[cfg(target_vendor = "sie")]
pub use buf::CString;
#[cfg(all(target_vendor = "sie", feature = "notify"))]
pub use notify::{notify, Toast};
/// The raw bindings, for everything without a safe wrapper.
#[cfg(target_vendor = "sie")]
pub use ps5_sys as sys;

/// Raw SDK error code (negative `SCE_*` value or errno-style int).
#[cfg(target_vendor = "sie")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Error(pub i32);

#[cfg(target_vendor = "sie")]
pub type Result<T> = core::result::Result<T, Error>;

#[cfg(target_vendor = "sie")]
pub(crate) fn check(code: i32) -> Result<()> {
    if code == 0 { Ok(()) } else { Err(Error(code)) }
}

/// Firmware version as reported by the kernel, e.g. 0x13600000.
#[cfg(target_vendor = "sie")]
pub fn fw_version() -> u32 {
    unsafe { sys::kernel_get_fw_version() }
}

/// Current process id.
#[cfg(target_vendor = "sie")]
pub fn pid() -> i32 {
    unsafe { sys::getpid() }
}

/// Sleeps the calling thread.
#[cfg(target_vendor = "sie")]
pub fn sleep_us(us: u32) {
    unsafe { sys::usleep(us) };
}
