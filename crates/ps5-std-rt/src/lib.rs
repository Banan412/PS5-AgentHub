//! Makes Rust's FreeBSD `std` link as a PS5 payload.
#![no_std]
#![cfg_attr(not(target_vendor = "sie"), allow(unused))]

#[cfg(target_vendor = "sie")]
mod rt_impl;
#[cfg(target_vendor = "sie")]
pub use rt_impl::*;
