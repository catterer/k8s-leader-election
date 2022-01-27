#![deny(unsafe_code)]

mod lease;

pub use lease::{Error, LeaseLock, LeaseGuard};

