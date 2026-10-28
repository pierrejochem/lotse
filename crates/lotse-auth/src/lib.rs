//! Authentication (and, later, authorization) for Lotse, free of I/O.
//!
//! - [`scram`]: SCRAM-SHA-512 for both ends; the broker keeps only salted
//!   keys, never passwords.
//! - [`oauth`]: OAUTHBEARER messages and OpenID Connect access tokens,
//!   checked against the issuer's key set and a time the caller passes in.
//!
//! Nothing here reads a clock, a socket or a file: the broker and the client
//! fetch what is needed through the runtime seam, so the simulation decides
//! every input.

#![warn(missing_docs)]

pub mod oauth;
pub mod scram;
#[cfg(feature = "testing")]
pub mod testing;

#[cfg(test)]
mod tests;
