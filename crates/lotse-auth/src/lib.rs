//! Authentication (and, later, authorization) for Lotse, free of I/O.
//!
//! - [`scram`]: SCRAM-SHA-512 for both ends; the broker keeps only salted
//!   keys, never passwords.
//! - [`oauth`]: OAUTHBEARER messages and OpenID Connect access tokens,
//!   checked against the issuer's key set and a time the caller passes in.
//! - [`roles`]: who may do what, from roles bound per tenant.
//! - [`x509`]: how an issuer is named, and whether a recorded client
//!   certificate answers its signing request.
//!
//! Nothing here reads a clock, a socket or a file: the broker and the client
//! fetch what is needed through the runtime seam, so the simulation decides
//! every input.

#![warn(missing_docs)]

pub mod oauth;
pub mod roles;
pub mod scram;
#[cfg(feature = "testing")]
pub mod testing;
pub mod x509;

#[cfg(test)]
mod tests;
