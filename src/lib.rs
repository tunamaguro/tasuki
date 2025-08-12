#[allow(unused)]
mod queries;

mod client;
pub use client::{Client, Error as ClientError, InsertJob};
