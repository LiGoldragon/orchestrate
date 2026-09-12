#![allow(dead_code, non_camel_case_types, non_snake_case)]
#[rustfmt::skip]
pub type SocketPath = String;
#[rustfmt::skip]
pub type TransportError = String;
#[rustfmt::skip]
#[derive(datom_codec::Datomizable, datom_codec::Compositional, Clone, Debug, PartialEq)]
pub struct Unreachable {
    pub socket_path: SocketPath,
    pub transport_error: TransportError,
}
#[rustfmt::skip]
#[derive(datom_codec::Datomizable, datom_codec::Compositional, Clone, Debug, PartialEq)]
pub enum ClientFailure {
    Unreadable(datom_codec::Error),
    Unreachable(Unreachable),
}
