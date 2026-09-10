#![allow(dead_code, non_camel_case_types, non_snake_case)]
pub type SocketPath = String;
pub type TransportError = String;
#[derive(datom_codec::Datomizable, datom_codec::Compositional)]
pub struct Unreachable {
    pub socket_path: SocketPath,
    pub transport_error: TransportError,
}
#[derive(datom_codec::Datomizable, datom_codec::Compositional)]
pub enum ClientFailure {
    Unreadable(datom_codec::Error),
    Unreachable(Unreachable),
}
