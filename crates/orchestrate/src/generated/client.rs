#![allow(dead_code, non_camel_case_types, non_snake_case)]
#[rustfmt::skip]
pub type SocketPath = String;
#[rustfmt::skip]
pub type TransportError = String;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Unreachable {
    pub socket_path: SocketPath,
    pub transport_error: TransportError,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum ClientFailure {
    Unreadable(datom_codec::Error),
    Unreachable(Unreachable),
    GreetingRefused(signal::HandshakeRejection),
    ExchangeFaulted(signal::ExchangeFault),
}
