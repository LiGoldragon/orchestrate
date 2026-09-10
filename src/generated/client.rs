#![allow(dead_code, non_camel_case_types, non_snake_case)]
#[derive(datom_codec::Datomizable, datom_codec::Compositional)]
pub struct Unreachable_Data {
    pub first_string: String,
    pub second_string: String,
}
#[derive(datom_codec::Datomizable, datom_codec::Compositional)]
pub enum ClientFailure {
    Unreadable(datom_codec::Error),
    Unreachable(Unreachable_Data),
    Refused(signal_orchestrate::Refusal),
}
