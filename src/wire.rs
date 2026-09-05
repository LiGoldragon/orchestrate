//! Nexus contract bindings and route checks for generated Signal wires.

use std::num::{NonZeroU16, NonZeroU32};

use signal_frame::{
    ContractBinding, ContractId, RootCode, VariantCode, WireContract, WireRevision, WireRoute,
};

pub struct OrdinaryContract;
impl WireContract for OrdinaryContract {
    const BINDING: ContractBinding = ContractBinding::new(
        ContractId::new(NonZeroU32::new(1).expect("static contract id")),
        WireRevision::new(NonZeroU16::new(4).expect("static revision")),
    );
}

pub struct MetaContract;
impl WireContract for MetaContract {
    const BINDING: ContractBinding = ContractBinding::new(
        ContractId::new(NonZeroU32::new(2).expect("static contract id")),
        WireRevision::new(NonZeroU16::new(3).expect("static revision")),
    );
}

const REQUEST_ROOT: RootCode = RootCode::new(0);
const RESPONSE_ROOT: RootCode = RootCode::new(1);

fn route(root: RootCode, variant: u8) -> WireRoute {
    WireRoute::new(root, VariantCode::new(variant))
}

pub fn ordinary_request_route(value: &signal_orchestrate::Request) -> WireRoute {
    route(
        REQUEST_ROOT,
        match value {
            signal_orchestrate::Request::Lock(_) => 0,
            signal_orchestrate::Request::Release(_) => 1,
            signal_orchestrate::Request::Observe(_) => 2,
        },
    )
}
pub fn ordinary_response_route(value: &signal_orchestrate::Response) -> WireRoute {
    route(
        RESPONSE_ROOT,
        match value {
            signal_orchestrate::Response::Locked(_) => 0,
            signal_orchestrate::Response::Released(_) => 1,
            signal_orchestrate::Response::Observed(_) => 2,
            signal_orchestrate::Response::LockRejected(_) => 3,
            signal_orchestrate::Response::ReleaseRejected(_) => 4,
        },
    )
}
pub fn meta_request_route(_: &meta_signal_orchestrate::Request) -> WireRoute {
    route(REQUEST_ROOT, 0)
}
pub fn meta_response_route(value: &meta_signal_orchestrate::Response) -> WireRoute {
    route(
        RESPONSE_ROOT,
        match value {
            meta_signal_orchestrate::Response::Configured(_) => 0,
            meta_signal_orchestrate::Response::ConfigurationRejected(_) => 1,
        },
    )
}
