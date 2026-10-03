//! The greeting gate: a connection settles its contract once, before any
//! exchange is opened on it.

use std::marker::PhantomData;

use signal::{Contracted, ExchangeFault, Handshake, HandshakeReceipt};

/// Whether this connection has been greeted, and how it was answered.
///
/// The contract is the type parameter because its identity belongs to the
/// type — the digest of its authored source — and not to any value of it.
pub(crate) struct GreetingGate<Q> {
    receipt: Option<HandshakeReceipt>,
    contract: PhantomData<fn() -> Q>,
}

impl<Q> Default for GreetingGate<Q> {
    fn default() -> Self {
        Self {
            receipt: None,
            contract: PhantomData,
        }
    }
}

pub(crate) trait Gating {
    /// Answer the peer's greeting. A second greeting is a fault: the contract
    /// is settled once or not at all.
    fn greeted(&mut self, greeting: &Handshake) -> Result<HandshakeReceipt, ExchangeFault>;

    /// Whether the peer greeted with this contract's own digest.
    fn settled(&self) -> bool;

    /// Refuse an exchange opened before a greeting settled the contract.
    fn require_settled(&self) -> Result<(), ExchangeFault> {
        if self.settled() {
            Ok(())
        } else {
            Err(ExchangeFault::GreetingExpected)
        }
    }
}

impl<Q: Contracted> Gating for GreetingGate<Q> {
    fn greeted(&mut self, greeting: &Handshake) -> Result<HandshakeReceipt, ExchangeFault> {
        if self.receipt.is_some() {
            return Err(ExchangeFault::GreetingRepeated);
        }
        let receipt = Q::receipt(greeting);
        self.receipt = Some(receipt.clone());
        Ok(receipt)
    }

    fn settled(&self) -> bool {
        matches!(self.receipt, Some(HandshakeReceipt::Greeted(_)))
    }
}
