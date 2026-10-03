//! The exchange ledger: which exchanges are open on one connection, and
//! which of them are subscriptions still being fed.

use signal::{ExchangeFault, ExchangeId, ExchangeLedger, ExchangeTracking, Greeted};
use tokio::task::AbortHandle;

/// The answering side's record of one connection's exchanges.
///
/// The open set is `signal`'s ledger, so the ceiling on open exchanges and the
/// faults for a reused or unknown identifier are the ones every Nexus shares.
/// What this adds is the task feeding each subscription, so that abandoning
/// one — or closing the connection — stops the work done for it.
#[derive(Default)]
pub(crate) struct SessionLedger {
    exchanges: ExchangeLedger,
    streams: Vec<(ExchangeId, AbortHandle)>,
}

pub(crate) trait Ledgering {
    /// Record that the greeting settled the contract; exchanges may open.
    fn settle(&mut self) -> Result<(), ExchangeFault>;

    /// Take an exchange the peer opened into the open set.
    fn admit(&mut self, exchange: ExchangeId) -> Result<ExchangeId, ExchangeFault>;

    /// Record the task feeding a subscription on an admitted exchange.
    fn streaming(&mut self, exchange: ExchangeId, feeding: AbortHandle);

    /// The answering side is done with an exchange: release it.
    fn conclude(&mut self, exchange: ExchangeId) -> Result<ExchangeId, ExchangeFault>;

    /// The peer gave up an exchange: stop feeding it and release it.
    fn abandon(&mut self, exchange: ExchangeId) -> Result<ExchangeId, ExchangeFault>;

    /// Whether a frame for this exchange is still wanted.
    fn bears(&self, exchange: ExchangeId) -> bool;
}

impl Ledgering for SessionLedger {
    fn settle(&mut self) -> Result<(), ExchangeFault> {
        self.exchanges.greet()
    }

    fn admit(&mut self, exchange: ExchangeId) -> Result<ExchangeId, ExchangeFault> {
        self.exchanges.admit(exchange)
    }

    fn streaming(&mut self, exchange: ExchangeId, feeding: AbortHandle) {
        self.streams.push((exchange, feeding));
    }

    fn conclude(&mut self, exchange: ExchangeId) -> Result<ExchangeId, ExchangeFault> {
        self.streams.retain(|(streamed, _)| *streamed != exchange);
        self.exchanges.release(exchange)
    }

    fn abandon(&mut self, exchange: ExchangeId) -> Result<ExchangeId, ExchangeFault> {
        let released = self.exchanges.release(exchange)?;
        self.streams.retain(|(streamed, feeding)| {
            let abandoned = *streamed == exchange;
            if abandoned {
                feeding.abort();
            }
            !abandoned
        });
        Ok(released)
    }

    fn bears(&self, exchange: ExchangeId) -> bool {
        self.exchanges.bears(exchange)
    }
}

/// A closed connection retracts every exchange on it, so nothing goes on
/// being fed for a peer that is gone.
impl Drop for SessionLedger {
    fn drop(&mut self) {
        for (_, feeding) in &self.streams {
            feeding.abort();
        }
    }
}
