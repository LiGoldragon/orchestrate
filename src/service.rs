//! The single owner of the path-lock registry.

use meta_signal_orchestrate::{
    MetaOrchestrateReply, MetaOrchestrateRequest, RefreshRepositoryIndexOrder,
    RepositoryIndexRefreshed,
};
use signal_frame::{
    NonEmpty, OperationFailureReason, Reply, Request, RequestRejectionReason, SubReply,
};
use signal_orchestrate::{OrchestrateReply, OrchestrateRequest, PathLock};

use crate::{OrchestrateTables, Result, StoreLocation};

pub struct OrchestrateService {
    tables: OrchestrateTables,
}

impl OrchestrateService {
    pub fn open(store: &StoreLocation) -> Result<Self> {
        Ok(Self {
            tables: OrchestrateTables::open(store)?,
        })
    }

    /// Direct-service entrypoint used by the durable behavior witness.
    pub fn register(&mut self, lock: PathLock) -> Result<OrchestrateReply> {
        Ok(match self.tables.register_path_lock(lock)? {
            Ok(registered) => OrchestrateReply::PathLockRegistered(registered),
            Err(rejected) => OrchestrateReply::PathLockRegistrationRejected(rejected),
        })
    }

    pub fn active_path_locks(&self) -> Result<Vec<crate::StoredPathLock>> {
        self.tables.active_path_locks()
    }

    #[doc(hidden)]
    pub fn fail_next_atomic_commit_for_test(&self) {
        self.tables.fail_next_atomic_commit_for_test();
    }

    pub async fn handle_request(
        &mut self,
        request: Request<OrchestrateRequest>,
    ) -> Reply<OrchestrateReply> {
        if !request.payloads().tail().is_empty() {
            return Reply::rejected(RequestRejectionReason::Internal);
        }
        let OrchestrateRequest::Register(lock) = request.payloads().head().clone();
        match self.register(lock) {
            Ok(reply @ OrchestrateReply::PathLockRegistered(_)) => {
                Reply::committed(NonEmpty::single(SubReply::Ok(reply)))
            }
            Ok(reply @ OrchestrateReply::PathLockRegistrationRejected(_)) => {
                Reply::operation_aborted(
                    0,
                    OperationFailureReason::DomainRejection,
                    NonEmpty::single(SubReply::Failed {
                        reason: OperationFailureReason::DomainRejection,
                        detail: Some(reply),
                    }),
                )
            }
            Err(_) => Reply::rejected(RequestRejectionReason::Internal),
        }
    }

    pub async fn handle_meta_request(
        &mut self,
        request: Request<MetaOrchestrateRequest>,
    ) -> Reply<MetaOrchestrateReply> {
        if !request.payloads().tail().is_empty() {
            return Reply::rejected(RequestRejectionReason::Internal);
        }
        let MetaOrchestrateRequest::Refresh(RefreshRepositoryIndexOrder {}) =
            request.payloads().head().clone();
        Reply::committed(NonEmpty::single(SubReply::Ok(
            MetaOrchestrateReply::RepositoryIndexRefreshed(RepositoryIndexRefreshed::new(0)),
        )))
    }
}
