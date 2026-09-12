//! The configuration authority the Nexus keeps: who may configure it, and the
//! durable record of whether the privileged surface ever did.

use std::{path::Path, sync::Arc};

use meta_signal_orchestrate::{Query as MetaQuery, Response as MetaResponse};
use orchestrate_nexus::{
    OpensStore, OrchestrateStore,
    core::{Applies, Founding, NexusCore},
};
use signal_orchestrate::{
    ConfigurationReceipt, ConfigurationRejection, ConfigurationRejectionReason,
    OrchestrateNexusConfiguration, Query as OrdinaryQuery, Response as OrdinaryResponse,
};

trait Defaults {
    fn socket_paths(&self, prefix: &str) -> OrchestrateNexusConfiguration;
}

impl Defaults for Path {
    fn socket_paths(&self, prefix: &str) -> OrchestrateNexusConfiguration {
        OrchestrateNexusConfiguration {
            ordinary_socket_path: self
                .join(format!("{prefix}-ordinary.sock"))
                .display()
                .to_string(),
            meta_socket_path: self
                .join(format!("{prefix}-meta.sock"))
                .display()
                .to_string(),
        }
    }
}

trait Founds {
    fn core(&self, store_name: &str) -> Arc<NexusCore>;
}

impl Founds for tempfile::TempDir {
    fn core(&self, store_name: &str) -> Arc<NexusCore> {
        let (store, _) = OrchestrateStore::open(
            &self.path().join(store_name),
            self.path().socket_paths("default"),
        )
        .expect("open store");
        NexusCore::found(store)
    }
}

#[tokio::test]
async fn ordinary_configure_is_open_until_the_privileged_surface_closes_it() {
    let directory = tempfile::tempdir().expect("isolated Nexus store");
    let core = directory.core("authority.sema");
    let ordinary = directory.path().socket_paths("ordinary");

    assert_eq!(
        core.apply(OrdinaryQuery::Configure(ordinary.clone()))
            .await
            .expect("ordinary Configure on a fresh store"),
        OrdinaryResponse::ConfigurationAccepted(ConfigurationReceipt {
            orchestrate_nexus_configuration: ordinary,
            meta_configure_done: false,
        }),
    );

    let privileged = directory.path().socket_paths("privileged");
    assert_eq!(
        core.apply(MetaQuery::Configure(privileged.clone()))
            .await
            .expect("privileged Configure"),
        MetaResponse::Configured(ConfigurationReceipt {
            orchestrate_nexus_configuration: privileged.clone(),
            meta_configure_done: true,
        }),
    );

    assert_eq!(
        core.apply(OrdinaryQuery::Configure(
            directory.path().socket_paths("refused")
        ))
        .await
        .expect("ordinary Configure after the privileged one"),
        OrdinaryResponse::ConfigurationRefused(ConfigurationRejection {
            configuration_rejection_reason: ConfigurationRejectionReason::MetaConfigureOccurred,
        }),
    );

    assert_eq!(
        core.apply(MetaQuery::ReverseMetaConfiguration)
            .await
            .expect("reverse"),
        MetaResponse::OrdinaryConfigurationReopened(ConfigurationReceipt {
            orchestrate_nexus_configuration: privileged,
            meta_configure_done: false,
        }),
    );
    let reopened = directory.path().socket_paths("reopened");
    assert_eq!(
        core.apply(OrdinaryQuery::Configure(reopened.clone()))
            .await
            .expect("ordinary Configure after reversal"),
        OrdinaryResponse::ConfigurationAccepted(ConfigurationReceipt {
            orchestrate_nexus_configuration: reopened,
            meta_configure_done: false,
        }),
    );
}

#[tokio::test]
async fn whether_the_privileged_configure_occurred_survives_a_restart() {
    let directory = tempfile::tempdir().expect("isolated Nexus store");
    let store_path = directory.path().join("durable.sema");
    let defaults = directory.path().socket_paths("default");
    let privileged = directory.path().socket_paths("privileged");

    let (store, _) = OrchestrateStore::open(&store_path, defaults.clone()).expect("open");
    let core = NexusCore::found(store);
    core.apply(MetaQuery::Configure(privileged.clone()))
        .await
        .expect("privileged Configure");
    drop(core);

    let (store, resumed) = OrchestrateStore::open(&store_path, defaults).expect("reopen");
    assert_eq!(
        resumed, privileged,
        "a store resumes its own configuration, not the executable's defaults"
    );
    let core = NexusCore::found(store);
    assert_eq!(
        core.apply(OrdinaryQuery::Configure(
            directory.path().socket_paths("refused")
        ))
        .await
        .expect("ordinary Configure after restart"),
        OrdinaryResponse::ConfigurationRefused(ConfigurationRejection {
            configuration_rejection_reason: ConfigurationRejectionReason::MetaConfigureOccurred,
        }),
        "the record that the privileged Configure occurred is durable"
    );
}

#[tokio::test]
async fn an_unbindable_configuration_is_refused_on_both_surfaces() {
    let directory = tempfile::tempdir().expect("isolated Nexus store");
    let core = directory.core("invalid.sema");
    let same_path = directory.path().join("one.sock").display().to_string();
    let invalid = OrchestrateNexusConfiguration {
        ordinary_socket_path: same_path.clone(),
        meta_socket_path: same_path,
    };

    assert_eq!(
        core.apply(OrdinaryQuery::Configure(invalid.clone()))
            .await
            .expect("ordinary refusal"),
        OrdinaryResponse::ConfigurationRefused(ConfigurationRejection {
            configuration_rejection_reason: ConfigurationRejectionReason::InvalidConfiguration,
        }),
    );
    assert_eq!(
        core.apply(MetaQuery::Configure(invalid))
            .await
            .expect("privileged refusal"),
        MetaResponse::ConfigurationRejected(meta_signal_orchestrate::ConfigurationRejection {
            configuration_rejection_reason:
                meta_signal_orchestrate::ConfigurationRejectionReason::InvalidConfiguration,
        }),
    );
}
